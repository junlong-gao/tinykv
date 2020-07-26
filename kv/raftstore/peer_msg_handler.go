package raftstore

import (
	"fmt"
	"time"

	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"

	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"

	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
	"github.com/pingcap/errors"
)

type PeerTick int

const (
	PeerTickRaft               PeerTick = 0
	PeerTickRaftLogGC          PeerTick = 1
	PeerTickSplitRegionCheck   PeerTick = 2
	PeerTickSchedulerHeartbeat PeerTick = 3
)

type peerMsgHandler struct {
	*peer
	ctx *GlobalContext
}

func newPeerMsgHandler(peer *peer, ctx *GlobalContext) *peerMsgHandler {
	return &peerMsgHandler{
		peer: peer,
		ctx:  ctx,
	}
}

type outStandingReq struct {
	req *raft_cmdpb.RaftCmdRequest
	p   *raft_cmdpb.RaftCmdResponse
}

// On updating region when there is split/config change:
// Unfortunately there is a race condition between mutation of the region here
// and sending region heart beat to the scheduler. This is by no means production ready code
// but let's just use copy and pointer swap to hope no bad thing can happen.
func (d *peerMsgHandler) processCmd(
	cb *message.Callback, m *pb.Entry, kvWB *engine_util.WriteBatch,
	p *raft_cmdpb.RaftCmdResponse,
	applied uint64) (req *raft_cmdpb.RaftCmdRequest, destroyed bool) {
	destroyed = false

	if m.EntryType == pb.EntryType_EntryConfChange {
		var cc pb.ConfChange
		err := cc.Unmarshal(m.Data)
		if err != nil {
			panic(err)
		}

		var ctx raft_cmdpb.RaftCmdRequest
		err = ctx.Unmarshal(cc.Context)
		if err != nil {
			panic(err)
		}
		err = util.CheckRegionEpoch(&ctx, d.Region(), true)
		if err != nil {
			return
		}

		var updatedRegion metapb.Region
		err = util.CloneMsg(d.Region(), &updatedRegion)
		if err != nil {
			panic(err)
		}

		if cc.ChangeType == pb.ConfChangeType_AddNode {
			log.Infof("%v adding node (req:%v) %v to region %v (index %v,%v)", d.Tag, ctx.Header.RegionEpoch, cc.NodeId, d.Region(), m.Index, m.Term)
			updatedRegion.RegionEpoch.ConfVer++
			newPeer := &metapb.Peer{
				Id:      cc.NodeId,
				StoreId: ctx.AdminRequest.ChangePeer.Peer.StoreId,
			}
			updatedRegion.Peers = append(updatedRegion.Peers, newPeer)

			d.SetRegion(&updatedRegion)
			d.ctx.storeMeta.regions[d.regionId] = &updatedRegion
			d.peer.insertPeerCache(newPeer)

			d.RaftGroup.ApplyConfChange(cc)
			meta.WriteRegionState(kvWB, d.Region(), rspb.PeerState_Normal)
			return
		} else {
			log.Infof("%v removing node (req:%v) %v from region %v (index %v,%v)", d.Tag, ctx.Header.RegionEpoch, cc.NodeId, d.Region(), m.Index, m.Term)
			idx := -1
			for i, p := range updatedRegion.Peers {
				if p.Id == cc.NodeId {
					idx = i
					break
				}
			}
			if idx < 0 {
				log.Panicf("%v:%v, looking for %v", d.Tag, updatedRegion, cc.NodeId)
			}

			updatedRegion.RegionEpoch.ConfVer++
			updatedRegion.Peers = append(updatedRegion.Peers[0:idx], d.Region().Peers[idx+1:]...)

			d.SetRegion(&updatedRegion)
			d.ctx.storeMeta.regions[d.regionId] = &updatedRegion
			d.peer.removePeerCache(cc.NodeId)

			d.RaftGroup.ApplyConfChange(cc)
			if cc.NodeId == d.PeerId() {
				log.Infof("Destroying node %v", cc.NodeId)
				destroyed = true
			} else {
				meta.WriteRegionState(kvWB, d.Region(), rspb.PeerState_Normal)
			}
			return
		}
	}

	// normal entry:
	if len(m.Data) == 0 {
		return
	}

	var r raft_cmdpb.RaftCmdRequest
	err := r.Unmarshal(m.Data)
	if err != nil {
		log.Panicf("%v(%v):%v", m.Data, m.EntryType, err)
	}

	req = &r

	regionErr := util.CheckRegionEpoch(&r, d.Region(), true)
	if regionErr != nil {
		if p != nil {
			BindRespError(p, regionErr)
		}
		return
	}

	if r.AdminRequest != nil {
		if len(r.Requests) > 0 {
			panic(r)
		}
		switch r.AdminRequest.CmdType {
		case raft_cmdpb.AdminCmdType_CompactLog:
			// This is done async. Updating truncate state does not need to wait on actual gc to complete.
			compactIdx := r.AdminRequest.CompactLog.CompactIndex
			compactTerm := r.AdminRequest.CompactLog.CompactTerm

			if compactIdx > applied {
				// Leader uses its applied index to coordinate cluster wide log compaction, but this
				// follower can be lagging behind. Adjust use its own compactIdx
				compactIdx = applied
				term, err := d.peer.RaftGroup.Raft.RaftLog.Term(compactIdx)
				if err != nil {
					panic(err)
				}
				compactTerm = term
			}

			if d.peerStorage.applyState.TruncatedState.Index > compactIdx {
				//// ignore
			} else {
				d.ScheduleCompactLog(compactIdx)
				d.peerStorage.applyState.TruncatedState = &rspb.RaftTruncatedState{
					Index: compactIdx,
					Term:  compactTerm,
				}

				if compactIdx > applied {
					log.Panicf("Node %v %v > %v", d.PeerId(), compactIdx, applied)
				}

				// there is no response gc callback.
			}
		case raft_cmdpb.AdminCmdType_Split:
			splitReq := r.AdminRequest.Split

			var updatedRegion metapb.Region
			err = util.CloneMsg(d.Region(), &updatedRegion)
			if err != nil {
				panic(err)
			}

			log.Warningf("%v splitting on %v (%v) (%v, %v), current %v", d.Tag, splitReq, r.Header.RegionEpoch, m.Index, m.Term, updatedRegion)

			if len(splitReq.NewPeerIds) != len(updatedRegion.Peers) {
				log.Panicf("peer mismatch %v (%v), %v", splitReq, r.Header.RegionEpoch, updatedRegion)
			}

			err := util.CheckKeyInRegion(splitReq.SplitKey, &updatedRegion)
			if err != nil {
				log.Errorf("%v got wrong split %v, %v", d.Tag, splitReq, err)
				if p != nil {
					BindRespError(p, err)
				}
				return
			}

			if p != nil {
				p.AdminResponse = &raft_cmdpb.AdminResponse{CmdType: raft_cmdpb.AdminCmdType_Split}
			}

			curEndKey := updatedRegion.EndKey
			updatedRegion.EndKey = splitReq.SplitKey
			updatedRegion.RegionEpoch.Version++

			newRegion := &metapb.Region{
				Id: splitReq.NewRegionId,
				Peers: func() []*metapb.Peer {
					var ret []*metapb.Peer
					for idx, p := range updatedRegion.Peers {
						ret = append(ret, &metapb.Peer{
							Id:      splitReq.NewPeerIds[idx],
							StoreId: p.StoreId,
						})
					}
					return ret
				}(),
				StartKey: splitReq.SplitKey,
				EndKey:   curEndKey,
				RegionEpoch: func() *metapb.RegionEpoch {
					var ret metapb.RegionEpoch
					ret.ConfVer = updatedRegion.RegionEpoch.ConfVer
					ret.Version = updatedRegion.RegionEpoch.Version
					return &ret
				}(),
			}

			overlap := d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{&updatedRegion})
			if overlap == nil {
				panic(d.ctx.storeMeta.regionRanges)
			}
			d.SetRegion(&updatedRegion)
			d.ctx.storeMeta.regions[d.regionId] = &updatedRegion

			log.Warningf("%v (%v) splitting %v %v -> %v at (index %v, term %v) to region %v",
				d.Tag, d.Region().RegionEpoch, d.Region().StartKey,
				curEndKey, d.Region().EndKey, m.Index, m.Term, splitReq)

			meta.WriteRegionState(kvWB, newRegion, rspb.PeerState_Normal)
			meta.WriteRegionState(kvWB, d.Region(), rspb.PeerState_Normal)

			if len(updatedRegion.Peers) == 0 {
				panic(updatedRegion)
			}

			d.ctx.storeMeta.regions[newRegion.Id] = newRegion
			overlap = d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{newRegion})
			if overlap != nil {
				panic(d.ctx.storeMeta.regionRanges)
			}

			peer, err := createPeer(d.ctx.store.Id, d.ctx.cfg, d.ctx.regionTaskSender, d.ctx.engine, newRegion)

			d.ctx.router.register(peer)
			if err != nil {
				panic(err)
			}

			err = d.ctx.router.send(newRegion.Id, message.Msg{RegionID: newRegion.Id, Type: message.MsgTypeStart})
			if err != nil {
				panic(err)
			}

			if p != nil {
				p.AdminResponse.Split = &raft_cmdpb.SplitResponse{
					Regions: []*metapb.Region{newRegion, &updatedRegion},
				}
			}

		default:
			panic("impossible")
		}
		return
	}

	for _, cmd := range r.Requests {
		switch cmd.CmdType {
		case raft_cmdpb.CmdType_Snap:
			if cb == nil {
				continue
			}
			p.Responses = append(p.Responses, &raft_cmdpb.Response{})

		case raft_cmdpb.CmdType_Get:
			if cb == nil {
				continue
			}

			err := util.CheckKeyInRegion(cmd.Get.Key, d.Region())
			if err != nil {
				log.Errorf("%v, %v:%v", m.Index, m.Term, err)
				BindRespError(p, err)
			} else {
				p.Responses = append(p.Responses, &raft_cmdpb.Response{})
			}

		case raft_cmdpb.CmdType_Put:
			err := util.CheckKeyInRegion(cmd.Put.Key, d.Region())
			if err != nil {
				if cb == nil {
					continue
				}
				log.Errorf("%v, %v:%v", m.Index, m.Term, err)
				BindRespError(p, err)
			} else {
				kvWB.SetCF(cmd.Put.Cf, cmd.Put.Key, cmd.Put.Value)
				if cb == nil {
					continue
				}
				p.Responses = append(p.Responses, &raft_cmdpb.Response{CmdType: cmd.CmdType})
			}

		case raft_cmdpb.CmdType_Delete:
			err := util.CheckKeyInRegion(cmd.Delete.Key, d.Region())
			if err != nil {
				if cb == nil {
					continue
				}
				log.Errorf("%v error in deleting key:%v (%v,%v):%v", d.Tag, cmd.Delete.Key, m.Index, m.Term, err)
				BindRespError(p, err)
			} else {
				kvWB.DeleteCF(cmd.Delete.Cf, cmd.Delete.Key)
				if cb == nil {
					continue
				}
				p.Responses = append(p.Responses, &raft_cmdpb.Response{CmdType: cmd.CmdType})
			}

		default:
			panic("unhandled message")
		}
	}

	return
}

func (d *peerMsgHandler) HandleRaftReady() {
	if d.stopped {
		return
	}
	// Your Code Here (2B).
	if !d.peer.RaftGroup.HasReady() {
		return
	}

	startTime := time.Now()
	rd := d.peer.RaftGroup.Ready()
	// save state, this includes the hard state and flush logs to stable storage
	snapshotRet, err := d.peer.peerStorage.SaveReadyState(&rd)
	if err != nil {
		panic(err)
	}

	kvWB := engine_util.WriteBatch{}
	if snapshotRet != nil {
		// if this node is just joining a group with a snapshot from leader, also restore
		// the region state including version/conf numbering
		d.SetRegion(snapshotRet.Region)

		// update the region info in memory from the snapshot
		d.ctx.storeMeta.regions[d.regionId] = d.Region()
		d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: d.Region()})

		d.peer.peerCache = map[uint64]*metapb.Peer{}
		for _, p := range d.Region().Peers {
			d.insertPeerCache(p)
		}
	}

	// The peer can be just started after restoring snapshot, or just started but without any
	// need of snapshot, in case system reuses peer id (which it really should not be doing).
	// Persist the region info and set it as normal here.
	if d.RaftGroup.Raft.IsMember(d.PeerId()) {
		meta.WriteRegionState(&kvWB, d.Region(), rspb.PeerState_Normal)
	}

	applied := d.peerStorage.applyState.AppliedIndex
	// send messages here:
	for _, m := range rd.Messages {
		err := d.peer.sendRaftMessage(m, d.ctx.trans)
		if err != nil {
			log.Debugf("%v failed sending message (%v, %v) to %v got err:%v",
				d.Tag, m.Index, m.Term, m.To, err)
			// continue
		}
	}

	// apply the committed entries to the state machine
	pending := make(map[*message.Callback]outStandingReq)

	destroyed := false
	for _, m := range rd.CommittedEntries {
		if m.Index <= d.peerStorage.applyState.AppliedIndex {
			continue
		}

		proposal, ok := d.proposals[m.Index]
		delete(d.proposals, m.Index)

		var p raft_cmdpb.RaftCmdResponse
		p.Header = &raft_cmdpb.RaftResponseHeader{
			Error:       nil,
			CurrentTerm: d.peer.Term(),
		}

		if !ok {
			// this is a replica
			_, dd := d.processCmd(nil, &m, &kvWB, nil, applied)
			destroyed = destroyed || dd
		} else {
			if proposal.term != m.Term {
				p = *ErrRespStaleCommand(m.Term)
				log.Warningf("%v got index %v term mismatch (%v -> %v), but that's fine, error it out and let caller retry",
					d.Tag, m.Index, m.Term, proposal.term)
				_, dd := d.processCmd(nil, &m, &kvWB, nil, applied)
				pending[proposal.cb] = outStandingReq{nil, &p}
				destroyed = destroyed || dd
			} else {
				req, dd := d.processCmd(proposal.cb, &m, &kvWB, &p, applied)
				pending[proposal.cb] = outStandingReq{req, &p}
				destroyed = destroyed || dd
			}
		}

		if d.peerStorage.applyState.AppliedIndex+1 != m.Index {
			panic("here")
		}

		d.peerStorage.applyState.AppliedIndex = m.Index
		kvWB.SetMeta(meta.ApplyStateKey(d.Region().Id), d.peerStorage.applyState)
	}

	if kvWB.Len() > 0 {
		err = d.peerStorage.Engines.WriteKV(&kvWB)
		if err != nil {
			log.Panicf("write error: %v", err)
		}
	}

	// Now replay the entire command for read
	for cb, outstanding := range pending {
		if outstanding.p.Header.Error != nil {
			log.Errorf("%v error at %v",
				d.Tag,
				outstanding.p.Header)
			continue
		}

		regionErr := util.CheckRegionEpoch(outstanding.req, d.Region(), true)
		if regionErr != nil {
			log.Errorf("%v got region epoch mismatch on reading: %v vs %v",
				d.Tag,
				outstanding.req.Header.RegionEpoch, d.Region().RegionEpoch)
			// error this out
			BindRespError(outstanding.p, regionErr)
			continue
		}

		for idx, cmd := range outstanding.req.Requests {
			switch cmd.CmdType {
			case raft_cmdpb.CmdType_Snap:
				cb.Txn = d.peerStorage.Engines.Kv.NewTransaction(false)
				outstanding.p.Responses[idx] = &raft_cmdpb.Response{
					CmdType: cmd.CmdType,
					Snap: &raft_cmdpb.SnapResponse{
						Region: &metapb.Region{
							Id:          d.Region().Id,
							StartKey:    d.Region().StartKey,
							EndKey:      d.Region().EndKey,
							RegionEpoch: d.Region().RegionEpoch,
							Peers:       d.Region().Peers,
						},
					},
				}
			case raft_cmdpb.CmdType_Get:
				txn := d.peerStorage.Engines.Kv.NewTransaction(false)
				val, err := txn.Get(engine_util.KeyWithCF(cmd.Get.Cf, cmd.Get.Key))
				if err != nil {
					txn.Discard()
					BindRespError(outstanding.p, err)
				} else {
					data, err := val.Value()
					if err != nil {
						panic(err)
					}

					txn.Discard()
					outstanding.p.Responses[idx] = &raft_cmdpb.Response{
						CmdType: cmd.CmdType,
						Get:     &raft_cmdpb.GetResponse{Value: data},
					}
				}
			}
		}
	}

	// finally, we are safe to issue response
	for cb, o := range pending {
		cb.Done(o.p)
	}

	d.peer.RaftGroup.Advance(rd)

	completion := time.Since(startTime)
	if completion > time.Second {
		log.Errorf("%v took %v to write to kv store", d.Tag, completion)
	}

	if destroyed {
		d.destroyPeer()
		for idx, pendingReq := range d.proposals {
			log.Warningf("%v is destroyed, canceling req %v", d.Tag, idx)
			var p raft_cmdpb.RaftCmdResponse
			BindRespError(&p, &util.ErrNotLeader{RegionId: d.regionId})
			pendingReq.cb.Done(&p)
		}
		d.proposals = nil
	}
}

func (d *peerMsgHandler) HandleMsg(msg message.Msg) {
	switch msg.Type {
	case message.MsgTypeRaftMessage:
		raftMsg := msg.Data.(*rspb.RaftMessage)
		if err := d.onRaftMsg(raftMsg); err != nil {
			log.Errorf("%s handle raft message error %v", d.Tag, err)
		}
	case message.MsgTypeRaftCmd:
		raftCMD := msg.Data.(*message.MsgRaftCmd)
		d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
	case message.MsgTypeTick:
		d.onTick()
	case message.MsgTypeSplitRegion:
		split := msg.Data.(*message.MsgSplitRegion)
		d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKey, split.Callback)
	case message.MsgTypeRegionApproximateSize:
		d.onApproximateRegionSize(msg.Data.(uint64))
	case message.MsgTypeGcSnap:
		gcSnap := msg.Data.(*message.MsgGCSnap)
		d.onGCSnap(gcSnap.Snaps)
	case message.MsgTypeStart:
		d.startTicker()
	}
}

func (d *peerMsgHandler) preProposeRaftCommand(req *raft_cmdpb.RaftCmdRequest) error {
	// Check store_id, make sure that the msg is dispatched to the right place.
	if err := util.CheckStoreID(req, d.storeID()); err != nil {
		return err
	}

	// Check whether the store has the right peer to handle the request.
	regionID := d.regionId
	leaderID := d.LeaderId()
	if !d.IsLeader() {
		leader := d.getPeerFromCache(leaderID)
		return &util.ErrNotLeader{RegionId: regionID, Leader: leader}
	}
	// peer_id must be the same as peer's.
	if err := util.CheckPeerID(req, d.PeerId()); err != nil {
		return err
	}
	// Check whether the term is stale.
	if err := util.CheckTerm(req, d.Term()); err != nil {
		return err
	}
	err := util.CheckRegionEpoch(req, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		// Attach the region which might be split from the current region. But it doesn't
		// matter if the region is not split from the current region. If the region meta
		// received by the TiKV driver is newer than the meta cached in the driver, the meta is
		// updated.
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		return errEpochNotMatching
	}
	return err
}

// Send the commands from the application to the raft module
func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	if msg.AdminRequest != nil && msg.AdminRequest.CmdType == raft_cmdpb.AdminCmdType_TransferLeader {
		if len(msg.Requests) != 0 {
			panic(msg)
		}
		resp := &raft_cmdpb.RaftCmdResponse{
			Header: &raft_cmdpb.RaftResponseHeader{
				Error:       nil,
				Uuid:        nil,
				CurrentTerm: d.RaftGroup.Raft.Term,
			}}
		d.RaftGroup.TransferLeader(msg.AdminRequest.TransferLeader.Peer.Id)
		resp.AdminResponse = &raft_cmdpb.AdminResponse{
			CmdType:        raft_cmdpb.AdminCmdType_TransferLeader,
			TransferLeader: &raft_cmdpb.TransferLeaderResponse{},
		}
		cb.Done(resp)
		return
	}

	err := d.preProposeRaftCommand(msg)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
	// Your Code Here (2B).
	if msg.AdminRequest != nil && msg.AdminRequest.CmdType == raft_cmdpb.AdminCmdType_ChangePeer {
		// There can be at most one pending peer adding/removing, so we check if this is done.
		if len(msg.Requests) != 0 {
			panic(msg)
		}
		resp := &raft_cmdpb.RaftCmdResponse{
			Header: &raft_cmdpb.RaftResponseHeader{
				Error:       nil,
				Uuid:        nil,
				CurrentTerm: d.RaftGroup.Raft.Term,
			}}
		changeReq := msg.AdminRequest.ChangePeer
		switch changeReq.ChangeType {
		case pb.ConfChangeType_AddNode:
			if d.RaftGroup.Raft.IsMember(changeReq.Peer.Id) {
				// done
			} else {
				// raft protocol will dedup this
				ctx, err := msg.Marshal()
				if err != nil {
					panic(err)
				}

				log.Infof("%v propose adding node %v", d.Tag, changeReq.Peer.Id)
				err = d.RaftGroup.ProposeConfChange(pb.ConfChange{
					ChangeType: pb.ConfChangeType_AddNode,
					NodeId:     changeReq.Peer.Id,
					Context:    ctx,
				})

				if err != nil {
					cb.Done(ErrResp(err))
					return
				}
			}

			resp.AdminResponse = &raft_cmdpb.AdminResponse{
				CmdType: raft_cmdpb.AdminCmdType_ChangePeer,
				ChangePeer: &raft_cmdpb.ChangePeerResponse{
					Region: d.Region(),
				},
			}
			cb.Done(resp)
		case pb.ConfChangeType_RemoveNode:
			if !d.RaftGroup.Raft.IsMember(changeReq.Peer.Id) {
				// done
			} else {
				ctx, err := msg.Marshal()
				if err != nil {
					panic(err)
				}
				// raft protocol will dedup this
				log.Infof("%v propose removing node %v", d.Tag, changeReq.Peer.Id)
				err = d.RaftGroup.ProposeConfChange(pb.ConfChange{
					ChangeType: pb.ConfChangeType_RemoveNode,
					NodeId:     changeReq.Peer.Id,
					Context:    ctx,
				})

				if err != nil {
					cb.Done(ErrResp(err))
					return
				}
			}
			resp.AdminResponse = &raft_cmdpb.AdminResponse{
				CmdType: raft_cmdpb.AdminCmdType_ChangePeer,
				ChangePeer: &raft_cmdpb.ChangePeerResponse{
					Region: d.Region(),
				},
			}
			cb.Done(resp)
		}
		return
	}

	index := d.peer.nextProposalIndex()
	term := d.peer.Term()
	data, err := msg.Marshal()
	if err != nil {
		log.Panic(err)
	}
	err = d.peer.RaftGroup.Propose(data)
	if err != nil {
		log.Errorf("Proposal got error %v", err)
		cb.Done(ErrResp(err))
	}
	d.peer.proposals[index] = proposal{
		index: index,
		term:  term,
		cb:    cb,
	}
}

func (d *peerMsgHandler) onTick() {
	if d.stopped {
		return
	}
	d.ticker.tickClock()
	if d.ticker.isOnTick(PeerTickRaft) {
		d.onRaftBaseTick()
	}
	if d.ticker.isOnTick(PeerTickRaftLogGC) {
		d.onRaftGCLogTick()
	}
	if d.ticker.isOnTick(PeerTickSchedulerHeartbeat) {
		d.onSchedulerHeartbeatTick()
	}
	if d.ticker.isOnTick(PeerTickSplitRegionCheck) {
		d.onSplitRegionCheckTick()
	}
	d.ctx.tickDriverSender <- d.regionId
}

func (d *peerMsgHandler) startTicker() {
	d.ticker = newTicker(d.regionId, d.ctx.cfg)
	d.ctx.tickDriverSender <- d.regionId
	d.ticker.schedule(PeerTickRaft)
	d.ticker.schedule(PeerTickRaftLogGC)
	d.ticker.schedule(PeerTickSplitRegionCheck)
	d.ticker.schedule(PeerTickSchedulerHeartbeat)
}

func (d *peerMsgHandler) onRaftBaseTick() {
	d.RaftGroup.Tick()
	d.ticker.schedule(PeerTickRaft)
}

func (d *peerMsgHandler) ScheduleCompactLog(truncatedIndex uint64) {
	raftLogGCTask := &runner.RaftLogGCTask{
		RaftEngine: d.ctx.engine.Raft,
		RegionID:   d.regionId,
		StartIdx:   d.LastCompactedIdx,
		EndIdx:     truncatedIndex + 1,
	}
	d.LastCompactedIdx = raftLogGCTask.EndIdx
	d.ctx.raftLogGCTaskSender <- raftLogGCTask
}

func (d *peerMsgHandler) onRaftMsg(msg *rspb.RaftMessage) error {
	log.Debugf("%s handle raft message %s from %d to %d",
		d.Tag, msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId())
	if !d.validateRaftMessage(msg) {
		return nil
	}
	if d.stopped {
		return nil
	}
	if msg.GetIsTombstone() {
		// we receive a message tells us to remove self.
		d.handleGCPeerMsg(msg)
		return nil
	}
	if d.checkMessage(msg) {
		return nil
	}
	key, err := d.checkSnapshot(msg)
	if err != nil {
		return err
	}
	if key != nil {
		// If the snapshot file is not used again, then it's OK to
		// delete them here. If the snapshot file will be reused when
		// receiving, then it will fail to pass the check again, so
		// missing snapshot files should not be noticed.
		s, err1 := d.ctx.snapMgr.GetSnapshotForApplying(*key)
		if err1 != nil {
			return err1
		}
		d.ctx.snapMgr.DeleteSnapshot(*key, s, false)
		return nil
	}
	d.insertPeerCache(msg.GetFromPeer())
	err = d.RaftGroup.Step(*msg.GetMessage())
	if err != nil {
		return err
	}
	if d.AnyNewPeerCatchUp(msg.FromPeer.Id) {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	return nil
}

// return false means the message is invalid, and can be ignored.
func (d *peerMsgHandler) validateRaftMessage(msg *rspb.RaftMessage) bool {
	regionID := msg.GetRegionId()
	from := msg.GetFromPeer()
	to := msg.GetToPeer()
	log.Debugf("[region %d] handle raft message %s from %d to %d", regionID, msg, from.GetId(), to.GetId())
	if to.GetStoreId() != d.storeID() {
		log.Warnf("[region %d] store not match, to store id %d, mine %d, ignore it",
			regionID, to.GetStoreId(), d.storeID())
		return false
	}
	if msg.RegionEpoch == nil {
		log.Errorf("[region %d] missing epoch in raft message, ignore it", regionID)
		return false
	}
	return true
}

/// Checks if the message is sent to the correct peer.
///
/// Returns true means that the message can be dropped silently.
func (d *peerMsgHandler) checkMessage(msg *rspb.RaftMessage) bool {
	fromEpoch := msg.GetRegionEpoch()
	isVoteMsg := util.IsVoteMessage(msg.Message)
	fromStoreID := msg.FromPeer.GetStoreId()

	// Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
	// a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
	//  We should ignore this stale message and let 2 remove itself after
	//  applying the ConfChange log.
	// b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
	//  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
	// c. 2 is isolated but can communicate with 3. 1 removes 3.
	//  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
	// d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
	//  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
	// e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
	//  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
	//  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
	//  rejoin the raft group again.
	// f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
	//  unlike case e, 2 will be stale forever.
	// TODO: for case f, if 2 is stale for a long time, 2 will communicate with scheduler and scheduler will
	// tell 2 is stale, so 2 can remove itself.
	region := d.Region()
	if util.IsEpochStale(fromEpoch, region.RegionEpoch) && util.FindPeer(region, fromStoreID) == nil {
		// The message is stale and not in current region.
		log.Warningf("epoch mismatch and peer does not exist. %v from:%v, current:%v, from %v, %v in %v", d.Tag, fromEpoch, region.RegionEpoch, msg.FromPeer, fromStoreID, region)
		handleStaleMsg(d.ctx.trans, msg, region.RegionEpoch, isVoteMsg)
		return true
	}
	target := msg.GetToPeer()
	if target.Id < d.PeerId() {
		log.Warningf("%s target peer ID %d is less than %d, msg maybe stale", d.Tag, target.Id, d.PeerId())
		return true
	} else if target.Id > d.PeerId() {
		if d.MaybeDestroy() {
			log.Infof("%s is stale as received a larger peer %s, destroying", d.Tag, target)
			d.destroyPeer()
			d.ctx.router.sendStore(message.NewMsg(message.MsgTypeStoreRaftMessage, msg))
		}
		return true
	}
	return false
}

func handleStaleMsg(trans Transport, msg *rspb.RaftMessage, curEpoch *metapb.RegionEpoch,
	needGC bool) {
	regionID := msg.RegionId
	fromPeer := msg.FromPeer
	toPeer := msg.ToPeer
	msgType := msg.Message.GetMsgType()

	if !needGC {
		log.Infof("[region %d] raft message %s is stale, current %v ignore it",
			regionID, msgType, curEpoch)
		return
	}
	gcMsg := &rspb.RaftMessage{
		RegionId:    regionID,
		FromPeer:    fromPeer,
		ToPeer:      toPeer,
		RegionEpoch: curEpoch,
		IsTombstone: true,
	}
	if err := trans.Send(gcMsg); err != nil {
		log.Errorf("[region %d] send message failed %v", regionID, err)
	}
}

func (d *peerMsgHandler) handleGCPeerMsg(msg *rspb.RaftMessage) {
	fromEpoch := msg.RegionEpoch
	if !util.IsEpochStale(d.Region().RegionEpoch, fromEpoch) {
		return
	}
	if !util.PeerEqual(d.Meta, msg.ToPeer) {
		log.Infof("%s receive stale gc msg, ignore", d.Tag)
		return
	}
	log.Infof("%s peer %s receives gc message, trying to remove", d.Tag, msg.ToPeer)
	if d.MaybeDestroy() {
		d.destroyPeer()
	}
}

// Returns `None` if the `msg` doesn't contain a snapshot or it contains a snapshot which
// doesn't conflict with any other snapshots or regions. Otherwise a `snap.SnapKey` is returned.
func (d *peerMsgHandler) checkSnapshot(msg *rspb.RaftMessage) (*snap.SnapKey, error) {
	if msg.Message.Snapshot == nil {
		return nil, nil
	}
	regionID := msg.RegionId
	snapshot := msg.Message.Snapshot
	key := snap.SnapKeyFromRegionSnap(regionID, snapshot)
	snapData := new(rspb.RaftSnapshotData)
	err := snapData.Unmarshal(snapshot.Data)
	if err != nil {
		return nil, err
	}
	snapRegion := snapData.Region
	peerID := msg.ToPeer.Id
	var contains bool
	for _, peer := range snapRegion.Peers {
		if peer.Id == peerID {
			contains = true
			break
		}
	}
	if !contains {
		log.Infof("%s %s doesn't contain peer %d, skip", d.Tag, snapRegion, peerID)
		return &key, nil
	}
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	if !util.RegionEqual(meta.regions[d.regionId], d.Region()) {
		if !d.isInitialized() {
			log.Infof("%s stale delegate detected, skip", d.Tag)
			return &key, nil
		} else {
			panic(fmt.Sprintf("%s meta corrupted %s != %s", d.Tag, meta.regions[d.regionId], d.Region()))
		}
	}

	existRegions := meta.getOverlapRegions(snapRegion)
	for _, existRegion := range existRegions {
		if existRegion.GetId() == snapRegion.GetId() {
			continue
		}
		log.Infof("%s region overlapped %s %s", d.Tag, existRegion, snapRegion)
		return &key, nil
	}

	// check if snapshot file exists.
	_, err = d.ctx.snapMgr.GetSnapshotForApplying(key)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (d *peerMsgHandler) destroyPeer() {
	log.Infof("%s starts destroy", d.Tag)
	regionID := d.regionId
	// We can't destroy a peer which is applying snapshot.
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	isInitialized := d.isInitialized()
	if err := d.Destroy(d.ctx.engine, false); err != nil {
		// If not panic here, the peer will be recreated in the next restart,
		// then it will be gc again. But if some overlap region is created
		// before restarting, the gc action will delete the overlap region's
		// data too.
		panic(fmt.Sprintf("%s destroy peer %v", d.Tag, err))
	}
	d.ctx.router.close(regionID)
	d.stopped = true
	if isInitialized && meta.regionRanges.Delete(&regionItem{region: d.Region()}) == nil {
		panic(d.Tag + " meta corruption detected")
	}
	if _, ok := meta.regions[regionID]; !ok {
		panic(d.Tag + " meta corruption detected")
	}
	delete(meta.regions, regionID)
}

func (d *peerMsgHandler) findSiblingRegion() (result *metapb.Region) {
	meta := d.ctx.storeMeta
	meta.RLock()
	defer meta.RUnlock()
	item := &regionItem{region: d.Region()}
	meta.regionRanges.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem).region
		return true
	})
	return
}

func (d *peerMsgHandler) onRaftGCLogTick() {
	d.ticker.schedule(PeerTickRaftLogGC)
	if !d.IsLeader() {
		return
	}

	appliedIdx := d.peerStorage.AppliedIndex()
	firstIdx, _ := d.peerStorage.FirstIndex()
	var compactIdx uint64
	if appliedIdx > firstIdx && appliedIdx-firstIdx >= d.ctx.cfg.RaftLogGcCountLimit {
		compactIdx = appliedIdx
	} else {
		return
	}

	y.Assert(compactIdx > 0)
	compactIdx -= 1
	if compactIdx < firstIdx {
		// In case compact_idx == first_idx before subtraction.
		return
	}

	term, err := d.RaftGroup.Raft.RaftLog.Term(compactIdx)
	if err != nil {
		log.Panicf("appliedIdx: %d, firstIdx: %d, compactIdx: %d", appliedIdx, firstIdx, compactIdx)
	}

	// Create a compact log request and notify directly.
	regionID := d.regionId
	request := newCompactLogRequest(regionID, d.Meta, compactIdx, term)
	d.proposeRaftCommand(request, nil)
}

func (d *peerMsgHandler) onSplitRegionCheckTick() {
	d.ticker.schedule(PeerTickSplitRegionCheck)
	// To avoid frequent scan, we only add new scan tasks if all previous tasks
	// have finished.
	if len(d.ctx.splitCheckTaskSender) > 0 {
		return
	}

	if !d.IsLeader() {
		return
	}
	if d.ApproximateSize != nil && d.SizeDiffHint < d.ctx.cfg.RegionSplitSize/8 {
		return
	}
	d.ctx.splitCheckTaskSender <- &runner.SplitCheckTask{
		Region: d.Region(),
	}
	d.SizeDiffHint = 0
}

func (d *peerMsgHandler) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKey []byte, cb *message.Callback) {
	if err := d.validateSplitRegion(regionEpoch, splitKey); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	region := d.Region()
	d.ctx.schedulerTaskSender <- &runner.SchedulerAskSplitTask{
		Region:   region,
		SplitKey: splitKey,
		Peer:     d.Meta,
		Callback: cb,
	}
}

func (d *peerMsgHandler) validateSplitRegion(epoch *metapb.RegionEpoch, splitKey []byte) error {
	if len(splitKey) == 0 {
		err := errors.Errorf("%s split key should not be empty", d.Tag)
		log.Error(err)
		return err
	}

	if !d.IsLeader() {
		// region on this store is no longer leader, skipped.
		log.Infof("%s not leader, skip", d.Tag)
		return &util.ErrNotLeader{
			RegionId: d.regionId,
			Leader:   d.getPeerFromCache(d.LeaderId()),
		}
	}

	region := d.Region()
	latestEpoch := region.GetRegionEpoch()

	// This is a little difference for `check_region_epoch` in region split case.
	// Here we just need to check `version` because `conf_ver` will be update
	// to the latest value of the peer, and then send to Scheduler.
	if latestEpoch.Version != epoch.Version {
		log.Infof("%s epoch changed, retry later, prev_epoch: %s, epoch %s",
			d.Tag, latestEpoch, epoch)
		return &util.ErrEpochNotMatch{
			Message: fmt.Sprintf("%s epoch changed %s != %s, retry later", d.Tag, latestEpoch, epoch),
			Regions: []*metapb.Region{region},
		}
	}
	return nil
}

func (d *peerMsgHandler) onApproximateRegionSize(size uint64) {
	d.ApproximateSize = &size
}

func (d *peerMsgHandler) onSchedulerHeartbeatTick() {
	d.ticker.schedule(PeerTickSchedulerHeartbeat)

	if !d.IsLeader() {
		return
	}
	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
}

func (d *peerMsgHandler) onGCSnap(snaps []snap.SnapKeyWithSending) {
	compactedIdx := d.peerStorage.truncatedIndex()
	compactedTerm := d.peerStorage.truncatedTerm()
	for _, snapKeyWithSending := range snaps {
		key := snapKeyWithSending.SnapKey
		if snapKeyWithSending.IsSending {
			snap, err := d.ctx.snapMgr.GetSnapshotForSending(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			if key.Term < compactedTerm || key.Index < compactedIdx {
				log.Infof("%s snap file %s has been compacted, delete", d.Tag, key)
				d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
			} else if fi, err1 := snap.Meta(); err1 == nil {
				modTime := fi.ModTime()
				if time.Since(modTime) > 4*time.Hour {
					log.Infof("%s snap file %s has been expired, delete", d.Tag, key)
					d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
				}
			}
		} else if key.Term <= compactedTerm &&
			(key.Index < compactedIdx || key.Index == compactedIdx) {
			log.Infof("%s snap file %s has been applied, delete", d.Tag, key)
			a, err := d.ctx.snapMgr.GetSnapshotForApplying(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			d.ctx.snapMgr.DeleteSnapshot(key, a, false)
		}
	}
}

func newAdminRequest(regionID uint64, peer *metapb.Peer) *raft_cmdpb.RaftCmdRequest {
	return &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId: regionID,
			Peer:     peer,
		},
	}
}

func newCompactLogRequest(regionID uint64, peer *metapb.Peer, compactIndex, compactTerm uint64) *raft_cmdpb.RaftCmdRequest {
	req := newAdminRequest(regionID, peer)
	req.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogRequest{
			CompactIndex: compactIndex,
			CompactTerm:  compactTerm,
		},
	}
	return req
}
