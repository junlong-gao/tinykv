// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"math/rand"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	Peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	// tracks the last time voted, only valid in follower state
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes         map[uint64]bool
	votesRejected map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int

	electionTimeoutConfig int

	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// number of ticks since it reached last electionTimeout
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex  uint64
	pendingConfChange *pb.ConfChange
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).

	var r Raft

	// Reset log state:
	r.RaftLog = newLogWithAppliedIndex(c.Storage, c.Applied)
	initHardState, _, err := c.Storage.InitialState()
	if err != nil {
		log.Panicf("%v", err)
	}
	r.Vote = initHardState.Vote
	r.Term = initHardState.Term

	r.id = c.ID
	r.heartbeatTimeout = c.HeartbeatTick
	r.electionTimeoutConfig = c.ElectionTick

	r.Prs = make(map[uint64]*Progress)
	for _, id := range c.Peers {
		if id == None {
			panic("No one should have id 0")
		}
		r.Prs[id] = &Progress{Match: 0, Next: 0}
	}

	r.RaftLog.pendingSnapshot = nil

	r.PendingConfIndex = r.findPendingConfChangeIndex()
	r.becomeFollower(r.Term, None)
	return &r
}

// move the message to the outbound queue
func (r *Raft) send(m pb.Message) {
	m.From = r.id
	m.Term = r.Term
	r.msgs = append(r.msgs, m)
}

func (r *Raft) getSoftState() SoftState {
	return SoftState{Lead: r.Lead, RaftState: r.State}
}

func (r *Raft) getHardState() pb.HardState {
	return pb.HardState{Term: r.Term, Vote: r.Vote, Commit: r.RaftLog.committed}
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	pr := r.Prs[to]
	lastIdx := r.RaftLog.LastIndex()
	if lastIdx < pr.Match {
		log.Panicf("%v < %v (%v)", lastIdx, pr.Match, to)
	}

	// the log contains [FirstIndex, ... and term can be retrieved starting at FirstIndex - 1
	if pr.Next < r.RaftLog.FirstIndex() {
		/*
			    In 'sendAppend', if a leader fails to get term or entries,
				the leader requests snapshot by sending 'MessageType_MsgSnapshot' type message.
		*/
		snap, err := r.RaftLog.storage.Snapshot()
		if err == ErrSnapshotTemporarilyUnavailable {
			// no need to respond anything, waiting for next tick...
			return false
		}
		if err != nil {
			panic(err)
		}
		log.Debugf("Leader %v sending snapshot %v to %v", r.id, snap.Metadata, to)
		r.send(pb.Message{
			MsgType: pb.MessageType_MsgSnapshot,
			To:      to,
			Commit:  r.RaftLog.committed,

			Snapshot: &snap,
		})
	} else {
		idx := pr.Next - 1
		log.Debugf("Leader %v append entry with index %v to %v", r.id, idx, to)
		term, err := r.RaftLog.Term(idx)
		if err != nil {
			log.Panic(err)
		}
		ents := r.RaftLog.Slice(idx+1, lastIdx-idx)
		//log.Infof("Node %v sending entries (%v,%v) to %v", r.id, idx, term, to)
		r.send(pb.Message{
			MsgType: pb.MessageType_MsgAppend,
			To:      to,
			Commit:  r.RaftLog.committed,

			// Log matching, follower will check these first:
			Index:   idx,
			LogTerm: term, // this will always be available in log
			Entries: ents,
		})
	}
	return true
}

// Try to replicate to all the followers
func (r *Raft) broadcastAppend() {
	for id, _ := range r.Prs {
		if id == r.id {
			continue
		}
		r.sendAppend(id) // ignore return here
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).

	if r.State != StateFollower {
		r.becomeFollower(m.Term, m.From)
	}

	r.Lead = m.From

	r.electionElapsed = 0
	//log.Infof("Node %v got append (%v,%v) from %v", r.id, m.Index, m.LogTerm, m.From)
	if !r.RaftLog.CheckMatch(m.Index, m.LogTerm) {
		// XXX use committed as a reply can be further optimized by calling r.RaftLog.Compare
		// and get the first conflict index. cf. the dissertation.
		r.send(pb.Message{
			To:      m.From,
			MsgType: pb.MessageType_MsgAppendResponse,
			Reject:  true,

			Index: r.RaftLog.committed, // hint a reply
		})
		if m.Index <= r.RaftLog.committed {
			log.Panicf("Impossible:%v<=%v", m.Index, r.RaftLog.committed)
		}
	} else {
		if len(m.Entries) == 0 {
			r.send(pb.Message{
				To:      m.From,
				MsgType: pb.MessageType_MsgAppendResponse,
				Reject:  false,
				Index:   m.Index,
			})
			m.Commit = min(m.Commit, m.Index)
		} else {
			idx := r.RaftLog.AppendEntries(m.Entries)

			r.send(pb.Message{
				To:      m.From,
				MsgType: pb.MessageType_MsgAppendResponse,
				Reject:  false,
				Index:   idx,
			})
			m.Commit = min(m.Commit, idx)
		}

		if m.Commit > r.RaftLog.committed {
			r.RaftLog.commitTo(m.Commit)
		}
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).

	r.RaftLog.ResetBySnapshot(m.Snapshot)

	r.Prs = make(map[uint64]*Progress) // node removal was in the snapshot
	for _, n := range m.Snapshot.Metadata.ConfState.Nodes {
		if _, ok := r.Prs[n]; !ok {
			r.Prs[n] = &Progress{}
		}
	}

	r.electionElapsed = 0
	r.send(pb.Message{
		To:      m.From,
		MsgType: pb.MessageType_MsgAppendResponse,
		Reject:  false,
		Index:   m.Snapshot.Metadata.Index,
	})
}

func (r *Raft) tryUpdateCommitIdx(newCommitIdx uint64) bool {
	if newCommitIdx <= r.RaftLog.committed {
		return false
	}

	// Need to check if this is from an older term (5.3, 5.4)
	term, err := r.RaftLog.Term(newCommitIdx)
	if err != nil {
		log.Panic(err)
	}
	if term > r.Term {
		log.Panicf("Term goes backwards: %v -> %v", term, r.Term)
	}
	if term != r.Term {
		// assert term < r.Term
		// Leader cannot use an older term entry to advance commit index. cf. Figure 8.
		return false
	}

	// We have a chance to update commit index
	count := 0
	total := 0
	for id, _ := range r.Prs {
		if r.isPendingRemoval(id) {
			continue
		}
		total++
	}
	for id, pr := range r.Prs {
		if r.isPendingRemoval(id) {
			continue
		}

		if pr.Match >= newCommitIdx {
			count++
		}
	}
	if 2*count > total {
		r.RaftLog.commitTo(newCommitIdx)
		r.broadcastAppend() // quickly update followers about this new commit index
		return true
	}
	return false
}

func (r *Raft) handleAppendEntriesResp(m pb.Message) {
	// Your Code Here (2A).

	if r.State != StateLeader {
		return
	}

	if m.Reject {
		if r.Prs[m.From].Next > 0 {
			// If the response of append is duplicated (or leader sent 2 append entries get 2 same responses),
			// don't blindly decrease the next index.
			r.Prs[m.From].Next = max(m.Index+1, r.Prs[m.From].Next-1)
			r.Prs[m.From].Next = max(r.Prs[m.From].Next, r.Prs[m.From].Match)
		}
		r.sendAppend(m.From)
	} else {
		r.Prs[m.From].Match = max(r.Prs[m.From].Match, m.Index)
		r.Prs[m.From].Next = max(r.Prs[m.From].Next, r.Prs[m.From].Match+1)

		if m.From == r.leadTransferee {
			log.Infof("Node %v:  %v is up-to-date, transferring leader ship to %v",
				r.id, m.From, r.leadTransferee)
			r.transferLeader()
		} else {
			r.tryUpdateCommitIdx(r.Prs[m.From].Match)
		}
	}
}

// Try to replicate to all the followers
func (r *Raft) broadcastHeartBeat() {
	for id, _ := range r.Prs {
		if id == r.id {
			continue
		}
		r.sendHeartbeat(id) // ignore return here
	}
}

// sendHeartbeat sends a heartbeat RPC to the given peer. It serves 2 purposes 1) maintain
// leadership 2) allow leader to know which followers are alive
// and replicate data
func (r *Raft) sendHeartbeat(to uint64) {
	if r.State != StateLeader {
		panic(r.id)
	}
	// Your Code Here (2A).
	r.send(pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		Commit:  util.RaftInvalidIndex,
	})
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	if r.State != StateFollower {
		//log.Panicf("Node %v got %v", r.id, m)
		r.becomeFollower(m.Term, m.From)
	}

	r.Lead = m.From

	r.electionElapsed = 0
	r.Lead = m.From
	r.send(pb.Message{
		To:      m.From,
		MsgType: pb.MessageType_MsgHeartbeatResponse,
	})
}

func (r *Raft) handleHeartbeatResp(m pb.Message) {
	// Your Code Here (2A).

	if r.State != StateLeader {
		return
	}

	r.sendAppend(m.From)
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).

	r.State = StateFollower
	if term > r.Term {
		r.Vote = None
	}
	r.Term = term

	r.Lead = lead

	r.electionElapsed = 0
	r.electionTimeout = r.electionTimeoutConfig + rand.Int()%r.electionTimeoutConfig
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).

	if r.State == StateLeader {
		panic("Leader cannot become candidate!")
	}

	r.Term++
	r.State = StateCandidate
	r.electionTimeout = r.electionTimeoutConfig + rand.Int()%r.electionTimeoutConfig
	r.Lead = None
	r.PendingConfIndex = r.findPendingConfChangeIndex()
}

func (r *Raft) campaign() {
	if r.State != StateCandidate {
		log.Panicf("Only candidate can start election!")
	}

	r.Vote = r.id
	r.votes = make(map[uint64]bool)
	r.votesRejected = make(map[uint64]bool)
	for p, _ := range r.Prs {
		if p == r.id {
			r.votes[r.id] = true
			r.votesRejected[r.id] = false
		} else {
			lastIndex := r.RaftLog.LastIndex()
			lastTerm, err := r.RaftLog.Term(lastIndex)
			if err != nil {
				panic(err)
			}

			m := pb.Message{
				MsgType: pb.MessageType_MsgRequestVote,
				To:      p,

				LogTerm: lastTerm,
				Index:   lastIndex,
			}
			r.send(m)
		}
	}

	r.electionElapsed = 0
	if r.countVotes(r.votes) {
		r.becomeLeader()
	}
}

func (r *Raft) handleRequestVote(m pb.Message) {
	if m.From == r.id {
		log.Debugf("Self voting %v, should be handled", m)
	}

	if r.State != StateFollower {
		r.send(pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			To:      m.From,
			From:    r.id,
			Reject:  true,
		})
		return
	}

	// Cast vote
	lastIndex := r.RaftLog.LastIndex()
	lastTerm, err := r.RaftLog.Term(lastIndex)
	if err != nil {
		panic(err)
	}
	// The candidate is voted by us and its log is longer or of the same
	// length but the last entry is newer
	if (r.Vote == None || r.Vote == m.From) &&
		(lastTerm < m.LogTerm ||
			(lastTerm == m.LogTerm && lastIndex <= m.Index)) {
		// Accept
		r.send(pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			To:      m.From,
			From:    r.id,
			Reject:  false,
		})

		r.Vote = m.From
		r.electionElapsed = 0
	} else {
		r.send(pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			To:      m.From,
			From:    r.id,
			Reject:  true,
		})
	}
}

func (r *Raft) handleRequestVoteResp(m pb.Message) {
	if r.State != StateCandidate {
		// From a previous election, do nothing
		return
	}
	if m.Reject {
		r.votesRejected[m.From] = true
		if r.countVotes(r.votesRejected) {
			r.becomeFollower(r.Term, None)
		}
		return
	} else {
		r.votes[m.From] = true
		if r.countVotes(r.votes) {
			r.becomeLeader()
		}
	}
}

func (r *Raft) abortLeaderTransfer() {
	if r.State != StateLeader {
		panic("only leader can do that")
	}

	r.electionTimeout = 0
	r.leadTransferee = None
}

func (r *Raft) transferLeader() {
	if !r.IsMember(r.leadTransferee) {
		log.Panicf("Unexpected loss of node %v", r.leadTransferee)
	}
	r.send(pb.Message{
		MsgType: pb.MessageType_MsgTimeoutNow,
		To:      r.leadTransferee,
	})
}

func (r *Raft) handleLeaderTransfer(m pb.Message) {
	if r.State == StateCandidate {
		return
	}

	if r.State == StateFollower {
		if !r.IsMember(r.Lead) {
			return
		}
		if m.From == r.Lead {
			return
		}
		// Need to preserve m.From, don't use send
		m.To = r.Lead
		m.Term = r.Term
		r.msgs = append(r.msgs, m)
	} else {
		if !r.IsMember(m.From) {
			log.Infof("Node %v is no longer part of the group", m.From)
			return
		}

		if m.From == r.id {
			if r.IsMember(r.leadTransferee) {
				r.abortLeaderTransfer()
			}
			log.Infof("Self transfer, done")
			return
		} else {
			r.abortLeaderTransfer()
			r.leadTransferee = m.From

			if r.Prs[r.leadTransferee].Match == r.RaftLog.LastIndex() {
				log.Infof("Transferring leader %v -> %v", r.id, r.leadTransferee)
				r.transferLeader()
			} else {
				log.Infof("Updating node %v", r.leadTransferee)
				r.sendAppend(r.leadTransferee)
			}
		}
	}
}

func (r *Raft) handleTimeoutNow(m pb.Message) {
	if r.State != StateFollower {
		log.Info("Node %v is not follower, ignoring %v", r.id, m)
		return
	}
	if !r.IsMember(r.id) {
		log.Infof("Node %v is no longer part of the group, ignoring %v", r.id, m)
		return
	}
	r.Term++
	r.becomeCandidate()
	r.campaign()
}

func (r *Raft) countVotes(ballet map[uint64]bool) bool {
	// Got majority, count vote
	voteCount := 0
	total := 0
	for id, _ := range r.Prs {
		if r.isPendingRemoval(id) {
			continue
		}
		total++
	}

	for id, v := range ballet {
		if r.isPendingRemoval(id) {
			continue
		}
		if _, ok := r.Prs[id]; ok && v {
			voteCount++
		}
	}
	return voteCount*2 > total
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term

	if r.State != StateCandidate {
		panic("Only candidates can become a leader!")
	}

	log.Warningf("Node %v becomes leader", r.id)
	r.State = StateLeader
	r.Lead = r.id

	r.heartbeatElapsed = 0

	newLastIdx := r.RaftLog.LastIndex() + 1 // leader will propose a no-op
	for p, _ := range r.Prs {

		if p == r.id {
			r.Prs[p].Next = newLastIdx + 1
			r.Prs[p].Match = newLastIdx
		} else {
			r.Prs[p].Next = newLastIdx // this is where we replicate in log BEFORE we append no-op
			// A follower can lag arbitrarily behind, we will update this on receiving reply
			// followers will get piggy-backed by this too
			r.Prs[p].Match = 0
		}
	}

	// Clear any previous leader transferring progress
	r.abortLeaderTransfer()

	r.PendingConfIndex = r.findPendingConfChangeIndex()

	// On election, the leader immediately triggers broadcasting a no-op entry,
	// this optimizes the likelihood of leadership after election and ensures leader knows for sure how many
	// of the entries in its log can be committed (figure 8 in paper).
	r.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		From:    r.id,
		Entries: []*pb.Entry{
			{Term: r.Term, Index: newLastIdx, Data: nil},
		},
	})
}

func (r *Raft) findPendingConfChangeIndex() uint64 {
	ret := uint64(0)
	// find the pending conf change:
	lastIdx := r.RaftLog.LastIndex()
	for idx := r.RaftLog.applied + 1; idx <= r.RaftLog.LastIndex(); idx++ {
		if lastIdx != r.RaftLog.LastIndex() {
			log.Panicf("%v %v", lastIdx, r.RaftLog.LastIndex())
		}
		ent := r.RaftLog.Slice(idx, 1)[0]
		if ent.EntryType != pb.EntryType_EntryConfChange {
			continue
		}
		ret = idx

		var cc pb.ConfChange
		err := cc.Unmarshal(ent.Data)
		if err != nil {
			panic(err)
		}

		r.pendingConfChange = &cc
	}
	return ret
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
	if _, ok := r.Prs[id]; ok {
		return
	}
	log.Infof("Node %v adding node %v", r.id, id)
	r.Prs[id] = &Progress{
		Match: 0,
		Next:  r.RaftLog.LastIndex(),
	}
}

func (r *Raft) isPendingRemoval(id uint64) bool {
	if r.RaftLog.applied >= r.PendingConfIndex {
		return false
	}
	if r.pendingConfChange.ChangeType != pb.ConfChangeType_RemoveNode {
		return false
	}
	return r.pendingConfChange.NodeId == id
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
	if _, ok := r.Prs[id]; !ok {
		return
	}
	log.Infof("Node %v removing node %v", r.id, id)
	delete(r.Prs, id)
	if r.id == id {
	} else {
		if r.State == StateLeader {
			// XXX this is not needed
			for idx := r.RaftLog.committed + 1; idx <= r.RaftLog.LastIndex() && r.tryUpdateCommitIdx(idx); idx++ {
			}
		} else {
			if r.Lead == id && r.State == StateFollower {
				log.Infof("%v (%v) find leader %v is dead, campagning", r.id, r.Term, id)
				r.becomeCandidate()
				r.campaign()
			}
		}
	}
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	if r.State == StateLeader {
		r.heartbeatElapsed++
		r.electionElapsed++
		if r.electionTimeout >= r.electionTimeout && r.IsMember(r.leadTransferee) {
			log.Infof("Aborting leader ship transfer %v -> %v", r.id, r.leadTransferee)
			r.abortLeaderTransfer()
		}
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.broadcastHeartBeat()
			r.heartbeatElapsed = 0
		}
	} else {
		r.electionElapsed++
		if r.electionElapsed >= r.electionTimeout {
			if r.Lead != None {
				log.Errorf("!!Node %v lost leader %v, campagining", r.id, r.Lead)
			}
			r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
		}
	}
}

func (r *Raft) IsMember(id uint64) bool {
	if id == None {
		return false
	}
	_, ok := r.Prs[id]
	return ok
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).

	// Local messages first: they have no term and return on handling
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		if r.State == StateLeader {
			r.broadcastHeartBeat()
		}
		return nil
	case pb.MessageType_MsgHup:
		if r.State == StateLeader {
		} else {
			r.becomeCandidate()
			r.campaign()
		}
		return nil

	case pb.MessageType_MsgPropose:
		if r.State != StateLeader {
			return &util.ErrNotLeader{}
		} else {
			if r.IsMember(r.leadTransferee) {
				log.Infof("Node %v is transferring leader ship to %v",
					r.id,
					r.leadTransferee)
				return nil
			}

			lastIdx := r.RaftLog.LastIndex()
			lastTerm, err := r.RaftLog.Term(lastIdx)
			if err != nil {
				log.Panic(err)
			}
			m.Index = lastIdx
			m.LogTerm = lastTerm
			for i, _ := range m.Entries {
				if m.Entries[i].EntryType == pb.EntryType_EntryConfChange {
					if r.PendingConfIndex > r.RaftLog.applied {
						// XXX optimize here: don't waste an entry here
						m.Entries[i] = &pb.Entry{EntryType: pb.EntryType_EntryNormal, Data: nil} // an empty entry
					} else {
						var cc pb.ConfChange
						err := cc.Unmarshal(m.Entries[i].Data)
						if err != nil {
							panic(err)
						}

						r.PendingConfIndex = lastIdx + uint64(i) + 1
						r.pendingConfChange = &cc
					}
				}

				m.Entries[i].Index = lastIdx + uint64(i) + 1
				m.Entries[i].Term = r.Term
			}
			idx := r.RaftLog.AppendEntries(m.Entries)
			r.Prs[r.id].Next = idx + 1
			r.Prs[r.id].Match = idx

			r.broadcastAppend()
			r.tryUpdateCommitIdx(idx) // This will be used for single node case
		}
		return nil
	}

	// Received a message from some older term node.
	// Ignore/reject.
	if m.Term < r.Term {
		switch m.MsgType {
		case pb.MessageType_MsgSnapshot:
			fallthrough
		case pb.MessageType_MsgAppend:
			r.send(pb.Message{
				MsgType: pb.MessageType_MsgAppendResponse,
				To:      m.From,
				From:    r.id,
				Term:    r.Term,
				Reject:  true,
			})
		case pb.MessageType_MsgRequestVote:
			r.send(pb.Message{
				MsgType: pb.MessageType_MsgRequestVoteResponse,
				To:      m.From,
				From:    r.id,
				Term:    r.Term,
				Reject:  true,
			})
		case pb.MessageType_MsgHeartbeat:
			r.send(pb.Message{
				MsgType: pb.MessageType_MsgHeartbeatResponse,
				To:      m.From,
				From:    r.id,
				Term:    r.Term,
				Reject:  true,
			})

		// Ignore these messages
		case pb.MessageType_MsgAppendResponse:
			fallthrough
		case pb.MessageType_MsgRequestVoteResponse:
			fallthrough
		case pb.MessageType_MsgHeartbeatResponse:
			fallthrough
		case pb.MessageType_MsgTransferLeader:
			fallthrough
		case pb.MessageType_MsgTimeoutNow:
			break
		default:
			log.Panicf("Not implemented %v", m)
		}

		return nil
	}

	// Receive a message from a newer term, step down immediately
	// and proceed to handle the message as a follower.
	if m.Term > r.Term {
		switch m.MsgType {
		case pb.MessageType_MsgSnapshot:
			fallthrough
		case pb.MessageType_MsgAppendResponse:
			fallthrough
		case pb.MessageType_MsgHeartbeat:
			fallthrough
		case pb.MessageType_MsgTimeoutNow:
			fallthrough
		case pb.MessageType_MsgAppend:
			fallthrough
		case pb.MessageType_MsgHeartbeatResponse:
			r.becomeFollower(m.Term, m.From)
		case pb.MessageType_MsgRequestVoteResponse:
			fallthrough
		case pb.MessageType_MsgRequestVote:
			fallthrough
		case pb.MessageType_MsgTransferLeader:
			r.becomeFollower(m.Term, None)
		default:
			log.Panicf("Not reached %v", m)
		}
	}

	// To simplify, all the handler logic assumes the message and the node
	// now has the same term.
	// assert m.Term == r.Term
	switch m.MsgType {
	case pb.MessageType_MsgTimeoutNow:
		r.handleTimeoutNow(m)
	case pb.MessageType_MsgTransferLeader:
		r.handleLeaderTransfer(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleRequestVoteResp(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
		r.handleHeartbeatResp(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendEntriesResp(m)
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	default:
		log.Panicf("Not implemented %v", m)
	}
	return nil
}
