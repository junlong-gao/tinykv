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
	peers []uint64

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
	PendingConfIndex uint64
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
	for _, id := range c.peers {
		if id == None {
			panic("No one should have id 0")
		}
		r.Prs[id] = &Progress{Match: 0, Next: 0}
	}

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
	if lastIdx == pr.Match {
		log.Debugf("Sending empty entries to %v", to)
	}
	if lastIdx < pr.Match {
		log.Panicf("%v < %v (%v)", lastIdx, pr.Match, to)
	}
	idx := pr.Next - 1
	term, err := r.RaftLog.Term(idx)
	if err != nil {
		log.Panic(err)
	}
	ents := r.RaftLog.Slice(idx+1, lastIdx-idx)
	r.send(pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		Commit:  r.RaftLog.committed,

		// Log matching, follower will check these first:
		Index:   idx,
		LogTerm: term, // this will always be available in log
		Entries: ents,
	})
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
	if !r.RaftLog.CheckMatch(m.Index, m.LogTerm) {
		r.send(pb.Message{
			To:      m.From,
			MsgType: pb.MessageType_MsgAppendResponse,
			Reject:  true,
		})
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
			ci := r.RaftLog.Compare(m.Entries)
			idx := m.Entries[len(m.Entries)-1].Index
			if ci != 0 {
				idx = r.RaftLog.AppendEntries(m.Entries[ci-m.Entries[0].Index:])
			}

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

func (r *Raft) tryUpdateCommitIdx(newCommitIdx uint64) {
	if newCommitIdx <= r.RaftLog.committed {
		return
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
		return
	}

	// We have a chance to update commit index
	count := 0
	for _, pr := range r.Prs {
		if pr.Match >= newCommitIdx {
			count++
		}
	}
	if 2*count > len(r.Prs) {
		r.RaftLog.commitTo(newCommitIdx)
		r.broadcastAppend() // quickly update followers about this new commit index
	}
}

func (r *Raft) handleAppendEntriesResp(m pb.Message) {
	// Your Code Here (2A).

	if r.State != StateLeader {
		return
	}

	if m.Reject {
		r.Prs[m.From].Next--
		r.sendAppend(m.From)
	} else {
		r.Prs[m.From].Match = max(r.Prs[m.From].Match, m.Index)
		r.Prs[m.From].Next = max(r.Prs[m.From].Next, r.Prs[m.From].Match+1)

		r.tryUpdateCommitIdx(r.Prs[m.From].Match)
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
// leadership and piggy-backs the commit index 2) allow leader to know which followers are alive
// and replicate data
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	r.send(pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		Commit:  r.RaftLog.committed,
	})
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	if r.State != StateFollower {
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

	r.votes = make(map[uint64]bool)

	r.electionElapsed = 0
	r.electionTimeout = r.electionTimeoutConfig + rand.Int()%(r.electionTimeoutConfig+1)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).

	if r.State == StateLeader {
		panic("Leader cannot become candidate!")
	}

	r.Term++
	r.State = StateCandidate
	r.electionTimeout = r.electionTimeoutConfig + rand.Int()%(r.electionTimeoutConfig+1)
	r.Lead = None

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

			r.send(pb.Message{
				MsgType: pb.MessageType_MsgRequestVote,
				To:      p,

				LogTerm: lastTerm,
				Index:   lastIndex,
			})
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

func (r *Raft) countVotes(ballet map[uint64]bool) bool {
	// Got majority, count vote
	voteCount := 0
	for _, v := range ballet {
		if v {
			voteCount++
		}
	}
	return voteCount*2 > len(r.Prs)
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term

	if r.State != StateCandidate {
		panic("Only candidates can become a leader!")
	}

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

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).

	log.Panicf("Not implemented %v", m.MsgType)
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
	log.Panicf("Not implemented")
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
	log.Panicf("Not implemented")
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	if r.State == StateLeader {
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.broadcastHeartBeat()
		}
	} else {
		r.electionElapsed++
		if r.electionElapsed >= r.electionTimeout {
			r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
		}
	}
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
			log.Debug("No longer leader, cannot propose")
		} else {
			lastIdx := r.RaftLog.LastIndex()
			lastTerm, err := r.RaftLog.Term(lastIdx)
			if err != nil {
				log.Panic(err)
			}
			m.Index = lastIdx
			m.LogTerm = lastTerm
			for i, _ := range m.Entries {
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
				MsgType: pb.MessageType_MsgHeartbeat,
				To:      m.From,
				From:    r.id,
				Term:    r.Term,
				Reject:  true,
			})

		case pb.MessageType_MsgAppendResponse:
			fallthrough
		case pb.MessageType_MsgRequestVoteResponse:
			fallthrough
		case pb.MessageType_MsgHeartbeatResponse:
			// ignore
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
		case pb.MessageType_MsgAppend:
			r.becomeFollower(m.Term, m.From)
		case pb.MessageType_MsgRequestVote:
			r.becomeFollower(m.Term, None)
		case pb.MessageType_MsgHeartbeat:
			r.becomeFollower(m.Term, m.From)
		default:
			log.Panicf("Not reached %v", m)
		}
	}

	// To simplify, all the handler logic assumes the message and the node
	// now has the same term.
	// assert m.Term == r.Term
	switch m.MsgType {
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
	default:
		log.Panicf("Not implemented %v", m)
	}
	return nil
}
