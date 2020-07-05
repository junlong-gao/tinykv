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
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//                            applied           committed
//  snapshot/first.....                      stabled  ....last
//  --------|------------------------------------------------|
//                            log entries
//          |<----          storage          |---->|
//                                           XXXXXXX
//	        storage.FirsIndex()	 storage.LastIndex()
//  The part XXX is to be discarded if the log mismatches, or
//  used to update field "stabled" if log matches.
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries on this node since the last snapshot.
	storage Storage

	// committed is the highest log index that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log index that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Every time handling `Ready`, the unstabled logs will be included.
	// This value can be less than storage.LastIndex()
	// Invariant: l.stabled <= storageLastIdx
	stabled uint64

	// all entries that have not yet stable.
	// Index range [stabled + 1...last]
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
}

func checkInv(l *RaftLog) {
	firstIdx, err := l.storage.FirstIndex()
	if err != nil {
		log.Panic(err)
	}
	if firstIdx > l.applied+1 {
		log.Panicf("%v > %v (data loss?)", firstIdx, l.applied+1)
	}
	if l.applied > l.committed {
		log.Panicf("%v > %v", l.applied, l.committed)
	}

	if len(l.entries) == 0 {
		return
	}

	if l.entries[0].Index != l.stabled+1 {
		log.Panicf("%v != %v", l.entries[0].Index, l.stabled+1)
	}
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	lastIdx, err := storage.LastIndex()
	if err != nil {
		log.Fatal(err)
	}
	return &RaftLog{
		storage: storage,
		stabled: lastIdx,
		entries: []pb.Entry{},
	}
}

func newLogWithAppliedIndex(storage Storage, appliedIndex uint64) *RaftLog {
	// Your Code Here (2A).

	lastIdx, err := storage.LastIndex()
	if err != nil {
		log.Fatal(err)
	}

	s, _, err := storage.InitialState()
	if err != nil {
		log.Fatal(err)
	}

	ret := RaftLog{
		storage:   storage,
		committed: s.GetCommit(), // lower bound, real commit index will be updated by the node
		applied:   appliedIndex,  // applied <= committed
		stabled:   lastIdx,
		entries:   []pb.Entry{},
	}

	checkInv(&ret)
	return &ret
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

func (l *RaftLog) hasUnstableEnts() bool {
	checkInv(l)
	return len(l.entries) > 0
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).

	checkInv(l)
	return l.entries
}

// Advance the stable field on WAL flush. This updates the l.stable
// and truncate the prefix of l.entries accordingly because application
// has flushed WAL and storage last index has advanced as a result.
func (l *RaftLog) tryStableTo(lastTerm, lastIndex uint64) {
	checkInv(l)
	if len(l.entries) == 0 {
		return
	}

	{ // check to stable index is within current unstable set, otherwise ignore
		off := lastIndex - (l.stabled + 1)
		if off < 0 || off > uint64(len(l.entries)) {
			// Completely mismatch with unstable entries
			return
		}

		if l.entries[off].Term != lastTerm {
			// Term mismatch, probably some append entry RPC overwritten
			// in-memory unstable entries
			return
		}
	}

	{ // Make sure storage last index has advanced to at least lastIndex
		storageLastIdx, err := l.storage.LastIndex()
		if err != nil {
			log.Panic(err)
		}

		// Invariant
		if storageLastIdx < l.stabled {
			log.Panicf("%v >= %v", storageLastIdx, l.stabled)
		}
		// Completely mismatch with stable entries: probably some append
		// entry RPC overwritten stable logs
		if lastIndex > storageLastIdx {
			return
		}

		storageTerm, err := l.storage.Term(lastIndex)
		if err != nil {
			log.Panic(err)
		}
		if storageTerm != lastTerm {
			// Term mismatch, probably some append entry RPC overwritten
			// the stable logs
			return
		}
	}

	// assert l.stabled < lastIndex <= storageLastIdx
	// assert log match upto lastIndex between l.entries and storage.
	//
	// Truncate [oldStableIdx + 1, l.stabled] in l.entries
	oldStableIdx := l.stabled
	l.stabled = lastIndex
	l.entries = l.entries[l.stabled-oldStableIdx:]

	checkInv(l)
	return
}

func (l *RaftLog) hasNextEnts() bool {
	return l.applied+1 < l.committed
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).

	checkInv(l)
	firstIdx, err := l.storage.FirstIndex()
	if err != nil {
		log.Panic(err)
	}
	if l.applied+1 < firstIdx {
		log.Panicf("Index %v < %v", l.applied+1, firstIdx)
	}

	if l.applied+1 >= l.committed+1 {
		return make([]pb.Entry, 0)
	}

	// We need to slice [l.applied + 1, l.committed]
	tmp := l.Slice(l.applied+1, l.committed-l.applied)
	ret := make([]pb.Entry, 0)
	for _, e := range tmp {
		ret = append(ret, *e)
	}

	return ret
}

// Update applied index, this has fewer cases then tryStableTo()
func (l *RaftLog) appliedTo(idx uint64) {
	checkInv(l)
	// XXX Not sure why etcd does https://github.com/etcd-io/etcd/blob/15884e90854981494e6889bdb663439612b547a7/raft/log.go#L214
	if idx <= l.applied || idx > l.committed {
		log.Panicf("Applied %v out of range: [%v, %v]", idx, l.applied, l.committed)
	}

	l.applied = idx
	checkInv(l)
}

// On append entry / heartbeat, leader will piggy-back commit index
// to this follower.
func (l *RaftLog) commitTo(idx uint64) {
	checkInv(l)
	if idx < l.committed {
		log.Panicf("Commit index cannot go backwards: %v -> %v", l.committed, idx)
	}

	if idx > l.LastIndex() {
		idx = l.LastIndex()
	}
	log.Debugf("Update commit index %v -> %v", l.committed, idx)
	l.committed = idx
	checkInv(l)
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).

	return l.stabled + uint64(len(l.entries))
}

func (l *RaftLog) LastTerm() uint64 {
	// Your Code Here (2A).
	term, err := l.Term(l.LastIndex())
	if err != nil {
		log.Panicf("%v", err)
	}

	return term
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	checkInv(l)
	if i >= l.stabled+1 {
		return l.entries[i-l.stabled-1].Term, nil
	}

	return l.storage.Term(i)
}

func (l *RaftLog) CheckMatch(idx, term uint64) bool {
	firstIdx, err := l.storage.FirstIndex()
	if err != nil {
		log.Panicf("%v", err)
	}
	if idx < firstIdx {
		return true
	}
	if idx > l.LastIndex() {
		return false
	}
	return l.Slice(idx, 1)[0].Term == term
}

// Find the smallest index such that its term does not agree with the log.
// If no such index exist, 0 will be returned.
func (l *RaftLog) Compare(ents []*pb.Entry) uint64 {
	for _, e := range ents {
		if !l.CheckMatch(e.Index, e.Term) {
			if e.Index <= l.LastIndex() {
				log.Debugf("Conflicting index %v, current [%v, %v]", e.Index,
					l.Slice(e.Index, 1)[0].Term, l.Slice(e.Index, 1)[0].Index)
			}
			return e.Index
		}
	}

	return 0
}

// AppendEntries to the log, it will erase all entries in the existing log with index > entsPtrs.last.Index.
// ents must be contiguous and start no further than LastIndex() + 1,
// Must succeed, and return the new last index as a result of the append.
func (l *RaftLog) AppendEntries(entsPtrs []*pb.Entry) uint64 {
	checkInv(l)
	if len(entsPtrs) == 0 {
		return l.LastIndex()
	}

	// Just to make a contiguous copy of all the entries, XXX we'll fix
	// the impedance mismatch later between this interface and pb.Message
	ents := make([]pb.Entry, len(entsPtrs))
	for i, e := range entsPtrs {
		ents[i] = *e
	}

	entsStartIdx := ents[0].Index
	lastIdx := l.LastIndex()

	switch {
	case entsStartIdx > lastIdx+1:
		log.Panicf("Hole to append, logic error")
	case entsStartIdx == lastIdx+1:
		// Direct append
		l.entries = append(l.entries, ents...)
	case entsStartIdx <= l.stabled+1:
		// Replace and page in
		l.entries = ents
		l.stabled = entsStartIdx - 1
	default:
		// assert entsStartIdx > l.stabled + 1
		// [entsStartIdx ... ] in ents has to be unstabled into l.entries.
		// Truncate the prefix [l.stabled + 1, entStartIdx) in l.entries
		// and append
		l.entries = l.entries[:entsStartIdx-l.stabled-1]
		l.entries = append(l.entries, ents...)
	}

	checkInv(l)
	return l.LastIndex()
}

// Construct a slice of entries in [startIdx, startIdx + length)
func (l *RaftLog) Slice(startIdx, length uint64) []*pb.Entry {
	checkInv(l)

	var ret []*pb.Entry
	if startIdx > l.LastIndex() {
		return ret
	}
	if startIdx <= l.stabled {
		ents, err := l.storage.Entries(startIdx, l.stabled+1)
		if err != nil {
			log.Panic(err)
		}

		for i := startIdx; i < l.stabled+1 && uint64(len(ret)) < length; i++ {
			if ents[i-startIdx].Index != i {
				log.Panic(ents[i-startIdx])
			}
			ret = append(ret, &ents[i-startIdx])
		}
	}
	for i := max(l.stabled+1, startIdx); i <= l.LastIndex() && uint64(len(ret)) < length; i++ {
		ret = append(ret, &l.entries[i-(l.stabled+1)])
	}
	return ret
}

func (l *RaftLog) allEntries() []pb.Entry {
	lastIdx := l.LastIndex()
	firstIdx, err := l.storage.FirstIndex()
	if err != nil {
		log.Panicf("%v", err)
	}
	tmp := l.Slice(firstIdx, lastIdx-firstIdx+1)
	ret := make([]pb.Entry, 0)
	for _, e := range tmp {
		ret = append(ret, *e)
	}

	return ret
}
