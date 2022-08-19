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
	"log"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// return if cannot find index in entries
var ErrCannotFindIndex = errors.New("cannot find index")

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	var raftLog *RaftLog
	firstIndex, err := storage.FirstIndex()
	if (err != nil) {
		log.Panicf("%v", err)
	}
	lastIndex, err := storage.LastIndex()
	if (err != nil) {
		log.Panicf("%v", err)
	}
	entries, err := storage.Entries(firstIndex, lastIndex + 1)
	if (err != nil) {
		log.Panicf("%v", err)
	}
	raftLog = &RaftLog{
		storage: storage,
		entries: entries,
		stabled: lastIndex,
	}
	return raftLog
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// get the position of smallest entry which satisfies entry.index > index
func (l *RaftLog) idxAfter(index uint64) int {
	var left int = 0
	var right int = len(l.entries)
	for left < right {
		var mid int = (left + right) / 2
		if (l.entries[mid].Index <= index) {
			left = mid + 1
		} else {
			right = mid
		}
	}
	return left
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	return l.entries[l.idxAfter(l.stabled):]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	firstIndex, err := l.storage.FirstIndex()
	if (err != nil) {
		log.Printf("%v\n", err)
		firstIndex = 0
	}
	lastIndex, err := l.storage.LastIndex()
	if (err != nil) {
		log.Printf("%v\n", err)
		lastIndex = firstIndex
	}
	var entries []pb.Entry = make([]pb.Entry, 0)
	if (firstIndex >= 1 && lastIndex >= firstIndex) {
		entries, err = l.storage.Entries(max(l.applied + 1, firstIndex) , min(lastIndex, l.committed) + 1)
		if (err != nil) {
			log.Panicf("%v l.applied:%d l.committed:%d\n", err, l.applied, l.committed)
		}
		entries = append(entries, l.entries[l.idxAfter(min(lastIndex, l.committed)):l.idxAfter(l.committed)]...)
	} else {
		entries = append(entries, l.entries[l.idxAfter(l.applied):l.idxAfter(l.committed)]...)
	}
	return entries
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	if (len(l.entries) == 0) {
		lastIndex, err := l.storage.LastIndex()
		if (err != nil) {
			log.Panicf("RaftLog.LastIndex() error\n")
		}
		return lastIndex
	} else {
		return l.entries[len(l.entries) - 1].Index
	}
}

// Term return the term of the entry in the given index
// return err if not found
func (l *RaftLog) Term(i uint64) (uint64, error) {
	if (i <= l.stabled) {
		return l.storage.Term(i)
	} else {
		var position int = l.idxAfter(i) - 1
		if (position >= 0 && l.entries[position].Index == i) {
			return l.entries[position].Term, nil
		}
		return 0, ErrCannotFindIndex
	}
}

// remove all entries after i and i iteself
func (l *RaftLog) RemoveAfter(i uint64) {
	var position = l.idxAfter(i) - 1
	if (position != -1) {
		l.entries = l.entries[:position]
	}
	if (l.stabled >= i) {
		l.stabled = i - 1
	}
	return
}
