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
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
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
	r := Raft {
		id: c.ID, 
		Term: 0,
		Vote: None,
		RaftLog: newLog(c.Storage),
		Prs: map[uint64]*Progress{},
		State: StateFollower, 
		votes: map[uint64]bool{},
		msgs: []pb.Message{},
		Lead: None,
		heartbeatTimeout: c.HeartbeatTick, 
		electionTimeout: c.ElectionTick,
		heartbeatElapsed: 0,
		electionElapsed: 0,
	}
	for _, peer_id := range c.peers {
		r.Prs[peer_id] = &Progress{0, 1} // initial match is 0, initial next is 1
	}
	r.becomeFollower(0, None)

	return &r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// get prev_log_index from progress
	prev_log_index := r.Prs[to].Next - 1
	// get prev_log_term 
	var prev_log_term uint64 = 0
	var err error = nil
	if (prev_log_index != 0) {
		prev_log_term, err = r.RaftLog.Term(prev_log_index)
	}
	if (err != nil) {
		log.Panicf("failed to find term for prev_log_index %d", prev_log_index)
	}
	log_entries, err := r.RaftLog.storage.Entries(prev_log_index + 1, r.RaftLog.stabled + 1)
	if (err != nil) {
		log.Panicf("failed to find log entries for prev_log_index %d", prev_log_index)
	}
	if (len(log_entries) == 0) {
		return false
	}
	log_entries = append(log_entries, r.RaftLog.unstableEntries()...)
	log_entries_to_send := make([]*pb.Entry, len(log_entries))
	for i, v := range log_entries {
		log_entries_to_send[i] = &v
	}

	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To: to,
		From: to,
		Term: r.Term,
		LogTerm: prev_log_term,
		Index: prev_log_index,
		Entries: log_entries_to_send,
		Commit: r.RaftLog.committed,
	})
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To: to, 
		From: r.id,
		Term: r.Term,
		Commit: r.RaftLog.committed,
	}) 
}

func (r *Raft) sendHeartbeatResponse(to uint64, reject bool) {
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To: to,
		From: r.id,
		Term: r.Term,
		Reject: reject,
	})
}

func (r *Raft) sendAppendEntriesResponse(to uint64, reject bool) {
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To: to,
		From: r.id,
		Term: r.Term,
		Reject: reject,
	})
}

func (r *Raft) sendRequestVote(to uint64, lastLogTerm uint64, lastLogIndex uint64) {
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		To: to,
		From: r.id,
		Term: r.Term,
		LogTerm: lastLogTerm,
		Index: lastLogIndex,
	})
}

func (r *Raft) sendRequestVoteResponse(to uint64, voteGranted bool) {
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To: to,
		From: r.id,
		Term: r.Term,
		Reject: !voteGranted,
	})
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	if (r.State == StateLeader) {
		r.heartbeatElapsed++
		if (r.heartbeatElapsed >= r.heartbeatTimeout) {
			for peerID := range r.Prs {
				if (peerID != r.id) {
					r.sendHeartbeat(peerID)
				}
			}
			r.heartbeatElapsed = 0
		}
	}
	r.electionElapsed++
	if (r.electionElapsed >= r.electionElapsed) {
		if (r.State == StateFollower || r.State == StateCandidate) {
			r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgHup})
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	r.Vote = None
	r.State = StateFollower
	r.Term = term
	r.Lead = lead
	r.electionElapsed = 0
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	r.State = StateCandidate
	r.Term += 1 // increments current term
	r.Lead = None
	r.Vote = r.id
	for k := range r.votes {
		r.votes[k] = false
	}
	r.electionElapsed = 0
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHup,
	})	
	// get last log term
	prevLogIndex := r.RaftLog.stabled
	var lastLogTerm uint64 = 0
	var err error = nil
	if (prevLogIndex != 0) {
		lastLogTerm, err = r.RaftLog.Term(prevLogIndex)
	}
	if (err != nil) {
		log.Panicf("becomeCandidate(): find term for index %d error %v", prevLogIndex, err)
	}

	for peerID := range r.Prs {
		r.sendRequestVote(peerID, lastLogTerm, prevLogIndex)
	}
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.Lead = r.id
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	switch r.State {
	case StateFollower:
		switch m.MsgType {
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
			break
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
			break
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
			break
		default:
		}
	case StateCandidate:
		switch m.MsgType {
		case pb.MessageType_MsgRequestVoteResponse:
			r.handleRequestVoteResponse(m)
			break
		case pb.MessageType_MsgAppend:
			if (m.Term > r.Term) { // if greater than current term, then we should become follower
				r.becomeFollower(m.Term, m.From)
				r.handleAppendEntries(m)
			}
			break
		case pb.MessageType_MsgHeartbeat:
			if (m.Term > r.Term) { // if greater than current term, then we should become follower
				r.becomeFollower(m.Term, m.From)
				r.handleHeartbeat(m)
			}
			break
		default:
		}
	case StateLeader:
		switch m.MsgType {
		case pb.MessageType_MsgPropose:
			r.handlePropose(m)
			break
		case pb.MessageType_MsgAppend:
			if (m.Term > r.Term) { // if greater than current term, then we should become follower
				r.becomeFollower(m.Term, m.From)
				r.handleAppendEntries(m)
			}
			break
		case pb.MessageType_MsgHeartbeat:
			if (m.Term > r.Term) { // if greater than current term, then we should become follower
				r.becomeFollower(m.Term, m.From)
				r.handleHeartbeat(m)
			}
			break
		case pb.MessageType_MsgAppendResponse:
			r.handleAppendEntriesResponse(m)
			break
		case pb.MessageType_MsgHeartbeatResponse:
			r.handleHeartbeatReponse(m)
			break
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
			break
		default:
		}
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
// NOTE! after leader's message being rejected by this raft, leader should dec corresponding progress state
func (r *Raft) handleAppendEntries(m pb.Message) {
	// reply false if term < currentTerm
	if (m.Term < r.Term) {
		r.sendAppendEntriesResponse(m.From, false)
		return
	} else if (m.Term > r.Term) {
		r.Term = m.Term
		r.Vote = None
	}
	// reply false if log doesn't contain an entry at prevLogIndex
	var term uint64 = 0
	var err error = nil
	if (m.Index != 0) {
		term, err = r.RaftLog.Term(m.Index)
	}
	if (err != nil || term != m.LogTerm) {
		r.sendAppendEntriesResponse(m.From, false)
		return
	}
	// if find a mismatched entry, delete all entries after mismatched entries
	for _, entry := range m.Entries {
		term, err = r.RaftLog.Term(entry.Index)
		// not found or find mistached, if all matched, then we can skip this entry
		if (err != nil || term != entry.Term) {
			r.RaftLog.RemoveAfter(entry.Index + 1)
			r.RaftLog.entries = append(r.RaftLog.entries, *entry)
			r.RaftLog.committed = min(m.Commit, entry.Index) // update committed
		}
	}
	r.sendAppendEntriesResponse(m.From, true)
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// reply false if term < currentTerm
	if (m.Term < r.Term) {
		r.sendHeartbeatResponse(m.From, false)
		return
	} else if (m.Term > r.Term) {
		r.Term = m.Term
		r.Vote = None
	}
	// reply false if log doesn't contain an entry at prevLogIndex
	var term uint64 = 0
	var err error = nil
	if (m.Index != 0) {
		term, err = r.RaftLog.Term(m.Index)
	}
	if (err != nil || term != m.LogTerm) {
		r.sendHeartbeatResponse(m.From, false)
		return
	}
	r.sendHeartbeatResponse(m.From, true)
	r.electionElapsed = 0
}

func (r *Raft) handleAppendEntriesResponse(m pb.Message) {
	if progress, ok := r.Prs[m.From]; ok {
		if (m.Reject) {
			progress.Next -= 1
			r.sendAppend(m.From)
		} else {
			progress.Next += 1
		}
	} else {
		log.Printf("cant find %d in progress map", m.From)
	}
}

func (r *Raft) handleHeartbeatReponse(m pb.Message) {}

func (r *Raft) handleRequestVote(m pb.Message) {
	if (m.Term < r.Term || r.Vote == None) {
		r.sendRequestVoteResponse(m.From, false)
	} else {
		lastLogIndex := r.RaftLog.stabled
		var lastLogTerm uint64 = 0
		var err error = nil
		if (lastLogIndex != 0) {
			lastLogTerm, err = r.RaftLog.Term(lastLogIndex)
		}
		if (err != nil) {
			log.Panicf("cannot find term for lastLogIndex %d", lastLogIndex)
		}
		// candidate is null and candidate'log is at least 
		// as up-to-date as receiver's log, grant vote
		if (m.LogTerm > lastLogTerm || (m.LogTerm == lastLogTerm && m.Index >= lastLogIndex)) {
			r.sendAppendEntriesResponse(m.From, true)
		} else {
			r.sendAppendEntriesResponse(m.From, false)
		}
	}
}

func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	if _, ok := r.Prs[m.From]; ok {
		if (!m.Reject) {
			r.votes[m.From] = true
		} else {
			r.votes[m.From] = false
		}
	} else {
		log.Printf("cannot find peer id %d", m.From)
	}
	if (len(r.votes) == len(r.Prs)) {
		var granted_cnt int = 0
		for _, v := range r.votes {
			if v {
				granted_cnt++
			}
		}
		if (granted_cnt * 2 > len(r.Prs)) { // wins election
			r.becomeLeader()
		}
	}
}

func (r *Raft) handlePropose(m pb.Message) {
	lastIndex := r.RaftLog.LastIndex()
	for _, e := range m.Entries {
		lastIndex++
		e.Index = lastIndex
		e.Term = r.Term
		r.RaftLog.entries = append(r.RaftLog.entries, *e)
	}
	for peerID := range r.Prs {
		if (peerID != r.id) {
			r.sendAppend(peerID)
		}
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
