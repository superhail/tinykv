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
	"math/rand"

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
	baseLineElectionTimeout int
	// election interval
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
	hardState, _, err := c.Storage.InitialState()
	if (err != nil) {
		log.Panicf("%v", err)
	}
	r := Raft {
		id: c.ID, 
		Term: hardState.Term,
		Vote: hardState.Vote,
		RaftLog: newLog(c.Storage),
		Prs: map[uint64]*Progress{},
		State: StateFollower, 
		votes: map[uint64]bool{},
		msgs: []pb.Message{},
		Lead: None,
		heartbeatTimeout: c.HeartbeatTick, 
		baseLineElectionTimeout: c.ElectionTick,
		heartbeatElapsed: 0,
		electionElapsed: c.ElectionTick + rand.Intn(c.ElectionTick),
	}
	r.RaftLog.applied = c.Applied
	r.RaftLog.committed = hardState.Commit
	for _, peer_id := range c.peers {
		r.Prs[peer_id] = &Progress{0, 1} // initial match is 0, initial next is 1
	}

	return &r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// get prevLogIndex from progress
	prevLogIndex := r.Prs[to].Next - 1
	// get prevLogTerm 
	var prevLogTerm uint64 = 0
	var err error = nil
	if (prevLogIndex != 0) {
		prevLogTerm, err = r.RaftLog.Term(prevLogIndex)
	}
	if (err != nil) {
		log.Panicf("failed to find term for prevLogIndex %d", prevLogIndex)
	}
	var logEntries []pb.Entry = make([]pb.Entry, 0)
	if (prevLogIndex < r.RaftLog.stabled) {
		logEntries, err = r.RaftLog.storage.Entries(prevLogIndex + 1, r.RaftLog.stabled + 1)
		if (err != nil) {
			log.Panicf("failed to find log entries for prevLogIndex %d", prevLogIndex)
		}
	}
	if (prevLogIndex < r.RaftLog.LastIndex()) {
		logEntries = append(logEntries, r.RaftLog.entries[r.RaftLog.idxAfter(prevLogIndex):]...)
	}
	logEntriesToSend := make([]*pb.Entry, len(logEntries))
	for i := range logEntries {
		logEntriesToSend[i] = &logEntries[i]
	}
	if (len(logEntriesToSend) != 0) {
		r.Prs[to].Next = logEntriesToSend[len(logEntriesToSend) - 1].Index + 1
	}
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To: to,
		From: r.id,
		Term: r.Term,
		LogTerm: prevLogTerm,
		Index: prevLogIndex,
		Entries: logEntriesToSend,
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
		Index: r.RaftLog.LastIndex(),
	})
}

func (r *Raft) sendAppendEntriesResponse(to uint64, lastIndex uint64, reject bool) {
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To: to,
		From: r.id,
		Term: r.Term,
		Index: lastIndex,
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
			r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat, From: r.id, To: r.id})
			r.heartbeatElapsed = 0
		}
	}
	r.electionElapsed++
	if (r.electionElapsed >= r.electionTimeout) {
		r.Step(pb.Message{MsgType: pb.MessageType_MsgHup, From: r.id, To: r.id})		
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	r.Vote = None
	r.State = StateFollower
	r.Term = term
	r.Lead = lead
	r.electionElapsed = 0
	r.electionTimeout = r.baseLineElectionTimeout + rand.Intn(r.baseLineElectionTimeout)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	r.State = StateCandidate
	r.Term += 1 // increments current term
	r.Lead = None
	r.Vote = r.id
	r.votes = make(map[uint64]bool)
	// r.handleRequestVoteResponse(pb.Message{From: r.id, To: r.id, Term: r.Term, Reject: false})
	r.electionElapsed = 0
	r.electionTimeout = r.baseLineElectionTimeout + rand.Intn(r.baseLineElectionTimeout)
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.Lead = r.id
	for _, progress := range r.Prs {
		progress.Match = 0						// matchIndex is initialised to 0
		progress.Next = r.RaftLog.LastIndex() + 1	// next is initialised to the index just after the last one in its log
	}
	r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{
			EntryType: pb.EntryType_EntryNormal, 
			Term: r.Term, 
			Index: r.Prs[r.id].Next, 
			Data: nil})
	r.handleAppendEntriesResponse(pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse, 
		To: r.id, 
		From: r.id, 
		Index: r.Prs[r.id].Next,
		Entries: []*pb.Entry{{
				EntryType: pb.EntryType_EntryNormal, 
				Term: r.Term, 
				Index: r.Prs[r.id].Next,
				Data: nil,
			}}})
	r.Prs[r.id].Match = r.Prs[r.id].Next
	r.Prs[r.id].Next++
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	if ((m.Term > r.Term || r.Lead == None)) {
		if (m.MsgType == pb.MessageType_MsgAppend || m.MsgType == pb.MessageType_MsgHeartbeat) {
			r.becomeFollower(m.Term, m.From)
		}
		if (m.Term > r.Term && m.MsgType == pb.MessageType_MsgRequestVote) {
			r.becomeFollower(m.Term, None)
		}
	}
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
		case pb.MessageType_MsgHup:
			r.handleMsgHup(m)
			break
		case pb.MessageType_MsgAppendResponse:
		case pb.MessageType_MsgRequestVoteResponse:
		case pb.MessageType_MsgHeartbeatResponse:
		case pb.MessageType_MsgBeat:
			break
		default:
			log.Panicf("unrecognized message state:%v msg:%v \n", r.State.String(), m.String())
		}
	case StateCandidate:
		switch m.MsgType {
		case pb.MessageType_MsgAppend:
			if (m.Term >= r.Term) { // if >= current term, then we should become follower
				r.becomeFollower(m.Term, m.From)
				r.handleAppendEntries(m)
			}
			break
		case pb.MessageType_MsgHeartbeat:
			if (m.Term >= r.Term) { // if >= current term, then we should become follower
				r.becomeFollower(m.Term, m.From)
				r.handleHeartbeat(m)
			}
			break
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
			break
		case pb.MessageType_MsgHup:
			r.handleMsgHup(m)
			break
		case pb.MessageType_MsgRequestVoteResponse:
			r.handleRequestVoteResponse(m)
			break
		case pb.MessageType_MsgAppendResponse:
		case pb.MessageType_MsgHeartbeatResponse:
		case pb.MessageType_MsgBeat:
		case pb.MessageType_MsgPropose:
			break
		default:
			log.Panicf("unrecognized message state:%v msg:%v \n", r.State.String(), m.String())
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
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
			break
		case pb.MessageType_MsgAppendResponse:
			r.handleAppendEntriesResponse(m)
			break
		case pb.MessageType_MsgHeartbeatResponse:
			r.handleHeartbeatResponse(m)
			break
		case pb.MessageType_MsgBeat:
			r.handleMsgBeat(m)
		case pb.MessageType_MsgHup:
		case pb.MessageType_MsgRequestVoteResponse:
			break
		default:
			log.Panicf("unrecognized message state:%v msg:%v \n", r.State.String(), m.String())
		}
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
// NOTE! after leader's message being rejected by this raft, leader should dec corresponding progress state
func (r *Raft) handleAppendEntries(m pb.Message) {
	// reply false if term < currentTerm
	if (m.Term < r.Term) {
		r.sendAppendEntriesResponse(m.From, m.Index, true)
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
		r.sendAppendEntriesResponse(m.From, m.Index, true)
		return
	}
	// if found a mismatched entry, delete all entries after mismatched entries
	for _, entry := range m.Entries {
		term, err = r.RaftLog.Term(entry.Index)
		// not found or find mistached, if all matched, then we can skip this entry
		if (err != nil || term != entry.Term) {
			if (term != 0) { // mistached
				r.RaftLog.RemoveAfter(entry.Index)
			}
			r.RaftLog.entries = append(r.RaftLog.entries, *entry)
		}
		if (m.Commit > r.RaftLog.committed) {
			r.RaftLog.committed = min(m.Commit, entry.Index)
		}
	}
	// update committed
	if (m.Commit > r.RaftLog.committed) {
		if (len(m.Entries) == 0) { // broadcase message
			r.RaftLog.committed = min(m.Commit, m.Index)
		}
	}
	r.sendAppendEntriesResponse(m.From, r.RaftLog.LastIndex(), false)
	r.electionElapsed = 0
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// reply false if term < currentTerm
	if (m.Term < r.Term) {
		r.sendHeartbeatResponse(m.From, true)
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
		r.sendHeartbeatResponse(m.From, true)
		return
	}
	// update committed
	if (m.Commit > r.RaftLog.committed) {
		r.RaftLog.committed = min(m.Commit, m.Index)
	}	
	r.sendHeartbeatResponse(m.From, false)
	r.electionElapsed = 0
}

// if message is rejected, then we will send Append agian with lower Index.
// if message is not rejected, then we will update committed if necessary(if more than half agreed upon this message).
func (r *Raft) handleAppendEntriesResponse(m pb.Message) {
	if progress, ok := r.Prs[m.From]; ok {
		if (m.Reject) {
			progress.Next = m.Index
			r.sendAppend(m.From)
		} else {
			if (progress.Match < m.Index) {
				progress.Match = m.Index
				term, _ := r.RaftLog.Term(m.Index)
				// test if can be new committed, leader will only commit entries of current term
				if (term == r.Term) {
					var cnt int = 0
					for _, p := range r.Prs {
						if (p.Match >= progress.Match) {
							cnt++
						}
					}
					// if majority of servers agree, and the index of new committed == r.Term
					if (cnt * 2 >  len(r.Prs)) {
						if (r.RaftLog.committed < m.Index) {
							r.RaftLog.committed = m.Index
							// broadcast commitIndex to all peers
							for peerID := range r.Prs {
								if (peerID != r.id) {
									r.sendAppend(peerID)
								}
							}
						}
					}
				}
			}
		}
	} else {
		log.Printf("cant find %d in progress map", m.From)
	}
}

func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	// if found logs missing or mismatched, send back
	if (m.Reject || m.Index < r.RaftLog.committed) {
		r.sendAppend(m.From)
	}
}

func (r *Raft) handleRequestVote(m pb.Message) {
	if (m.Term < r.Term) {
		r.sendRequestVoteResponse(m.From, false)
	} else {
		lastLogIndex := r.RaftLog.LastIndex()
		var lastLogTerm uint64 = 0
		var err error = nil
		if (lastLogIndex != 0) {
			lastLogTerm, err = r.RaftLog.Term(lastLogIndex)
		}
		if (err != nil) {
			log.Panicf("cannot find term for lastLogIndex %d", lastLogIndex)
		}
		// candidate is null or candidateId 
		// and candidate'log is at least as up-to-date as receiver's log, 
		// grant vote
		if ((r.Vote == None || r.Vote == m.From) && (m.LogTerm > lastLogTerm || (m.LogTerm == lastLogTerm && m.Index >= lastLogIndex))) {
			r.Vote = m.From
			r.Term = m.Term
			r.sendRequestVoteResponse(m.From, true)
		} else {
			r.sendRequestVoteResponse(m.From, false)
		}
	}
}

func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	if (m.Term != r.Term) { // neglect outdated responses
		return
	}
	if _, ok := r.Prs[m.From]; ok {
		r.votes[m.From] = !m.Reject
	} else {
		log.Printf("cannot find peer id %d", m.From)
	}
	if (len(r.votes) * 2 > len(r.Prs)) {
		var grantedCnt int = 0
		var failedCnt int = 0
		for _, v := range r.votes {
			if v {
				grantedCnt++
			} else {
				failedCnt++
			}
		}
		if (grantedCnt * 2 > len(r.Prs)) { // won election
			r.becomeLeader()
			for peerID := range r.Prs {
				if (peerID != r.id) {
					r.sendAppend(peerID)
				}
			}
		} else if (failedCnt * 2 > len(r.Prs)) { // failed election
			r.becomeFollower(r.Term, None)
		}
	}
}

func (r *Raft) handlePropose(m pb.Message) {
	if (len(m.Entries) == 0) {
		return
	}
	lastIndex := r.RaftLog.LastIndex()
	for _, e := range m.Entries {
		lastIndex++
		e.Index = lastIndex
		e.Term = r.Term
		r.RaftLog.entries = append(r.RaftLog.entries, *e)
	}
	r.Step(pb.Message{MsgType: pb.MessageType_MsgAppendResponse, From: r.id, To: r.id, Index: r.Prs[r.id].Next, Reject: false})
	r.Prs[r.id].Next++
	for peerID := range r.Prs {
		if (peerID != r.id) {
			r.sendAppend(peerID)
		}
	}
}

func (r *Raft) handleMsgHup(m pb.Message) {
	r.becomeCandidate()
	r.requestVotes()
}

func (r *Raft) handleMsgBeat(m pb.Message) {
	for peerID := range r.Prs {
		if (peerID != r.id) {
			r.sendHeartbeat(peerID)
		}
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// send request vote requests to all peers other than self
func (r *Raft) requestVotes() {
	// get last log term
	prevLogIndex := r.RaftLog.LastIndex()
	var lastLogTerm uint64 = 0
	var err error = nil
	if (prevLogIndex != 0) {
		lastLogTerm, err = r.RaftLog.Term(prevLogIndex)
	}
	if (err != nil) {
		log.Panicf("becomeCandidate(): find term for index %d error %v", prevLogIndex, err)
	}

	for peerID := range r.Prs {
		if (peerID != r.id) {
			r.sendRequestVote(peerID, lastLogTerm, prevLogIndex)
		} else {
			r.handleRequestVoteResponse(pb.Message{From: r.id, To: r.id, Term: r.Term, Reject: false})
		}
	}	
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
