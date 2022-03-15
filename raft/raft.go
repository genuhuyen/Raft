package raft

// Raft implementation.

// This file is divided into the following sections, each containing functions
// that implement the roles described in the Raft paper.

// Background Election Thread -------------------------------------------------
// Perioidic thread that checks whether this server should start an election.
// Also attempts to commit any outstanding entries, if necessary (this could be
// a separate periodic thread but is included here).

// Background Leader Thread ---------------------------------------------------
// Periodic thread that performs leadership duties if leader. The leader must
// 1) call the AppendEntries RPC of its followers (either with empty messages or
// with entries to append) and 2) check whether the commit index has advanced.

// Start an Election ----------------------------------------------------------
// Functions to start an election by sending RequestVote RPCs to followers.

// Send AppendEntries RPCs to Followers ---------------------------------------
// Send followers AppendEntries, either as heartbeat or with state machine
// changes.

// Incoming RPC Handlers ------------------------------------------------------
// Handlers for incoming AppendEntries and RequestVote requests. Should populate
// reply based on the current state.

// Commit-Related Functions ---------------------------------------------------
// Functions to update the leader commit index based on the match indices of all
// followers, as well as to apply all outstanding commits.

// Persistence Functions ------------------------------------------------------
// Functions to persist and to read from persistent state. recoverState should
// only be called upon recovery (in CreateRaft). persist should be called
// whenever the durable state changes.

// Outgoing RPC Handlers ------------------------------------------------------
// Call these to invoke a given RPC on the given client.

import (
	// "raft/common" If want to use DieIfError.

	"bytes"
	"encoding/gob"
	"math"
	"raft/common"

	// "bytes" Part 4
	// "encoding/gob" Part 4
	"math/rand"
	"time"
	"github.com/golang/glog"
)

// Resets the election timer. Randomizes timeout to between 0.5 * election
// timeout and 1.5 * election timeout.
// REQUIRES r.mu
func (r *Raft) resetTimer() {
	to := int64(RaftElectionTimeout) / 2
	d := time.Duration(rand.Int63()%(2*to) + to)
	r.deadline = time.Now().Add(d)
}

// getStateLocked returns the current state while locked.
// REQUIRES r.mu
func (r *Raft) getStateLocked() (int, bool) {
	return r.currentTerm, r.state == leader
}

// Background Election Thread -------------------------------------------------
// Perioidic thread that checks whether this server should start an election.
// Also attempts to commit any outstanding entries, if necessary (this could be
// a separate periodic thread but is included here).

// Called once every n ms, where n is < election timeout. Checks whether the
// election has timed out. If so, starts an election. If not, returns
// immediately.
// EXCLUDES r.mu
func (r *Raft) electOnce() bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.stopping {
		// Stopping, quit.
		return false
	}

	if time.Now().Before(r.deadline) {
		// Election deadline hasn't passed. Try later.
		// See if we can commit.
		go r.commit()
		return true
	}

	// Deadline has passed. Convert to candidate and start an election.
	glog.V(1).Infof("%d starting election for term %d", r.me, r.currentTerm)

	//conversion to candidate to do
	r.state = candidate
	r.votes = 0
	r.votedFor = -1
	r.currentTerm++
	r.votedFor = r.me
	r.votes++
	r.resetTimer()

	// Send RequestVote RPCs to followers.
	r.sendBallots()

	return true
}

// Background Leader Thread ---------------------------------------------------
// Periodic thread that performs leadership duties if leader. The leader must:
// call AppendEntries on its followers (either with empty messages or with
// entries to append) and check whether the commit index has advanced.

// Called once every n ms, where n is much less than election timeout. If the
// process is a leader, performs leadership duties.
// EXCLUDES r.mu
func (r *Raft) leadOnce() bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.stopping {
		// Stopping, quit.
		return false
	}

	if r.state != leader {
		// Not leader, try later.
		return true
	}

	// Reset deadline timer.
	r.resetTimer()

	// Send heartbeat messages.
	r.sendAppendEntries()
	// Update commitIndex, if possible.
	r.updateCommitIndex()

	return true
}

// Start an Election ----------------------------------------------------------
// Functions to start an election by sending RequestVote RPCs to followers.

// Send RequestVote RPCs to all peers.
// REQUIRES r.mu
func (r *Raft) sendBallots() {
	for p := range r.peers {
		if p == r.me {
			continue
		}

		//a follower increments its current term
		//change state from to candidate
		args := requestVoteArgs{
			Term:         r.currentTerm,
			CandidateID:  r.me,
			LastLogIndex: r.lastApplied,
			LastLogTerm:  r.currentTerm,
		}
		go r.sendBallot(p, args)
	}
}

// Send an individual ballot to a peer; upon completion, check if we are now the
// leader. If so, become leader.
// EXCLUDES r.mu
func (r *Raft) sendBallot(peer int, args requestVoteArgs) {
	// Call RequestVote on peer.
	var reply requestVoteReply
	var ok bool
	var rreply = 0
	if ok = r.callRequestVote(peer, args, &reply); !ok {
		glog.V(6).Infof("%d sendballot RPC to %d failed.",r.me, peer)
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	//candidate term is old, need to learn the new time
	if reply.Term > r.currentTerm{
		r.currentTerm = reply.Term
		r.votedFor = -1
		r.state = follower
		r.persist()
		return
	}

	//don't count vote, this is not our term
	if reply.Term < r.currentTerm{
		return
	}

	//count how many replies we got
	if reply.VoteGranted == false || reply.VoteGranted == true{
		rreply++
	}
	//count how many votes we got
	if reply.VoteGranted{
		r.votes++
	}

	//if gets majority vote, becomes leader
	majority := len(r.peers)/2
	if r.votes > majority{
		r.state = leader
		r.currentTerm = reply.Term
		glog.V(6).Infof("%v is new leader!", r.me)

		//create spots for all peers, initialize nextindex, matchindex
		if len(r.nextIndex) != len(r.peers){
			for i := 0; i < len(r.peers); i++ {
				r.nextIndex = append(r.nextIndex, 0)
				r.matchIndex = append(r.matchIndex, 0)
			}
		}
		r.resetTimer()
	}
	//election timeout occurs, start a new election
	return
}

// Send AppendEntries RPCs to Followers ---------------------------------------
// Send followers AppendEntries, either as heartbeat or with state machine
// changes.

// Send AppendEntries RPCs to all followers.
// REQUIRES r.mu
func (r *Raft) sendAppendEntries() {
	for p := range r.peers {
		if p == r.me {
			continue
		}

		//fill out appendentries rpc to send to followers
		var ni = len(r.log) //next index
			args := appendEntriesArgs{
				Term:         r.currentTerm,
				LeaderID:     r.me,
				PrevLogTerm:  r.log[ni-1].Term,
				PrevLogIndex: ni - 1,
				Entries:      r.log[ni-1:],
				LeaderCommit: r.commitIndex,
			}
			go r.sendAppendEntry(p, args)
	}
}

// Send a single AppendEntries RPC to a follower. After it returns, update state
// based on the response.
// EXCLUDES r.mu
func (r *Raft) sendAppendEntry(peer int, args appendEntriesArgs) {
	// Call AppendEntries on peer.
	glog.V(6).Infof("%d calling ae on %d", r.me, peer)
	var reply appendEntriesReply
	if ok := r.callAppendEntries(peer, args, &reply); !ok {
		glog.V(6).Infof("send appendentry RPC to %d failed.", peer)
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	//our term is old, need to learn the new time
	if reply.Term > r.currentTerm{
		glog.V(6).Infof("stepping down")
		r.currentTerm = reply.Term
		r.state = follower
		r.votedFor = -1
		r.persist()
		return
	}

	if reply.Success{
		//only update nextindex and match index if there's something in the log entries
		if args.Entries != nil {
			r.nextIndex[peer] = reply.NextIndex + len(args.Entries) - 1
			r.matchIndex[peer] = reply.NextIndex - 1 + len(args.Entries) - 1
			return
		}
	}

	//resend appendentries with right index and entries to catch a follower up!
	if !reply.Success{
		r.nextIndex[peer] = reply.NextIndex

		//retry
		if reply.NextIndex < len(r.log) {
			args := appendEntriesArgs{
				Term:         r.currentTerm,
				LeaderID:     r.me,
				PrevLogTerm:  r.log[reply.NextIndex].Term,
				PrevLogIndex: reply.NextIndex - 1,
				Entries:      r.log[reply.NextIndex-1:],
				LeaderCommit: r.commitIndex,
			}
			r.sendAppendEntry(peer, args)
		}
	}

	return
}

// Incoming RPC Handlers ------------------------------------------------------
// Handlers for incoming AppendEntries and RequestVote requests. Should populate
// reply based on the current state.

// RequestVote RPC handler.
// EXCLUDES r.mu
func (r *Raft) RequestVote(args requestVoteArgs, reply *requestVoteReply) {
	r.mu.Lock()
	defer r.mu.Unlock()

	//our term is old, we need to catch up
	if args.Term > r.currentTerm{
		r.currentTerm = args.Term
		r.state = follower
		r.votedFor = -1
	}

	//don't grant vote to a candidate whose term is old!
	if r.currentTerm > args.Term{
		glog.V(6).Infof("vote NOT granted to %v", args.CandidateID)
		reply.VoteGranted = false
		reply.Term = r.currentTerm
	}

	//only grant vote if the candidate's log is ahead of its own log
	//only vote if it hasn't voted before
	if (r.votedFor == -1 || r.votedFor == args.CandidateID) && (args.LastLogIndex >= len(r.log)-1){
		r.votedFor = args.CandidateID
		reply.VoteGranted = true
		reply.Term = r.currentTerm
		glog.V(6).Infof("vote granted to %v", args.CandidateID)
		r.resetTimer()
	}

	return
}

// AppendEntries RPC handler.
// EXCLUDES r.mu
func (r *Raft) AppendEntries(args appendEntriesArgs, reply *appendEntriesReply) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.resetTimer()

	//we receive an appendentries rpc from a new leader, go back to follower
	if r.state == candidate && args.LeaderID != r.me{
		r.state = follower
	}

	//our term is old, catch up to current term!
	if args.Term > r.currentTerm{
		r.currentTerm = args.Term
		r.state = follower
		r.votedFor = -1
		r.persist()
	}

	//leader sent an index that is too far ahead, let the leader know where our log is at to
	//get the right entries
	if args.PrevLogIndex > len(r.log){
		reply.Term = r.currentTerm
		reply.Success = false
		reply.NextIndex = len(r.log)
		return
	}

	//ignore old term
	if args.Term < r.currentTerm && len(r.log) >= args.PrevLogTerm{
		reply.Term = r.currentTerm
		reply.Success = false
		reply.NextIndex = len(r.log)
		return
	}

	//handle heartbeat
	if len(args.Entries) == 0{
		reply.Term = r.currentTerm
		reply.Success = true
		reply.NextIndex = len(r.log)
		r.commitIndex = args.LeaderCommit
		return
	}

	//rule 1- leader term is old, reject leader
	if args.Term < r.currentTerm{
		reply.Term = r.currentTerm
		reply.Success = false
		reply.NextIndex = len(r.log)-1
		return
	}

	//our next spot is empty, go ahead and append that entries, rule 4
	if args.PrevLogIndex == len(r.log) {
		for i := 0; i < len(args.Entries); i++ {
			r.log = append(r.log, args.Entries[i])
		}

		reply.Term = r.currentTerm
		reply.Success = true
		reply.NextIndex = len(r.log)
		//advance commit
		if args.LeaderCommit > r.commitIndex{
			r.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(len(r.log))-1))
		}
		return
	}

	//optimization
	//our term in our log is wrong, back up for that term and tell leader what to send over
	if r.log[args.PrevLogIndex].Term != args.PrevLogTerm{
		badterm := r.log[args.PrevLogIndex].Term
		ci := args.PrevLogIndex //conflicted index
		//back up for that bad term in log
		for i := args.PrevLogIndex; i > 0; i--{
			if r.log[i].Term == badterm{
				ci--
			} else{
				break
			}
		}
		reply.Term = r.currentTerm
		reply.Success = false
		reply.NextIndex = ci+1
		return
	}

	//rule 3, overwrite bad log entries
	if len(args.Entries) > 0 {
		//if there's only 1 entry and it matches our entry, don't need to append, just return true
		if len(args.Entries) == 1 && r.log[args.PrevLogIndex].Term == args.Entries[0].Term{
				reply.Term = r.currentTerm
				reply.Success = true
				reply.NextIndex = len(r.log)

				//advance commit
				if args.LeaderCommit > r.commitIndex{
					r.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(len(r.log)-1)))
				}
				return
			}

		//an existing entries conflicts with a new one, overwrite that entry and everything after
		if r.log[args.PrevLogIndex].Term != args.Entries[0].Term {
			for i := 0; i < len(args.Entries); i++ {
				if r.log[args.PrevLogIndex].Term == args.Entries[i].Term {
					r.log = append(r.log[:args.PrevLogIndex+i], args.Entries[i])
				}
			}
		} else {
			//if it matches, append after that entry
			if r.log[args.PrevLogIndex].Term == args.Entries[0].Term {
				for i := 1; i < len(args.Entries); i++ {
					r.log = append(r.log[:args.PrevLogIndex+i], args.Entries[i])
				}
			}
			//advance commit
			if args.LeaderCommit > r.commitIndex{
				r.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(len(r.log))-1))
			}
			reply.Term = r.currentTerm
			reply.Success = true
			reply.NextIndex = len(r.log)
			return
		}

	}

	//rule 4, append new entries
	for i := 0; i < len(args.Entries); i++ {
		r.log = append(r.log[:args.PrevLogIndex+1+i], args.Entries[i])
	}


	//5. if leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry
	if args.LeaderCommit > r.commitIndex{
		r.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(len(r.log))-1))
	}

	reply.Term = r.currentTerm
	reply.Success = true
	reply.NextIndex = len(r.log)
	return
}

// Commit-Related Functions ---------------------------------------------------
// Functions to update the leader commit index based on the match indices of all
// followers, as well as to apply all outstanding commits.

// Update commitIndex for the leader based on the match indices of followers.
// REQUIRES r.mu
func (r *Raft) updateCommitIndex() {

	n := r.commitIndex
	mcount := 0
	var majority [] int
	var lowestn int

	//find N in peers matchindex
	for i := 0 ; i < len(r.matchIndex); i++{
		if n <= r.matchIndex[i] && r.matchIndex[i] > r.commitIndex{
			mcount ++
			n = r.matchIndex[i]
			majority = append(majority, n)
		}
	}
	//find lowest n
	if (len(majority) > 0) {
		lowestn = majority[0]
		for i := 1; i < len(majority); i++ {
			if majority[i] < lowestn {
				lowestn = majority[i]
			}
		}
	}

	//if majority have this match index, set leader commit to it
	if len(r.log) > lowestn {
		if mcount >= len(r.peers)/2 && r.log[lowestn].Term == r.currentTerm {
			r.commitIndex = lowestn
			return
		}
	}

	return
}

// Commit any outstanding committable indices.
// EXCLUDES r.mu
func (r *Raft) commit() {
	r.mu.Lock()
	defer r.mu.Unlock()

	//commit everything that hasn't been committed since last timer
	//skip over index 0, that's why we  +1 to lastapplied
	if r.commitIndex > r.lastApplied {
		for i := r.lastApplied + 1; i <= r.commitIndex; i++ {
			r.apply <- ApplyMsg{
				CommandIndex: i,
				Command:      r.log[i].Command,
			}
		}
		r.lastApplied = r.commitIndex
	}
	return
}

// Persistence Functions ------------------------------------------------------
// Functions to persist and to read from persistent state. recoverState should
// only be called upon recovery (in CreateRaft). persist should be called
// whenever the durable state changes.

// Save persistent state so that it can be recovered from the persistence layer
// after process recovery.
// REQUIRES r.mu
func (r *Raft) persist() {
	if r.persister == nil {
		// Null persister; called in test.
		return
	}

	//encode raft state to buffer
	buf := new(bytes.Buffer)
	e := gob.NewEncoder(buf)

	//encode persistent state - current term, voted for, log
	err := e.Encode(&r.currentTerm)
	common.DieIfError(err)
	err = e.Encode(&r.votedFor)
	common.DieIfError(err)
	err = e.Encode(&r.log)
	common.DieIfError(err)
	data := buf.Bytes()
	r.persister.WritePersistentState(data)

	return
}

// Called during recovery with previously-persisted state.
// REQUIRES r.mu
func (r *Raft) recoverState(data []byte) {
	if data == nil || len(data) < 1 { // Bootstrap.
		r.log = append(r.log, LogEntry{0, nil})
		r.persist()
		return
	}

	if r.persister == nil {
		// Null persister; called in test.
		return
	}

	//recover from persisted data
	buf := bytes.NewBuffer(data)
	d := gob.NewDecoder(buf)
	var term int
	var votedfor int
	var log[] LogEntry

	//decode into vars and assign it to our state
	err := d.Decode(&term)
	r.currentTerm = term
	err = d.Decode(&votedfor)
	r.votedFor = votedfor
	err = d.Decode(&log)
	r.log = log
	common.DieIfError(err)

	return
}

// Outgoing RPC Handlers ------------------------------------------------------
// Call these to invoke a given RPC on the given client.

// Call RequestVote on the given server. reply will be populated by the
// receiver server.
func (r *Raft) requestVote(server int, args requestVoteArgs, reply *requestVoteReply) {
	if ok := r.callRequestVote(server, args, reply); !ok {
		glog.V(6).Infof("request vote RPC to %d failed.", server)
		return
	}
}

// Call AppendEntries on the given server. reply will be populated by the
// receiver server.
func (r *Raft) appendEntries(server int, args appendEntriesArgs, reply *appendEntriesReply) {
	if ok := r.callAppendEntries(server, args, reply); !ok {
		glog.V(6).Infof("appendentries RPC to %d failed.", server)
		return
	}
}
