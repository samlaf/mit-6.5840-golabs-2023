package raft

import (
	"log"
	"time"
)

type State string

const (
	// Follower when someone else is leader (learn this from AppendEntries heartbeat rpcs)
	StateFollower State = "Follower"
	// Become Candidate when election timeout times out
	StateCandidate State = "Candidate"
	// Become Leader when receive a quorum of votes while a Candidate
	StateLeader State = "Leader"
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
	return rf.currentTerm, rf.state == StateLeader
}

func (rf *Raft) isFollower() bool {
	rf.mu.Lock()
	currentState := rf.state
	rf.mu.Unlock()
	return currentState == StateFollower
}

func (rf *Raft) isCandidate() bool {
	rf.mu.Lock()
	currentState := rf.state
	rf.mu.Unlock()
	return currentState == StateCandidate
}

func (rf *Raft) isLeader() bool {
	rf.mu.Lock()
	currentState := rf.state
	rf.mu.Unlock()
	return currentState == StateLeader
}

// Update the Raft state after receiving a term from another server,
// whether from an rpc or a response to an rpc call
// returns true if downgrade happened; false instead
func (rf *Raft) maybeUpdateTermAndDowngradeToFollower(otherServerTerm int, fromAppendEntries bool) bool {
	if otherServerTerm > rf.currentTerm {
		// Rule For All Servers: If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
		log.Printf("\033[31m[%d] downgrading to follower in term %d (prev term: %d)\033[0m", rf.me, otherServerTerm, rf.currentTerm)
		rf.mu.Lock()
		rf.currentTerm = otherServerTerm
		rf.state = StateFollower
		rf.votedFor = VotedForNoOne
		rf.log = []LogEntry{}
		rf.mu.Unlock()
		return true
	} else if otherServerTerm == rf.currentTerm && fromAppendEntries && rf.isCandidate() {
		// we received appendEntry while we are candidate.. have to downgrade to follower
		log.Printf("\033[31m[%d] downgrading to follower from candidate in term %d\033[0m", rf.me, rf.currentTerm)
		rf.mu.Lock()
		rf.state = StateFollower
		// this is coming from an appendEntries rpc call so we don't change vote or logEntry
		rf.mu.Unlock()
	}
	return false
}

// returns true if upgrade happened; false instead
func (rf *Raft) maybeUpgradeToCandidate(electionTimeout time.Duration) bool {
	if rf.isFollower() && !rf.receivedHeartbeatBeforeElectionTimeout.Load() {
		log.Printf("\033[33m[%d] upgrading to candidate in term %d (election timeout: %dms)\033[0m", rf.me, rf.currentTerm+1, electionTimeout.Milliseconds())
		rf.mu.Lock()
		rf.currentTerm++
		rf.state = StateCandidate
		rf.votedFor = rf.me
		rf.mu.Unlock()
		return true
	}
	rf.receivedHeartbeatBeforeElectionTimeout.Store(false)
	return false
}

func (rf *Raft) maybeUpgradeToLeader(votes []int) bool {
	if len(votes) >= 2*len(rf.peers)/3 {
		log.Printf("\033[32m[%d] upgrading to leader in term %d (received votes from %v)\033[0m", rf.me, rf.currentTerm, votes)
		rf.mu.Lock()
		rf.state = StateLeader
		rf.mu.Unlock()
		return true
	}
	return false
}
