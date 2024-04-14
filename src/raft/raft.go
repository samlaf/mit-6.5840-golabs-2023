package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

const (
	VotedForNoOne        int           = -1
	LeaderHeartbeat      time.Duration = 100 * time.Millisecond
	MinElectionTimeoutMs int           = 500
	MaxElectionTimeoutMs int           = 1000

	LogRpcCalls bool = true
)

// pause for a random amount of time between MinElectionTimeoutMs and MaxElectionTimeoutMs.
func (rf *Raft) getNewRandomElectionTimeout() time.Duration {
	diff := MaxElectionTimeoutMs - MinElectionTimeoutMs
	ms := MinElectionTimeoutMs + rand.Intn(diff)
	// log.Printf("[%d] sleeping for election timeout of %v ms", rf.me, ms)
	return time.Duration(ms) * time.Millisecond
}

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int // TODO: what is this?? index in the log?

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Command interface{} // command send by user
	Term    int         // term when entry was received by leader
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	applyCh   chan ApplyMsg
	dead      int32 // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state                                  State
	receivedHeartbeatBeforeElectionTimeout atomic.Bool // whether we have received a leader heartbeat (appendEntries rpc) during the current election timeout

	// Persistent state on all servers
	// TODO: is this needed? Or should this be in persister only?
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile state on all servers
	commitIndex int  // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied uint // index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	// Volatile state on leaders
	nextIndex  []uint // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []uint // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	log.Printf("[%d] killed", rf.me)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) candidateTicker() {
	for !rf.killed() {
		// sam: sleep at the beginning so that not all followers convert to candidate and request votes at the same time
		electionTimeout := rf.getNewRandomElectionTimeout()
		time.Sleep(electionTimeout)

		// Your code here (2A)
		// Check if a leader election should be started.

		// Rules For Followers: 4. If election timeout elapses without receiving AppendEntries RPC from current leader or granting vote to candidate: convert to candidate
		// TODO: do we also need to check in case state was changed elsewhere?
		rf.maybeUpgradeToCandidate(electionTimeout)
		if !rf.isCandidate() {
			continue
		}
		// If we are the candidate, then we try to become leader
		// by sending requestVote rpc requests and tallying votes until we get a quorum
		// TODO: Send rpcs in parallel
		peerVotesReceived := []int{rf.me} //peerIds who voted for us
		for i, _ := range rf.peers {
			if i == rf.me {
				continue
			}
			go func(i int) {
				var reply RequestVoteReply
				rf.sendRequestVote(i, &RequestVoteArgs{
					Term:        rf.currentTerm,
					CandidateId: rf.me,
					// TODO: implement these properly once we start dealing with logs
					LastLogIndex: 0,
					LastLogTerm:  0,
				}, &reply)
				if LogRpcCalls {
					log.Printf("[%d<-%d] received vote reply (%v) in term %d", rf.me, i, reply.VoteGranted, rf.currentTerm)
				}
				rf.maybeUpdateTermAndDowngradeToFollower(reply.Term, false)
				if reply.VoteGranted {
					peerVotesReceived = append(peerVotesReceived, i)
					rf.maybeUpgradeToLeader(peerVotesReceived)
				}
			}(i)
		}

	}
}

func (rf *Raft) leaderTicker() {
	for !rf.killed() {
		time.Sleep(LeaderHeartbeat)

		if !rf.isLeader() {
			continue
		}

		// TODO: send rpcs in parallel
		// also is there a way to merge this sending and the one for RequestVotes using a single common function?
		for i, _ := range rf.peers {
			if i == rf.me {
				continue
			}
			var reply AppendEntriesReply
			rf.sendAppendEntries(i, &AppendEntriesArgs{
				Term:     rf.currentTerm,
				LeaderId: rf.me,
				// TODO: implement these properly once we start dealing with logs
				PrevLogIndex: 0,
				PrevLogTerm:  0,
				Entries:      []LogEntry{},
				LeaderCommit: rf.commitIndex,
			}, &reply)
			rf.maybeUpdateTermAndDowngradeToFollower(reply.Term, false)
			// TODO: implement what to do if reply.Success is true
			//       I think we need to backoff on PrevLogIndex and resend
		}

	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.votedFor = -1
	rf.state = StateFollower

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections (when follower becomes candidate)
	go rf.candidateTicker()

	// start ticker goroutine to send appendLogEntries (as leader)
	go rf.leaderTicker()

	return rf
}
