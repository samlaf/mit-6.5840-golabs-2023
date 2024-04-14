package raft

import "log"

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate’s term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry (§5.4)
	LastLogTerm  int // term of candidate’s last log entry (§5.4)
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	if LogRpcCalls {
		log.Printf("[%d<-%d] received requestVote in term %d", rf.me, args.CandidateId, args.Term)
	}
	// Your code here (2A, 2B).
	// 1. Reply false if term < currentTerm (§5.1) so that candidate can update its term
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// if we are behind, then we update our term, become follower, and vote for the requesting candidate
	// note that we disregard the response (whether we downgraded or not), because what we do below is same in both cases
	_ = rf.maybeUpdateTermAndDowngradeToFollower(args.Term, false)
	// 2. If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	reply.Term = rf.currentTerm
	if (rf.votedFor == VotedForNoOne || rf.votedFor == args.CandidateId) && args.LastLogIndex >= len(rf.log) {
		log.Printf("[%d] voting for %d in term %d", rf.me, args.CandidateId, rf.currentTerm)
		rf.mu.Lock()
		rf.votedFor = args.CandidateId
		rf.mu.Unlock()
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	if LogRpcCalls {
		log.Printf("[%d->%d] sending requestVote", rf.me, server)
	}
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int        // leader’s term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // leader’s commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

// AppendEntries rpc is invoked by leader to replicate log entries (§5.3); also used as heartbeat (§5.2).
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.receivedHeartbeatBeforeElectionTimeout.Store(true)
	if LogRpcCalls {
		log.Printf("[%d<-%d] received appendEntries in term %d", rf.me, args.LeaderId, args.Term)
	}

	if args.Term < rf.currentTerm {
		// AppendEntries RPC rule 1. Reply false if term < currentTerm (§5.1)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// if we are behind, then we update our term, become follower, and vote for the requesting candidate
	// note that we disregard the response (whether we downgraded or not), because what we do below is same in both cases
	if downgraded := rf.maybeUpdateTermAndDowngradeToFollower(args.Term, true); downgraded {
		_ = downgraded
	}
	// AppendEntries RPC rule 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	if !(len(rf.log) > args.PrevLogIndex && rf.log[args.PrevLogIndex].Term == args.PrevLogTerm) {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	// TODO: implement these
	// 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
	// 4. Append any new entries not already in the log
	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	if LogRpcCalls {
		log.Printf("[%d->%d] sending appendEntries", rf.me, server)
	}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
