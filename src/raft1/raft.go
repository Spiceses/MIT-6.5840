package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

const (
	Follower = iota
	Candidate
	Leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state
	currentTerm int
	votedFor    int
	log         []Log

	// Volatilestate
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	state           int
	lastContact     time.Time
	electionTimeout time.Duration
}

type Log struct {
	command interface{}
	term    int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
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
	// Your code here (3C).
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

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// check candidate’s log is at least as up-to-date as receiver’s log
		if args.LastLogTerm < rf.log[len(rf.log)-1].term {
			reply.VoteGranted = false
			return
		}
		if args.LastLogTerm == rf.log[len(rf.log)-1].term {
			if args.LastLogIndex < (len(rf.log) - 1) {
				reply.VoteGranted = false
				return
			}
		}
		reply.VoteGranted = true
		return
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
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
//
// 使用 Raft 的服务（例如键/值服务器）希望就要附加到 Raft 日志的下一个命令启动一致性协议。
// 如果当前服务器不是 Leader，则返回 false。
// 否则，立即启动一致性协议并返回。
// 不保证此命令最终会提交到 Raft 日志，因为 Leader
// 可能会失败或在选举中失利。
// 即使 Raft 实例已被终止，此函数也应优雅地返回。
//
// 第一个返回值是如果命令最终被提交，它将出现的日志索引。
// 第二个返回值是当前任期。第三个返回值如果此服务器认为它是
// Leader，则为 true。
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// ticker 函数是 Raft 节点的核心驱动循环，在后台 goroutine 中持续运行。
// 它的主要职责是根据节点的当前状态（Follower, Candidate, Leader）来驱动周期性事件，
// 例如发起选举。
//
// 当节点是 Follower 或 Candidate 时，ticker 充当选举计时器。它会等待一个随机
// 的选举超时时间。如果在此期间计时器没有被重置（例如，通过收到领导者的有效心跳
// 或投票给其他候选人），它就会发起一次新的领导者选举。随机化的超时时间对于
// 减少选举中出现“分裂投票”（split votes）的情况至关重要。
//
// 这个循环会通过调用 rf.killed() 方法来检查是否应该退出，确保 goroutine 能够被干净地终止。
func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here (3A)
		// Check if a leader election should be started.

		switch rf.state {
		case Follower, Candidate:
			// 现有逻辑：检查选举超时并发起选举
			if time.Since(rf.lastContact) > rf.electionTimeout {
				rf.startElection()
			}
			// 等待一个随机时间
			ms := 50 + (rand.Int63() % 300)
			time.Sleep(time.Duration(ms) * time.Millisecond)

		case Leader:
			// 作为 Leader，周期性地发送心跳/日志
			rf.broadcastAppendEntries()

			// 睡一个固定的心跳间隔
			heartbeatInterval := 100 * time.Millisecond
			time.Sleep(heartbeatInterval)
		}
	}
}

func (rf *Raft) startElection() {
	rf.currentTerm++
	rf.state = Candidate
	rf.votedFor = rf.me
	rf.lastContact = time.Now()

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].term,
	}

	votes := 1
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(i, &args, &reply)

			if !ok {
				DPrintf("Node %d failed to send RequestVote to %d\n", rf.me, server)
				return
			}

			if reply.Term < rf.currentTerm {
				return
			}

			if reply.Term > rf.currentTerm {
				rf.state = Follower
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.lastContact = time.Now()
				return
			}

			if reply.VoteGranted {
				votes++
				if votes >= len(rf.peers)/2+1 {
					rf.state = Leader
					rf.lastContact = time.Now()
					rf.votedFor = -1

					for j := 0; j < len(rf.peers); j++ {
						rf.nextIndex[j] = len(rf.log) // Next entry to send to that server
						rf.matchIndex[j] = 0          // Highest log entry known to be replicated
					}
				}
			}
		}(i)
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
//
// Make 函数创建并初始化一个新的 Raft 服务器实例。
//
// peers 参数包含了集群中所有 Raft 服务器（包括当前服务器）的 RPC 端点。
// me 是当前服务器在 peers 切片中的索引。
// persister 用于保存服务器的持久化状态，并且在启动时也可能持有最近保存的状态。
// applyCh 是一个通道，Raft 实例通过它向其上层服务（或测试器）发送已提交的日志条目（封装为 ApplyMsg）。
//
// Make 函数必须迅速返回，因此任何长时间运行的工作（例如选举和心跳）都应该在独立的 goroutine 中启动。
// 函数会进行必要的初始化，包括从 persister 中恢复之前持久化的状态，并启动一个 ticker goroutine 来定期发起领导者选举。
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	// TODO: 初始化raft状态
	rf.currentTerm = 0
	rf.log = make([]Log, 1)

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.state = Follower
	rf.lastContact = time.Now()
	rf.electionTimeout = time.Duration(150 + (rand.Int63() % 150))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	reply.Term = rf.currentTerm
	rf.lastContact = time.Now()

	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
	}

	if args.PrevLogIndex >= len(rf.log) {
		reply.Success = false
		return
	}

	if rf.log[args.PrevLogIndex].term != args.PrevLogTerm {
		reply.Success = false
		rf.log = rf.log[0:args.PrevLogIndex]
		return
	}

	rf.log = append(rf.log[0:args.PrevLogIndex+1], args.Entries...)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
	}
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) broadcastAppendEntries() {
	for i := range rf.peers {
		if i == rf.me {
			continue // 跳过自己
		}

		go rf.replicateToPeer(i)
	}
}

func (rf *Raft) replicateToPeer(peerIndex int) {
	prevLogIndex := rf.nextIndex[peerIndex] - 1
	prevLogTerm := rf.log[prevLogIndex].term
	entries := make([]Log, len(rf.log[prevLogIndex+1:]))
	copy(entries, rf.log[prevLogIndex+1:])

	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}

	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(peerIndex, &args, &reply)
	if !ok {
		return
	}

	if reply.Success {
		rf.matchIndex[peerIndex] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[peerIndex] = rf.matchIndex[peerIndex] + 1
	} else {
		if reply.Term > rf.currentTerm {
			rf.state = Follower
			rf.currentTerm = reply.Term
			rf.votedFor = -1
		} else {
			// 日志不一致，后退 nextIndex 并重试
			// 为了防止快速失败导致的大量 RPC，可以实现更优化的后退策略
			rf.nextIndex[peerIndex]--
		}
	}
}
