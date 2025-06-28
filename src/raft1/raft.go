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

// --- 数据类型定义 ---

const (
	// 选举超时的基准时间 (例如 300ms)
	electionTimeoutBase = 400 * time.Millisecond
	// 心跳间隔 (必须远小于选举超时时间, 例如 120ms)
	heartbeatInterval = 120 * time.Millisecond
)

// Raft 节点有三种状态: Follower, Candidate, Leader
type State int

const (
	Follower State = iota
	Candidate
	Leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	persister *tester.Persister // Object to hold this peer's persisted state
	dead      int32             // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// --- 持久化状态 (在所有服务器上) ---
	// (在响应 RPC 请求之前，必须稳定地更新到存储中)
	currentTerm int        // 服务器已知晓的最新任期（在第一次启动时初始化为 0，单调递增）
	votedFor    int        // 在当前任期内收到选票的候选者 ID (如果没有则为 -1)
	log         []LogEntry // 日志条目；每个条目包含一个状态机命令和从领导者处接收到的任期号

	// --- 易失性状态 (在所有服务器上) ---
	commitIndex int // 已知的最大的已经被提交的日志条目的索引
	lastApplied int // 已经被应用到状态机的最高的日志条目的索引

	// --- 易失性状态 (在领导者上) ---
	// (选举后重新初始化)
	nextIndex  []int // 对于每一台服务器，发送到该服务器的下一个日志条目的索引（初始化为领导者最后的日志索引+1）
	matchIndex []int // 对于每一台服务器，已知的已经复制到该服务器的最高日志条目的索引（初始化为 0，单调递增）

	// --- 自定义实现所需的字段 ---
	mu    sync.Mutex          // 用于保护此结构体中共享数据的互斥锁
	state State               // 当前节点的状态 (Follower, Candidate, or Leader)
	me    int                 // 当前节点在 peers 数组中的索引/ID
	peers []*labrpc.ClientEnd // 所有对等节点的 RPC 客户端连接

	// 选举计时器. 当这个计时器触发时, Follower 会转变为 Candidate 并开始选举
	electionTimer *time.Timer

	// 心跳计时器. Leader 用它来定期发送心跳
	heartbeatTimer *time.Timer

	// 当选举成功或收到有效心跳时，通过这个 channel 来重置选举计时器
	resetElectionTimerCh chan struct{}

	// RPC 请求和回复的 channels
	requestVoteCh   chan RequestVoteArgs
	appendEntriesCh chan AppendEntriesArgs

	// (未来用于 K/V 存储) 一个 channel，用于将已提交的日志条目应用到状态机
	applyCh chan raftapi.ApplyMsg
}

// LogEntry 定义了日志条目的结构
type LogEntry struct {
	Term    int
	Command interface{} // 将要应用到状态机的命令
}

// RequestVoteArgs 是 RequestVote RPC 的参数结构体
type RequestVoteArgs struct {
	Term         int // 候选人的任期号
	CandidateID  int // 请求选票的候选人的 ID
	LastLogIndex int // 候选人的最后日志条目的索引值
	LastLogTerm  int // 候选人最后日志条目的任期号
}

// RequestVoteReply 是 RequestVote RPC 的回复结构体
type RequestVoteReply struct {
	Term        int  // 当前任期号，以便于候选人更新自己的任期
	VoteGranted bool // 候选人赢得了此张选票时为真
}

// AppendEntriesArgs 是 AppendEntries RPC 的参数结构体 (在选举阶段也用作心跳)
type AppendEntriesArgs struct {
	Term     int
	LeaderID int
	// ... (日志复制相关字段暂时忽略)
}

// AppendEntriesReply 是 AppendEntries RPC 的回复结构体
type AppendEntriesReply struct {
	Term    int
	Success bool
}

// --- 接口 ---

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
	// Your initialization code here (3A, 3B, 3C).
	rf := &Raft{
		peers:       peers,
		persister:   persister,
		me:          me,
		applyCh:     applyCh,
		state:       Follower, // 初始状态为 Follower
		currentTerm: 0,
		votedFor:    -1,                  // -1 表示尚未投票
		log:         make([]LogEntry, 1), // log index 从 1 开始，所以用一个空条目占据 index 0
		commitIndex: 0,
		lastApplied: 0,
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
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

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isleader := (rf.state == Leader)
	return term, isleader
}

// --- RPC 调用 ---

// RequestVote RPC handler.
// 这个方法必须符合 rpc.Register 的格式要求。
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 规则 1: 如果请求的任期(args.Term)小于当前任期(rf.currentTerm)，拒绝投票
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// 如果收到的请求任期更高，无论如何都要先更新自己的任期并转为 Follower
	if args.Term > rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
	}

	// 规则 2: 投票资格检查
	// a) 当前节点尚未在本任期内投票 (rf.votedFor == -1)
	// b) 或者已经投给了该候选人 (rf.votedFor == args.CandidateID)
	// 并且 c) 候选人的日志至少和自己一样新 (isLogUpToDate 检查)
	canVote := rf.votedFor == -1 || rf.votedFor == args.CandidateID
	logIsUpToDate := true // 在只实现选举时，暂时假设日志总是最新的
	// logIsUpToDate := isLogUpToDate(args.LastLogTerm, args.LastLogIndex, rf.getLastLogTerm(), rf.getLastLogIndex())

	if canVote && logIsUpToDate {
		// 同意投票
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID // 记录下投给了谁
		// **关键**：同意投票后，重置自己的选举计时器，因为候选人有潜力成为 Leader
		rf.electionTimer.Reset(randomizedElectionTimeout())
	} else {
		reply.VoteGranted = false
	}

	reply.Term = rf.currentTerm
	return
}

// AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 规则 1: 如果 Leader 的任期(args.Term)小于当前任期(rf.currentTerm)，
	// 这是一个过时的 Leader，拒绝它的请求。
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// **关键逻辑**：
	// 无论当前节点是什么状态（Follower, Candidate, 甚至是另一个任期较低的 Leader），
	// 只要收到一个任期不低于自己的 Leader 的心跳，就必须无条件转为 Follower。
	// 这确保了集群中任一时刻只有一个 Leader。
	rf.becomeFollower(args.Term)

	// **重要**：收到有效心跳，说明 Leader 还在，重置选举计时器，推迟下一轮选举。
	rf.electionTimer.Reset(randomizedElectionTimeout())

	reply.Term = rf.currentTerm
	reply.Success = true

	// (未来日志复制的逻辑会在这里添加...)

	return
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
//
// 示例代码，向服务器发送 RequestVote RPC。
// server 是目标服务器在 rf.peers[] 中的索引。
// RPC 参数预期在 args 中。
// *reply 中填充 RPC 回复，因此调用者应
// 传入 &reply。
// 传递给 Call() 的 args 和 reply 参数的类型必须
// 与处理函数中声明的参数类型完全一致
// （包括它们是否为指针）。
//
// labrpc 包模拟了一个有损网络，其中服务器
// 可能无法访问，并且请求和回复可能丢失。
// Call() 发送一个请求并等待回复。如果回复在
// 超时时间内到达，Call() 返回 true；否则
// Call() 返回 false。因此 Call() 可能会等待一段时间才返回。
// 返回 false 的原因可能是服务器宕机、无法访问的活动服务器、
// 请求丢失或回复丢失。
//
// Call() 保证会返回（可能会有延迟），*除非*
// 服务器端的处理函数没有返回。因此，
// 无需在 Call() 周围实现您自己的超时机制。
//
// 有关更多详细信息，请查阅 ../labrpc/labrpc.go 中的注释。
//
// 如果您在使 RPC 工作时遇到问题，请检查您是否
// 将通过 RPC 传递的结构体中的所有字段名都大写了，并且
// 调用者传递的是回复结构体的地址（带 &），而不是
// 结构体本身。
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// --- 实现 ---

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
	// 初始化计时器
	rf.electionTimer = time.NewTimer(randomizedElectionTimeout())
	rf.heartbeatTimer = time.NewTimer(heartbeatInterval)
	// 领导者一开始不发送心跳，所以先停止心跳计时器
	rf.heartbeatTimer.Stop()

	// Your code here (3A)
	// Check if a leader election should be started.
	for !rf.killed() {
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			// 在成为 Candidate 之前，再次确认自己还是 Follower 或 Candidate
			if rf.state == Follower || rf.state == Candidate {
				// becomeCandidate 内部会处理加锁，所以这里可以先解锁
				rf.mu.Unlock()
				rf.becomeCandidate()
			} else {
				// 如果已经成为了 Leader，就什么都不做
				rf.mu.Unlock()
			}

		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			// 确保自己仍然是 Leader
			if rf.state == Leader {
				// sendHeartbeats 内部会处理锁，所以先解锁
				rf.mu.Unlock()
				rf.sendHeartbeats()
				// 重置计时器也应该在锁外进行
				rf.heartbeatTimer.Reset(heartbeatInterval)
			} else {
				rf.mu.Unlock()
			}
		}
	}
}

// randomizedElectionTimeout 生成一个随机的选举超时时间
// 这对于防止多个节点同时超时、同时发起选举（导致投票分裂）至关重要
func randomizedElectionTimeout() time.Duration {
	// 例如，在 [300ms, 450ms] 之间选择一个随机值
	return electionTimeoutBase + time.Duration(rand.Intn(150))*time.Millisecond
}

// becomeFollower 切换节点状态为 Follower (对外暴露的接口)
// 这个函数会先获取锁，然后调用内部的非锁版本
func (rf *Raft) becomeFollower(term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.becomeFollowerLocked(term) // 调用内部函数
}

// becomeFollowerLocked 是实际的状态转换逻辑，它假设调用者已经持有锁。
// 函数名后缀 "Locked" 是一种常见的约定。
func (rf *Raft) becomeFollowerLocked(term int) {
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = -1

	rf.electionTimer.Reset(randomizedElectionTimeout())
	rf.heartbeatTimer.Stop()
}

// becomeCandidate 切换节点状态为 Candidate 并开始选举
func (rf *Raft) becomeCandidate() {
	rf.mu.Lock()

	rf.state = Candidate
	rf.currentTerm++    // 任期加1
	rf.votedFor = rf.me // 给自己投票

	// 重置选举计时器，为新一轮选举计时
	rf.electionTimer.Reset(randomizedElectionTimeout())

	// log.Printf("Node %d became Candidate, starting election for term %d", rf.me, rf.currentTerm)

	// 准备 RequestVote RPC 的参数
	args := RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateID: rf.me,
		// ... LastLogTerm 和 LastLogIndex 字段
	}
	rf.mu.Unlock() // 在发送 RPC 前解锁，避免阻塞

	// 为自己获得一票
	votesGranted := 1

	// 并发地向所有其他节点发送投票请求
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(serverIndex int) {
			reply := RequestVoteReply{}
			// `sendRequestVote` 是你封装的 RPC 调用函数
			ok := rf.sendRequestVote(serverIndex, &args, &reply)

			if !ok {
				return // RPC 失败，直接返回
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			// 如果对方的任期比我们还大，说明我们已经过时了，立刻转为 Follower
			if reply.Term > rf.currentTerm {
				rf.becomeFollowerLocked(reply.Term)
				return
			}

			// 检查是否还在当前的选举状态（任期和角色都没变）
			if rf.state != Candidate || rf.currentTerm != args.Term {
				return
			}

			if reply.VoteGranted {
				votesGranted++
				// 如果获得超过半数的选票，立即成为 Leader
				if votesGranted > len(rf.peers)/2 {
					// **关键**：只在还是 Candidate 时才转换
					if rf.state == Candidate {
						// becomeLeader 内部不再调用 sendHeartbeats
						// 而是由 ticker 驱动
						rf.becomeLeader()
					}
				}
			}
		}(i)
	}
}

// becomeLeader 切换节点状态为 Leader
func (rf *Raft) becomeLeader() {
	// 调用此函数时，必须已经持有锁
	if rf.state != Candidate {
		// 可能在等待投票期间，已经收到了新 leader 的心跳，变成了 follower
		// 此时不应该再成为 leader
		return
	}

	rf.state = Leader
	// log.Printf("Node %d became Leader for term %d!", rf.me, rf.currentTerm)

	// (3B) 成为 Leader 后，需要初始化 nextIndex 和 matchIndex
	// rf.nextIndex = make([]int, len(rf.peers))
	// lastLogIndex := rf.getLastLogIndex()
	// for i := range rf.peers {
	//     rf.nextIndex[i] = lastLogIndex + 1
	// }
	// rf.matchIndex = make([]int, len(rf.peers))

	// 成为 Leader 后，停止选举计时器
	rf.electionTimer.Stop()

	// **关键修改**：不要在这里直接调用 sendHeartbeats()。
	// 而是立即发送一轮心跳，然后让 ticker 来周期性地发送。
	// 为了立即发送，我们可以手动重置心跳计时器让它马上触发，或者直接调用。
	// 更清晰的做法是，让 ticker 负责所有周期性事件。
	// 我们在这里立刻发送第一轮心跳，然后重置计时器。
	// 注意：sendHeartbeats 必须在锁外调用！
	go rf.sendHeartbeats()                     // 启动一个 goroutine 来发送初始心跳
	rf.heartbeatTimer.Reset(heartbeatInterval) // 重置心跳计时器以维持领导
}

// sendHeartbeats 向所有 Follower 发送心跳
func (rf *Raft) sendHeartbeats() {
	rf.mu.Lock()
	// 如果在我们准备发送心跳时，状态已经不是 Leader 了，就直接返回
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	// log.Printf("Leader %d sending heartbeats for term %d", rf.me, rf.currentTerm)
	args := AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderID: rf.me,
	}
	rf.mu.Unlock() // **在 RPC 调用前释放锁！**

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(serverIndex int) {
			reply := AppendEntriesReply{}
			// 复制一份参数，以防万一
			localArgs := args
			ok := rf.sendAppendEntries(serverIndex, &localArgs, &reply)
			if ok {
				// 处理回复 (现在可以忽略，但之后很重要)
				rf.mu.Lock()
				// 检查收到的任期，如果发现更大的任期，自己需要退位
				if reply.Term > rf.currentTerm {
					rf.becomeFollowerLocked(reply.Term)
				}
				rf.mu.Unlock()
			}
		}(i)
	}
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

// --- 持久化 ---

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
