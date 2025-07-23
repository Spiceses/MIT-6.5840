package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	// 	"bytes"

	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	// 	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

// --- 数据类型定义 ---

const (
	// 选举超时的基准时间 (例如 400ms)
	electionTimeoutBase = 400 * time.Millisecond
	// 心跳间隔 (必须远小于选举超时时间, 例如 120ms)
	heartbeatInterval = 120 * time.Millisecond
	// Ticker 循环的轮询间隔，用于检查状态
	tickInterval = 20 * time.Millisecond
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
	mu sync.Mutex // 用于保护此结构体中共享数据的互斥锁

	peers            []*labrpc.ClientEnd // 所有对等节点的 RPC 客户端连接
	me               int                 // 当前节点在 peers 数组中的索引/ID
	persister        *tester.Persister   // Object to hold this peer's persisted state
	state            State               // 当前节点的状态 (Follower, Candidate, or Leader)
	dead             int32               // set by Kill()
	electionDeadline time.Time           // 记录下一次选举超时的时间点

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

	// 用于日志压缩的辅助字段
	lastIncludedIndex int
	lastIncludedTerm  int
	snapshot          []byte

	// (未来用于 K/V 存储) 一个 channel，用于将已提交的日志条目应用到状态机
	applyCh chan raftapi.ApplyMsg

	// --- 条件变量 ---
	applyCond  *sync.Cond // 用于唤醒 applier goroutine 的条件变量
	commitCond *sync.Cond // 【新增】用于唤醒 commit-updater goroutine 的条件变量
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

// RequestVoteReply 是 RequestVote RPC 的回`复结构体
type RequestVoteReply struct {
	Term        int  // 当前任期号，以便于候选人更新自己的任期
	VoteGranted bool // 候选人赢得了此张选票时为真
}

// AppendEntriesArgs 是 AppendEntries RPC 的参数结构体 (在选举阶段也用作心跳)
type AppendEntriesArgs struct {
	Term     int
	LeaderID int
	// 日志复制相关字段暂时忽略
	Entries      []LogEntry
	LeaderCommit int
	PrevLogIndex int
	PrevLogTerm  int
}

// AppendEntriesReply 是 AppendEntries RPC 的回复结构体
type AppendEntriesReply struct {
	Term    int
	Success bool
	// 添加以下字段用于快速回溯
	XTerm  int // 冲突条目的任期号 (如果存在)
	XIndex int // 冲突任期的第一个条目的索引
	XLen   int // Follower 日志的长度
}

type InstallSnapshotArgs struct {
	Term              int
	Data              []byte
	LastIncludedIndex int
	LastIncludedTerm  int
}

type InstallSnapshotReply struct {
	Term int
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
// 服务或测试器希望创建一个Raft服务器。
// 所有Raft服务器（包括此服务器）的端口都在peers[]中。
// 此服务器的端口是peers[me]。
// 所有服务器的peers[]数组都具有相同的顺序。
// persister是此服务器保存其持久状态的地方，
// 并且最初也持有任何最近保存的状态。
// applyCh是一个通道，测试器或服务期望Raft在此通道上发送ApplyMsg消息。
// Make()必须快速返回，因此它应该为任何长时间运行的工作启动goroutine。
func Make(peers []*labrpc.ClientEnd, me int, persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	// 初始化 raft 节点状态
	rf := &Raft{
		peers:       peers,
		persister:   persister,
		me:          me,
		applyCh:     applyCh,
		state:       Follower, // 初始状态为 Follower
		currentTerm: 0,
		votedFor:    -1, // -1 表示尚未投票
		// log 初始化时就包含一个哨兵条目，对应 index 0
		log:         make([]LogEntry, 1),
		commitIndex: 0,
		lastApplied: 0,
		// 初始化时，快照为空，所以 lastIncludedIndex 和 lastIncludedTerm 都为 0
		lastIncludedIndex: 0,
		lastIncludedTerm:  0,
		snapshot:          persister.ReadSnapshot(),
	}
	// 哨兵条目的任期为 0，没有命令
	rf.log[0].Term = 0
	rf.log[0].Command = nil

	// 初始化条件变量
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.commitCond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// 初始化选举超时时间点
	rf.resetElectionTimerLocked()

	// start ticker goroutine to start elections
	go rf.ticker()

	// 启动专门的 applier goroutine
	go rf.applyEntries()

	// 【新增】启动专门的 commit-updater goroutine
	go rf.commitUpdater()

	DPrintf("raft 端点 %d 创建成功", me)

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
// 使用 Raft 的服务（例如，一个键/值服务器）想要就下一条将要附加到 Raft 日志的命令达成共识。
// 如果此服务器不是领导者（leader），则返回 false。否则，启动共识过程并立即返回。
// 并不保证此命令最终一定会被提交（committed）到 Raft 日志中，因为领导者可能会宕机或在选举中落败。
// 即使 Raft 实例已经被终止，此函数也应该能正常地返回。
//
// 第一个返回值是该命令如果被成功提交后，将会出现在日志中的索引（index）。
// 第二个返回值是当前的任期号（term）。
// 第三个返回值标识此服务器是否认为自己是领导者（leader），如果是则为 true。
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// 在整个函数执行期间上锁
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm

	// 检查是否为领导者
	isLeader = rf.state == Leader
	if !isLeader {
		return index, term, isLeader
	}

	log := LogEntry{
		Term:    term,
		Command: command,
	}

	// 将命令附加到下一条日志

	// -- 附加到 leader 日志
	index = len(rf.log) + rf.lastIncludedIndex
	rf.log = append(rf.log, log)

	// 调用持久化
	rf.persist()

	DPrintf("Leader %d 接收到命令, log index=%d, term=%d", rf.me, index, term)

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

// --- RPC 调用 ---
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = max(args.Term, rf.currentTerm)
	reply.VoteGranted = false

	// 规则 1: 如果请求的任期(args.Term)小于当前任期(rf.currentTerm)，拒绝投票
	if args.Term < rf.currentTerm {
		return
	}

	// 如果收到的请求任期更高，无论如何都要先更新自己的任期并转为 Follower
	if args.Term > rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
	}

	// 规则 2: 投票资格检查
	canVote := rf.votedFor == -1 || rf.votedFor == args.CandidateID

	// 使用辅助函数获取最后一条日志的绝对索引和任期
	lastLogIndex := rf.getLastLogIndex()
	lastLogTerm := rf.getLogTerm(lastLogIndex)

	logIsUpToDate := args.LastLogTerm > lastLogTerm ||
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)

	if !canVote || !logIsUpToDate {
		return
	}

	// 同意投票
	reply.VoteGranted = true
	rf.votedFor = args.CandidateID // 记录下投给了谁

	// 修改: 调用持久化
	rf.persist()

	// **关键**：同意投票后，重置自己的选举计时器
	rf.resetElectionTimerLocked()

	DPrintf("Node %d 在任期 %d 投票给 %d", rf.me, rf.currentTerm, args.CandidateID)
}

// AppendEntries RPC handler.
// Invoked by leader to replicate log entries; also used as heartbeat.
//
//	如果请求的 term（任期）小于当前服务器的 currentTerm，则回复 false (§5.1)
//	如果日志在 prevLogIndex 位置的条目任期与 prevLogTerm 不匹配，则回复 false (§5.3)
//	如果一个已存在的条目与新的条目发生冲突（相同的索引，但任期不同），则删除这个已存在的条目以及其后所有的条目 (§5.3)
//	附加所有日志中不存在的新条目
//	如果领导者的 leaderCommit 大于接收者的 commitIndex，则将 commitIndex 设置为 min(leaderCommit, 最后一条新条目的索引)
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 无论如何, 回复更新的 term
	reply.Term = max(rf.currentTerm, args.Term)
	// 默认发送不成功, 直到函数末尾
	reply.Success = false

	// 规则 1: 如果 Leader 的任期(args.Term)小于当前任期(rf.currentTerm)，拒绝。
	if args.Term < rf.currentTerm {
		return
	}

	// --- 到此已经确定发送方为 leader ---

	if args.Term > rf.currentTerm {
		// 只要收到一个任期不低于自己的 Leader 的心跳，就必须无条件转为 Follower。
		rf.becomeFollowerLocked(args.Term)
	} else if rf.state == Candidate {
		// 如果任期相同，且自己是 Candidate，只需改变状态即可，不要重置 votedFor
		rf.state = Follower
	}

	// 重置选举超时计时器
	rf.resetElectionTimerLocked()

	// --- 日志一致性检查 ---

	// 检查 PrevLogIndex 是否小于我们的快照点，如果是，说明是过时的 RPC
	if args.PrevLogIndex < rf.lastIncludedIndex {
		// 可以直接回复成功，因为这部分日志已经被提交和快照了
		reply.Success = true
		return
	}

	// 检查 PrevLogIndex 是否在我们的日志范围内
	if args.PrevLogIndex > rf.getLastLogIndex() {
		reply.Success = false
		reply.XLen = rf.getLastLogIndex() + 1 // Follower 的日志长度应该是 (绝对索引+1)
		reply.XTerm = -1
		reply.XIndex = -1
		return
	}

	// 检查 PrevLogTerm 是否匹配
	if rf.getLogTerm(args.PrevLogIndex) != args.PrevLogTerm {
		reply.Success = false
		reply.XTerm = rf.getLogTerm(args.PrevLogIndex)
		// 找到任期为 XTerm 的第一个条目的索引
		firstIndex := args.PrevLogIndex
		for firstIndex > rf.lastIncludedIndex && rf.getLogTerm(firstIndex-1) == reply.XTerm {
			firstIndex--
		}
		reply.XIndex = firstIndex
		reply.XLen = rf.getLastLogIndex() + 1
		return
	}

	// --- 到此已经确定之前的日志已经吻合 ---

	// --- 日志截断与追加 ---
	logChanged := false
	for i, entry := range args.Entries {
		absoluteIndex := args.PrevLogIndex + 1 + i

		// 如果这个绝对索引超过了我们的日志长度
		if absoluteIndex > rf.getLastLogIndex() {
			// 将所有剩余的 entries 追加到日志末尾
			rf.log = append(rf.log, args.Entries[i:]...)
			logChanged = true
			break
		}

		// 如果存在冲突 (任期不同)
		if rf.getLogTerm(absoluteIndex) != entry.Term {
			// 截断日志
			sliceIndex := rf.getSliceIndex(absoluteIndex)
			rf.log = rf.log[:sliceIndex]
			// 追加所有新条目
			rf.log = append(rf.log, args.Entries[i:]...)
			logChanged = true
			break
		}
	}

	if logChanged {
		rf.persist()
	}

	// --- 更新 commitIndex ---
	if args.LeaderCommit > rf.commitIndex {
		// commitIndex 取 LeaderCommit 和最新日志索引的较小值
		rf.commitIndex = min(args.LeaderCommit, rf.getLastLogIndex())
		rf.applyCond.Signal()
	}

	reply.Success = true
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = max(rf.currentTerm, args.Term)

	// 检查 rpc 是否过期
	if args.Term < rf.currentTerm {
		return
	}

	rf.becomeFollowerLocked(reply.Term)

	// 如果快照的索引小于等于自己的 commitIndex，说明这个快照是过时的
	if args.LastIncludedIndex <= rf.commitIndex {
		return
	}

	// 更新 raft 状态
	rf.snapshot = args.Data
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.commitIndex = max(rf.commitIndex, args.LastIncludedIndex)
	rf.lastApplied = max(rf.lastApplied, args.LastIncludedIndex)
	newLog := []LogEntry{{Term: args.LastIncludedTerm, Command: nil}}
	rf.log = newLog

	// 持久化
	rf.persist()

	go func() {
		rf.applyCh <- raftapi.ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
	}()
}

// sendRequestVote 发送投票请求
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// sendAppendEntries 发送心跳或日志
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	// DPrintf("raft %d send AppendEntries to %d", args.LeaderID, server)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// sendAppendEntries 发送心跳或日志
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	// DPrintf("raft %d send AppendEntries to %d", args.LeaderID, server)
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// --- 多线程实现 ---

// ---- 计时线程(follower, candidate) ----

// ticker 函数现在是一个统一的驱动循环。
// 它不再使用 select 和 time.Timer channel，而是定期（由 tickInterval 定义）
// 醒来，并根据当前节点的状态和时间条件来决定是否需要执行操作
// （发起选举或发送心跳）。
func (rf *Raft) ticker() {
	for !rf.killed() {
		// 每次循环都短暂休眠，形成轮询
		time.Sleep(tickInterval)

		rf.mu.Lock()
		// 根据当前状态执行不同的逻辑
		switch rf.state {
		case Follower, Candidate:
			// 检查选举是否超时
			if time.Now().After(rf.electionDeadline) {
				// 选举超时，立即开始新的选举
				rf.becomeCandidateLocked()
			}
		case Leader:
			// Leader 不需要检查选举超时，它负责发送心跳。
			// 这个逻辑被移到 `leaderHeartbeatLoop` 中，以简化 ticker。
			// 这里什么都不做，让 Leader 的专用 goroutine 处理心跳。
		}
		rf.mu.Unlock()
	}
}

// ---- 应用日志线程(all) ----

// applyEntries 是一个长期运行的 goroutine，负责将已提交的日志条目应用到状态机。
func (rf *Raft) applyEntries() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}

		lastApplied := rf.lastApplied
		commitIndex := rf.commitIndex

		// 检查是否有快照导致 lastApplied < lastIncludedIndex
		// 如果是，说明有些日志已经被快照，我们应该从快照后开始应用
		if lastApplied < rf.lastIncludedIndex {
			// 这种情况不应该在 applyEntries 发生，而是在 InstallSnapshot 之后
			// 但作为防御，可以处理
			lastApplied = rf.lastIncludedIndex
		}

		startSliceIndex := rf.getSliceIndex(lastApplied + 1)
		endSliceIndex := rf.getSliceIndex(commitIndex + 1)

		entriesToApply := make([]LogEntry, endSliceIndex-startSliceIndex)
		copy(entriesToApply, rf.log[startSliceIndex:endSliceIndex])

		rf.mu.Unlock()

		for i, entry := range entriesToApply {
			applyIndex := lastApplied + 1 + i
			rf.applyCh <- raftapi.ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: applyIndex,
			}
		}

		rf.mu.Lock()
		// 使用 max 确保 lastApplied 不会因为旧的 commitIndex 值而回退
		rf.lastApplied = max(rf.lastApplied, commitIndex)
		rf.mu.Unlock()
	}
}

// ---- 提交日志线程(leader) ----

// commitUpdater 是一个长期运行的 goroutine，负责检查并更新 leader 的 commitIndex
func (rf *Raft) commitUpdater() {
	for !rf.killed() {
		rf.mu.Lock()
		// 等待来自 RPC 回复处理器的信号
		// Wait 会自动释放锁并在等待时阻塞；被唤醒后会重新获取锁
		rf.commitCond.Wait()

		// 被唤醒后，我们持有锁，可以安全地调用 updateCommitIndex
		rf.updateCommitIndex()

		rf.mu.Unlock()
	}
}

// 更新 commitIndex 的逻辑, 假设已持有锁
func (rf *Raft) updateCommitIndex() {
	// 从日志的最后一个条目开始反向迭代，找到可以被提交的最高索引 N
	// 循环变量 N 现在是绝对索引
	for N := rf.getLastLogIndex(); N > rf.commitIndex; N-- {
		// 关键：要提交的日志必须是当前任期的。Raft 论文 5.4.2 节
		if rf.getLogTerm(N) != rf.currentTerm {
			continue
		}

		count := 1 // 先把自己算上
		for i := range rf.peers {
			if i != rf.me && rf.matchIndex[i] >= N {
				count++
			}
		}

		if count > len(rf.peers)/2 {
			// 找到了可以提交的 N
			rf.commitIndex = N
			rf.applyCond.Signal()
			// 找到第一个满足条件的就可以退出
			return
		}
	}
}

// ---- 心跳线程(leader) ----

// 【重构】为 Leader 状态创建一个专门的、清晰的循环来发送心跳。
// 这个 goroutine 在节点成为 Leader 时启动，并在其不再是 Leader 时退出。
func (rf *Raft) leaderHeartbeatLoop() {
	for !rf.killed() {
		rf.mu.Lock()

		// 如果不再是 Leader，此循环的使命结束
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}

		// 持有锁，立即发送一轮心跳（sendHeartbeats内部会解锁）
		rf.sendHeartbeatsLocked()
		rf.mu.Unlock()

		// 等待一个心跳间隔
		time.Sleep(heartbeatInterval)
	}
}

// 它在 `leaderHeartbeatLoop` 中被调用，调用时已持有锁。
// 它会立即广播心跳。
func (rf *Raft) sendHeartbeatsLocked() {
	DPrintf("Leader %d sending heartbeats for term %d", rf.me, rf.currentTerm)

	// 向所有的 follower 发送心跳或者日志复制 rpc
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		// 如果 follower 需要的日志已经被快照
		if rf.nextIndex[i] <= rf.lastIncludedIndex {
			// 要发送的 log 已经在快照中了, 我们需要让 follower 直接安装快照

			// 构造 snapshot 参数
			installSnapshotArgs := InstallSnapshotArgs{
				Term:              rf.currentTerm,
				Data:              rf.persister.ReadSnapshot(),
				LastIncludedIndex: rf.lastIncludedIndex,
				LastIncludedTerm:  rf.lastIncludedTerm,
			}

			go func(i int, args InstallSnapshotArgs) {
				reply := InstallSnapshotReply{}
				ok := rf.sendInstallSnapshot(i, &args, &reply)
				if !ok {
					return
				}

				rf.mu.Lock()
				defer rf.mu.Unlock()

				// 处理 snapshot rpc 回复
				if reply.Term > rf.currentTerm {
					rf.becomeFollowerLocked(reply.Term)
					return
				}

				rf.nextIndex[i] = args.LastIncludedIndex + 1
				rf.matchIndex[i] = args.LastIncludedIndex
			}(i, installSnapshotArgs)

			continue
		}

		prevLogIndex := rf.nextIndex[i] - 1
		prevLogTerm := rf.getLogTerm(prevLogIndex)

		// 获取要发送的日志条目
		sliceStartIndex := rf.getSliceIndex(rf.nextIndex[i])

		entriesToSend := make([]LogEntry, len(rf.log[sliceStartIndex:]))
		copy(entriesToSend, rf.log[sliceStartIndex:])

		appendEntriesArgs := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderID:     rf.me,
			LeaderCommit: rf.commitIndex,
			Entries:      entriesToSend,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
		}

		go func(i int, args AppendEntriesArgs) {
			reply := AppendEntriesReply{}
			ok := rf.sendAppendEntries(i, &args, &reply)
			if !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			// 检查状态是否已过时。如果当前任期已改变，或者我们不再是 Leader，
			// 那么这个 RPC 的回复就是陈旧的，应该直接丢弃。
			if rf.state != Leader || rf.currentTerm != args.Term {
				return
			}

			// 如果发现更大的任期，自己需要退位
			if reply.Term > rf.currentTerm {
				rf.becomeFollowerLocked(reply.Term)
				return
			}

			// --- 仍然是 leader ---

			// 检测 follower 的日志是否冲突
			if !reply.Success {
				// 【优化】使用 Follower 返回的信息进行快速回退
				//  最终我们通过更新 nextIndex[i] 来让下一次心跳消息发送合理的日志切片

				// 检测冲突情况是 follower 的日志过短还是条目冲突
				if reply.XTerm == -1 {
					// 情况1: Follower的日志太短，没有PrevLogIndex
					// 直接将 nextIndex 设置为 Follower 日志的长度
					rf.nextIndex[i] = reply.XLen
				} else {
					// 情况2: 在PrevLogIndex处存在任期冲突
					lastLogIndexWithXTerm := -1
					// Leader 在自己的日志中寻找任期为 XTerm 的最后一个条目
					for j := len(rf.log) - 1; j >= 0; j-- {
						if rf.log[j].Term == reply.XTerm {
							lastLogIndexWithXTerm = j
							break
						}
					}

					if lastLogIndexWithXTerm != -1 {
						// 如果找到了，说明Leader有这个任期的日志
						// 将 nextIndex 设置为这个任期的最后一个条目的下一位
						rf.nextIndex[i] = lastLogIndexWithXTerm + 1
					} else {
						// 如果没找到，说明Leader没有这个任期的日志
						// 直接将 nextIndex 设置为Follower报告的、该冲突任期的第一个条目索引
						rf.nextIndex[i] = reply.XIndex
					}
				}
				// 确保 nextIndex 至少为1
				if rf.nextIndex[i] < 1 {
					rf.nextIndex[i] = 1
				}

				return
			}

			// 更新 leader 的状态(nextIndex 和 matchIndex)
			newMatchIndex := args.PrevLogIndex + len(args.Entries)
			if newMatchIndex > rf.matchIndex[i] {
				rf.matchIndex[i] = newMatchIndex
				// 将要发送到节点 i 的日志索引更新为已经匹配的日志索引 + 1
				rf.nextIndex[i] = rf.matchIndex[i] + 1
			}

			// 检查是否可以更新 commitIndex
			rf.commitCond.Signal()
		}(i, appendEntriesArgs)
	}
}

// --- 状态转换逻辑 ---

// becomeFollowerLocked 是实际的状态转换逻辑，假设调用者已持有锁。
func (rf *Raft) becomeFollowerLocked(term int) {
	// DPrintf("Node %d became Follower in term %d", rf.me, rf.currentTerm)
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = -1
	rf.persist()
	// 重置选举超时时间点
	rf.resetElectionTimerLocked()
}

// 【重构】`becomeCandidate` 现在是一个内部函数 `becomeCandidateLocked`。
// 它假设调用时已持有锁，并负责状态转换和发起并发的投票请求。
// ticker 循环会在选举超时后调用它。
func (rf *Raft) becomeCandidateLocked() {
	rf.state = Candidate
	rf.currentTerm++    // 任期加1
	rf.votedFor = rf.me // 给自己投票

	rf.persist()

	// 重置下一次的选举计时器
	rf.resetElectionTimerLocked()

	DPrintf("Node %d became Candidate, starting election for term %d", rf.me, rf.currentTerm)

	// 准备 RequestVote RPC 的参数
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.getLogTerm(rf.getLastLogIndex()),
	}

	// 为自己获得一票
	// 使用原子变量，因为它会被多个 goroutine 并发地增加
	count := 1

	// 并发地向所有其他节点发送投票请求
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(serverIndex int) {
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(serverIndex, &args, &reply)
			if !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			// 如果对方的任期比我们还大，说明我们已经过时了，立刻转为 Follower
			if reply.Term > rf.currentTerm {
				rf.becomeFollowerLocked(reply.Term)
				return
			}

			// 检查我们是否还在当初发起选举时的状态
			if rf.state != Candidate || rf.currentTerm != args.Term {
				return
			}

			if reply.VoteGranted {
				count++
				// 检查是否获得超过半数的选票
				if count > len(rf.peers)/2 {
					rf.becomeLeaderLocked()
				}
			}
		}(i)
	}
}

// 假设已持有锁。
func (rf *Raft) becomeLeaderLocked() {
	rf.state = Leader
	DPrintf("Node %d became Leader for term %d!", rf.me, rf.currentTerm)

	// 成为 Leader 后，需要初始化 nextIndex 和 matchIndex
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// 初始化 nextIndex 为 Leader 日志的下一个绝对索引
	leaderNextIndex := rf.getLastLogIndex() + 1

	for i := range rf.peers {
		rf.nextIndex[i] = leaderNextIndex
		// matchIndex 初始化为 0
	}

	go rf.leaderHeartbeatLoop()
}

// --- 持久化 ---

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	// 编码需要持久化的字段
	err := e.Encode(rf.currentTerm)
	if err != nil {
		DPrintf("编码 rf.currentTerm 失败: %v", err)
	}
	err = e.Encode(rf.votedFor)
	if err != nil {
		DPrintf("编码 rf.votedFor 失败: %v", err)
	}
	err = e.Encode(rf.log)
	if err != nil {
		DPrintf("编码 rf.log 失败: %v", err)
	}
	err = e.Encode(rf.lastIncludedIndex)
	if err != nil {
		DPrintf("编码 rf.lastIncludedIndex 失败: %v", err)
	}
	err = e.Encode(rf.lastIncludedTerm)
	if err != nil {
		DPrintf("编码 rf.lastIncludedTerm 失败: %v", err)
	}
	raftstate := w.Bytes()

	rf.persister.Save(raftstate, rf.snapshot)
}

// restore previously persisted state.
// 服务器初始化或重启时应读取持久化的状态
func (rf *Raft) readPersist(data []byte) {
	if len(data) < 1 {
		// 如果没有持久化状态，确保哨兵存在
		rf.log = make([]LogEntry, 1)
		rf.log[0].Term = 0
		rf.log[0].Command = nil
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var raftLog []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int

	// 按顺序解码
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&raftLog) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		// 如果解码出错，可以记录日志或直接 panic
		DPrintf("Node %d readPersist 解码失败", rf.me)
	} else {
		// 成功解码，恢复状态
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = raftLog
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		// 其他需要更新的 raft 状态
		rf.commitIndex = lastIncludedIndex
		rf.lastApplied = lastIncludedIndex
	}
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 检测是否快照是否更旧, 防止回滚
	if index <= rf.lastIncludedIndex {
		return
	}

	// 缓存快照
	rf.snapshot = snapshot

	// 获取要被快照覆盖的最后一个条目的任期
	newLastIncludedTerm := rf.getLogTerm(index)

	// 创建一个新的日志切片
	// 新的长度是原长度减去被快照覆盖的条目数
	// 简单地进行 rf.log = rf.log[sliceIndex:] 并不会释放底层数组的引用
	sliceIndex := rf.getSliceIndex(index)
	newLog := make([]LogEntry, len(rf.log)-sliceIndex)
	copy(newLog, rf.log[sliceIndex:])

	// --- 更新 Raft 状态 ---

	rf.log = newLog
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = newLastIncludedTerm

	// rf.log[0] 现在就是新的哨兵条目，它的 Term 必须是 newLastIncludedTerm。
	// 因为 copy 操作已经把正确的条目（原来在 rf.log[sliceIndex]）复制到了 newLog[0]，
	// 所以这里的 Term 已经是正确的了。我们只需要确保它的 Command 为 nil。
	rf.log[0].Command = nil

	// 持久化
	rf.persist()
}

// --- 功能函数 ---

// getSliceIndex 将绝对日志索引转换为 rf.log 切片的索引
func (rf *Raft) getSliceIndex(absoluteIndex int) int {
	// 假设 absoluteIndex 总是 >= rf.lastIncludedIndex
	return absoluteIndex - rf.lastIncludedIndex
}

// getAbsoluteIndex 将 rf.log 切片的索引转换为绝对日志索引
func (rf *Raft) getAbsoluteIndex(sliceIndex int) int {
	return rf.lastIncludedIndex + sliceIndex
}

// getLastLogIndex 返回日志中最后一个条目的绝对索引
func (rf *Raft) getLastLogIndex() int {
	// len(rf.log) - 1 是最后一个元素的 slice index
	return rf.getAbsoluteIndex(len(rf.log) - 1)
}

// getLogTerm 返回指定绝对索引的日志条目的任期
func (rf *Raft) getLogTerm(absoluteIndex int) int {
	// 因为有哨兵，所以 absoluteIndex 不会小于 lastIncludedIndex
	return rf.log[rf.getSliceIndex(absoluteIndex)].Term
}

// 辅助函数，用于重置选举超时时间点。
// "Locked" 后缀表示此函数期望在调用时已经持有锁。
func (rf *Raft) resetElectionTimerLocked() {
	rf.electionDeadline = time.Now().Add(randomizedElectionTimeout())
}

// 生成一个随机的选举超时时间
func randomizedElectionTimeout() time.Duration {
	return electionTimeoutBase + time.Duration(rand.Intn(150))*time.Millisecond
}
