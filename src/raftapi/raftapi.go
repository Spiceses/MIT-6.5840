package raftapi

// The Raft interface
type Raft interface {
	// Start agreement on a new log entry, and return the log index
	// for that entry, the term, and whether the peer is the leader.
	Start(command interface{}) (int, int, bool)

	// Ask a Raft for its current term, and whether it thinks it is
	// leader
	GetState() (int, bool)

	// For Snaphots (3D)
	Snapshot(index int, snapshot []byte)
	PersistBytes() int

	// For the tester to indicate to your code that is should cleanup
	// any long-running go routines.
	Kill()
}

// As each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the server (or
// tester), via the applyCh passed to Make(). Set CommandValid to true
// to indicate that the ApplyMsg contains a newly committed log entry.
//
// In Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
// 当每个 Raft 对等节点（peer）得知日志条目（log entries）被相继提交（committed）后，
// 该节点应通过传递给 Make() 函数的 applyCh 通道（channel），
// 向服务器（或测试程序）发送一个 ApplyMsg 消息。
// 将 CommandValid 设置为 true，以表明该 ApplyMsg 包含一条新提交的日志条目。
//
// 在实验 3 中，你会希望在 applyCh 上发送其他类型的消息（例如，快照）。
// 到那时，你可以向 ApplyMsg 添加新的字段，但对于这些其他用途，请将 CommandValid 设置为 false。
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}
