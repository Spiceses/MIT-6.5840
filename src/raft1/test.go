package raft

import (
	"fmt"
	//log
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type Test struct {
	*tester.Config
	t *testing.T
	n int
	g *tester.ServerGrp

	finished int32

	mu       sync.Mutex
	srvs     []*rfsrv
	maxIndex int
	snapshot bool
}

func makeTest(t *testing.T, n int, reliable bool, snapshot bool) *Test {
	ts := &Test{
		t:        t,
		n:        n,
		srvs:     make([]*rfsrv, n),
		snapshot: snapshot,
	}
	ts.Config = tester.MakeConfig(t, n, reliable, ts.mksrv)
	ts.Config.SetLongDelays(true)
	ts.g = ts.Group(tester.GRP0)
	return ts
}

func (ts *Test) cleanup() {
	atomic.StoreInt32(&ts.finished, 1)
	ts.End()
	ts.Config.Cleanup()
	ts.CheckTimeout()
}

func (ts *Test) mksrv(ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	s := newRfsrv(ts, srv, ends, persister, ts.snapshot)
	ts.mu.Lock()
	ts.srvs[srv] = s
	ts.mu.Unlock()
	return []tester.IService{s, s.raft}
}

func (ts *Test) restart(i int) {
	ts.g.StartServer(i) // which will call mksrv to make a new server
	ts.Group(tester.GRP0).ConnectAll()
}

func (ts *Test) checkOneLeader() int {
	tester.AnnotateCheckerBegin("checking for a single leader")
	for iters := 0; iters < 10; iters++ {
		ms := 450 + (rand.Int63() % 100)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		leaders := make(map[int][]int)
		for i := 0; i < ts.n; i++ {
			if ts.g.IsConnected(i) {
				if term, leader := ts.srvs[i].GetState(); leader {
					leaders[term] = append(leaders[term], i)
				}
			}
		}

		lastTermWithLeader := -1
		for term, leaders := range leaders {
			if len(leaders) > 1 {
				details := fmt.Sprintf("multiple leaders in term %v = %v", term, leaders)
				tester.AnnotateCheckerFailure("multiple leaders", details)
				ts.Fatalf("term %d has %d (>1) leaders", term, len(leaders))
			}
			if term > lastTermWithLeader {
				lastTermWithLeader = term
			}
		}

		if len(leaders) != 0 {
			details := fmt.Sprintf("leader in term %v = %v",
				lastTermWithLeader, leaders[lastTermWithLeader][0])
			tester.AnnotateCheckerSuccess(details, details)
			return leaders[lastTermWithLeader][0]
		}
	}
	details := fmt.Sprintf("unable to find a leader")
	tester.AnnotateCheckerFailure("no leader", details)
	ts.Fatalf("expected one leader, got none")
	return -1
}

func (ts *Test) checkTerms() int {
	tester.AnnotateCheckerBegin("checking term agreement")
	term := -1
	for i := 0; i < ts.n; i++ {
		if ts.g.IsConnected(i) {
			xterm, _ := ts.srvs[i].GetState()
			if term == -1 {
				term = xterm
			} else if term != xterm {
				details := fmt.Sprintf("node ids -> terms = { %v -> %v; %v -> %v }",
					i-1, term, i, xterm)
				tester.AnnotateCheckerFailure("term disagreed", details)
				ts.Fatalf("servers disagree on term")
			}
		}
	}
	details := fmt.Sprintf("term = %v", term)
	tester.AnnotateCheckerSuccess("term agreed", details)
	return term
}

func (ts *Test) checkLogs(i int, m raftapi.ApplyMsg) (string, bool) {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	err_msg := ""
	v := m.Command
	me := ts.srvs[i]
	for j, rs := range ts.srvs {
		if old, oldok := rs.Logs(m.CommandIndex); oldok && old != v {
			//log.Printf("%v: log %v; server %v\n", i, me.logs, rs.logs)
			// some server has already committed a different value for this entry!
			err_msg = fmt.Sprintf("commit index=%v server=%v %v != server=%v %v",
				m.CommandIndex, i, m.Command, j, old)
		}
	}
	_, prevok := me.logs[m.CommandIndex-1]
	me.logs[m.CommandIndex] = v
	if m.CommandIndex > ts.maxIndex {
		ts.maxIndex = m.CommandIndex
	}
	return err_msg, prevok
}

// check that none of the connected servers
// thinks it is the leader.
func (ts *Test) checkNoLeader() {
	tester.AnnotateCheckerBegin("checking no unexpected leader among connected servers")
	for i := 0; i < ts.n; i++ {
		if ts.g.IsConnected(i) {
			_, is_leader := ts.srvs[i].GetState()
			if is_leader {
				details := fmt.Sprintf("leader = %v", i)
				tester.AnnotateCheckerFailure("unexpected leader found", details)
				ts.Fatalf(details)
			}
		}
	}
	tester.AnnotateCheckerSuccess("no unexpected leader", "no unexpected leader")
}

func (ts *Test) checkNoAgreement(index int) {
	text := fmt.Sprintf("checking no unexpected agreement at index %v", index)
	tester.AnnotateCheckerBegin(text)
	n, _ := ts.nCommitted(index)
	if n > 0 {
		desp := fmt.Sprintf("unexpected agreement at index %v", index)
		details := fmt.Sprintf("%v server(s) commit incorrectly index", n)
		tester.AnnotateCheckerFailure(desp, details)
		ts.Fatalf("%v committed but no majority", n)
	}
	desp := fmt.Sprintf("no unexpected agreement at index %v", index)
	tester.AnnotateCheckerSuccess(desp, "OK")
}

// how many servers think a log entry is committed?
// nCommitted 函数检查在给定的日志索引 (index) 上，有多少个服务器认为该条目已经提交，
// 并返回该条目的内容。
// 它同时会验证所有声称已提交该条目的服务器，它们在该索引上的日志内容必须完全一致。
// 如果发现不一致，测试将立即失败。
//
// 返回值:
// - int: 认为在 `index` 处的日志已经提交的服务器数量。
// - any: 在 `index` 处提交的命令内容。如果所有服务器的命令不一致，或没有服务器提交该日志，则可能为 nil 或其中一个值。
func (ts *Test) nCommitted(index int) (int, any) {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	count := 0
	var cmd any = nil
	for _, rs := range ts.srvs {
		if rs.applyErr != "" {
			tester.AnnotateCheckerFailure("apply error", rs.applyErr)
			ts.t.Fatal(rs.applyErr)
		}

		cmd1, ok := rs.Logs(index)

		if ok {
			if count > 0 && cmd != cmd1 {
				text := fmt.Sprintf("committed values at index %v do not match (%v != %v)",
					index, cmd, cmd1)
				tester.AnnotateCheckerFailure("unmatched committed values", text)
				ts.Fatalf(text)
			}
			count += 1
			cmd = cmd1
		}
	}
	return count, cmd
}

// do a complete agreement.
// it might choose the wrong leader initially,
// and have to re-submit after giving up.
// entirely gives up after about 10 seconds.
// indirectly checks that the servers agree on the
// same value, since nCommitted() checks this,
// as do the threads that read from applyCh.
// returns index.
// if retry==true, may submit the command multiple
// times, in case a leader fails just after Start().
// if retry==false, calls Start() only once, in order
// to simplify the early Lab 3B tests.
//
// one 函数执行一个完整的共识流程。
// 它会尝试向一个 Raft 集群提交一个命令（cmd），并等待该命令在至少 expectedServers 个服务器上达成共识。
//
// 详细说明：
//   - 它最初可能会选错领导者（leader），因为它是通过轮询所有服务器来找到领导者的。
//   - 如果选错了领导者或者领导者在提交后崩溃，它会根据 retry 参数决定是否重新提交。
//   - 整个过程有大约 10 秒的超时时间，超时则测试失败。
//   - 它通过调用 ts.nCommitted() 来间接检查服务器们是否就同一个值（我们提交的 cmd）达成了一致。
//     nCommitted() 会检查有多少服务器在某个 index 上提交了日志，以及该日志的内容是什么。
//     从 applyCh 读取数据的后台线程也服务于这个检查逻辑。
//   - 成功达成共识后，函数会返回该命令对应的日志索引（index）。
//
// 参数：
// - cmd: 要提交的命令。
// - expectedServers: 期望达成共识的最小服务器数量。
// - retry:
//   - 如果为 true，在一次提交后若未在短时间内达成共识，函数会重新尝试寻找领导者并再次提交命令。这用于模拟领导者恰好在 Start() 调用后失败的场景。
//   - 如果为 false，则只会调用一次 Start()。如果此次提交失败，整个函数就会失败。这用于简化早期的 Lab 3B 测试，因为这些测试不要求处理领导者变更。
func (ts *Test) one(cmd any, expectedServers int, retry bool) int {
	// --- 初始化和准备 ---

	// 根据 retry 参数设置描述文本，用于日志输出。
	var textretry string
	if retry {
		textretry = "with"
	} else {
		textretry = "without"
	}
	// 将命令转换为字符串，用于日志输出，最多显示8个字符。
	textcmd := fmt.Sprintf("%v", cmd)
	// 构建一个描述本次测试操作的字符串。
	textb := fmt.Sprintf("checking agreement of %.8s by at least %v servers %v retry",
		textcmd, expectedServers, textretry)
	// 标记检查器开始执行，用于测试框架的日志记录。
	tester.AnnotateCheckerBegin(textb)
	t0 := time.Now() // 记录整个函数的开始时间，用于10秒超时控制。
	starts := 0      // 记录尝试调用 Start() 的服务器的起始索引，用于轮询。

	// --- 主循环：寻找领导者并提交命令 ---

	// 循环最多执行10秒，或者直到测试完成（ts.checkFinished() 返回 true）。
	for time.Since(t0).Seconds() < 10 && ts.checkFinished() == false {
		// try all the servers, maybe one is the leader.
		index := -1

		// 尝试遍历所有的服务器，寄希望于其中一个是领导者。
		for range ts.srvs {
			// 计算下一个要尝试的服务器的索引，实现循环遍历。
			starts = (starts + 1) % len(ts.srvs)
			var rf raftapi.Raft // 声明一个 Raft 接口变量。
			// 检查服务器是否处于连接状态。
			if ts.g.IsConnected(starts) {
				// 如果已连接，锁定该服务器以安全地访问其内部的 Raft 实例。
				ts.srvs[starts].mu.Lock()
				rf = ts.srvs[starts].raft
				ts.srvs[starts].mu.Unlock()
			}

			// 如果成功获取了 Raft 实例（即服务器是连接的且实例存在）。
			if rf != nil {
				// 调用 Start() 方法尝试提交命令。
				// rf.Start() 会返回（日志索引，任期号，是否是领导者）。
				//log.Printf("peer %d Start %v", starts, cmd)
				index1, _, ok := rf.Start(cmd)
				if ok {
					// 如果 ok 为 true，说明这个服务器认为自己是领导者，并接受了命令。
					index = index1 // 记录下返回的日志索引。
					break          // 成功提交，跳出内层 for 循环，不再尝试其他服务器。
				}
			}
		}

		// --- 检查共识阶段 ---
		if index != -1 {
			// 如果 index 不为 -1，说明有一个服务器声称是领导者并接受了我们的命令。
			// 现在我们需要等待一段时间，看集群是否能就此命令达成共识。
			t1 := time.Now() // 记录开始等待共识的时间。
			// 在接下来的 2 秒内，持续检查共识是否达成。
			for time.Since(t1).Seconds() < 2 {
				// 调用 nCommitted 检查在 index 位置有多少服务器提交了日志，以及提交的命令是什么。
				nd, cmd1 := ts.nCommitted(index)
				// 如果提交的服务器数量 (nd) 大于0 且达到了期望的数量 (expectedServers)。
				if nd > 0 && nd >= expectedServers {
					// committed
					if cmd1 == cmd {
						// and it was the command we submitted.
						desp := fmt.Sprintf("agreement of %.8s reached", textcmd)
						tester.AnnotateCheckerSuccess(desp, "OK")
						return index
					}
				}
				time.Sleep(20 * time.Millisecond)
			}
			if retry == false {
				desp := fmt.Sprintf("agreement of %.8s failed", textcmd)
				tester.AnnotateCheckerFailure(desp, "failed after submitting command")
				ts.Fatalf("one(%v) failed to reach agreement", cmd)
			}
		} else {
			time.Sleep(50 * time.Millisecond)
		}
	}
	if ts.checkFinished() == false {
		desp := fmt.Sprintf("agreement of %.8s failed", textcmd)
		tester.AnnotateCheckerFailure(desp, "failed after 10-second timeout")
		ts.Fatalf("one(%v) failed to reach agreement", cmd)
	}
	return -1
}

func (ts *Test) checkFinished() bool {
	z := atomic.LoadInt32(&ts.finished)
	return z != 0
}

// wait for at least n servers to commit.
// but don't wait forever.
func (ts *Test) wait(index int, n int, startTerm int) any {
	to := 10 * time.Millisecond
	for iters := 0; iters < 30; iters++ {
		nd, _ := ts.nCommitted(index)
		if nd >= n {
			break
		}
		time.Sleep(to)
		if to < time.Second {
			to *= 2
		}
		if startTerm > -1 {
			for _, rs := range ts.srvs {
				if t, _ := rs.raft.GetState(); t > startTerm {
					// someone has moved on
					// can no longer guarantee that we'll "win"
					return -1
				}
			}
		}
	}
	nd, cmd := ts.nCommitted(index)
	if nd < n {
		desp := fmt.Sprintf("less than %v servers commit index %v", n, index)
		details := fmt.Sprintf(
			"only %v (< %v) servers commit index %v at term %v", nd, n, index, startTerm)
		tester.AnnotateCheckerFailure(desp, details)
		ts.Fatalf("only %d decided for index %d; wanted %d",
			nd, index, n)
	}
	return cmd
}
