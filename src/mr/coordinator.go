package mr // 定义 mr 包，包含 MapReduce 协调器的实现

// 导入所需的包
import "log"      // 导入 log 包，用于记录日志和致命错误
import "net"      // 导入 net 包，用于网络连接，这里用于创建监听器
import "os"       // 导入 os 包，用于访问操作系统功能，这里用于删除旧的 socket 文件
import "net/rpc"  // 导入 net/rpc 包，用于 Go 语言的 RPC (远程过程调用)
import "net/http" // 导入 net/http 包，rpc 包使用 http 来处理 RPC 请求

import "sync"
import "time" // 需要用到时间来处理超时

// 定义任务状态的常量或类型
type TaskState int

const (
	Unstarted  TaskState = 0 // 任务尚未开始
	InProgress TaskState = 1 // 任务已被分配给 worker 正在进行
	Completed  TaskState = 2 // 任务已由 worker 成功完成
)

// 定义一个结构体来存储每个任务的信息
type Task struct {
	State     TaskState // 当前任务的状态
	InputFile string    // 输入文件路径 (仅用于 Map 任务)
	TaskID    int       // 任务的唯一 ID (可以是 Map/Reduce 的索引号)
	WorkerID  string    // 分配给哪个 worker (可选，用于调试或更精细的跟踪)
	StartTime time.Time // 任务开始时间 (用于超时检测)
}

// Coordinator 是协调器的主体结构体
// 它负责管理 MapReduce 任务的状态、分配任务给 worker 以及跟踪任务的完成情况
type Coordinator struct {
	// 在这里定义你的协调器需要维护的状态信息
	// 例如：
	// - 输入文件列表
	// - Map 任务的状态 (等待中、进行中、已完成)
	// - Reduce 任务的状态 (等待中、进行中、已完成)
	// - worker 的信息或状态
	// - 锁或其他同步机制来保护共享数据
	// - 存储中间结果的结构体 (尽管通常是 worker 直接写入文件)
	// Your definitions here.

	// 互斥锁：保护 Coordinator 结构体中的共享状态，防止并发访问导致数据竞争
	mu sync.Mutex

	// Map 任务状态列表：存储所有 Map 任务的详细信息
	// 列表的索引可以对应 Map 任务的 ID
	MapTasks []Task

	// Reduce 任务状态列表：存储所有 Reduce 任务的详细信息
	// 列表的索引可以对应 Reduce 任务的 ID (0 到 nReduce-1)
	ReduceTasks []Task

	// 总 Reduce 任务数：从 MakeCoordinator 获取
	NReduce int

	// 已完成的 Map 任务计数：方便快速判断 Map 阶段是否完成
	MapCompletedCount int

	// 已完成的 Reduce 任务计数：方便快速判断整个作业是否完成
	ReduceCompletedCount int

	// 输入文件列表：通常直接存储在 MapTasks 结构中，但也可以单独存储
	// InputFiles []string // 备选，如果 Task 结构中不存 InputFile

	// 标记map阶段是否完成
	MapPhaseCompleted bool

	// 标记整个 MapReduce 作业是否已完成
	JobCompleted bool
}

// Your code here -- RPC handlers for the worker to call.
// TODO: 在这里实现供 worker调用的 RPC 处理函数。
// 每个 RPC 处理函数都应该是一个 Coordinator 结构体的方法，
// 接收一个指向请求参数结构体的指针和一个指向回复结构体的指针，并返回一个 error。

// 实现coordinator的请求回复
func (c *Coordinator) HandleGetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 检查Job是否完成
	if c.JobCompleted {
		// 任务完成, 告知worker exit
		reply.Type = ExitTaskType
		return nil
	}

	// 如果 Map 阶段尚未完成
	if !c.MapPhaseCompleted {
		// 尝试寻找未开始或超时的 Map 任务
		for i := range c.MapTasks {
			if c.MapTasks[i].State == Unstarted ||
				(c.MapTasks[i].State == InProgress && time.Since(c.MapTasks[i].StartTime) > 10*time.Second) {
				// 找到了 Map 任务，分配给 Worker
				c.MapTasks[i].State = InProgress
				c.MapTasks[i].StartTime = time.Now()
				c.MapTasks[i].WorkerID = args.WorkerId

				reply.Type = MapTaskType
				reply.FileName = c.MapTasks[i].InputFile
				reply.MapTaskNumber = c.MapTasks[i].TaskID // Map TaskID 就是其在 MapTasks 列表中的索引
				reply.NReduce = c.NReduce
				return nil // 任务已分配，返回
			}
		}
		// 如果 Map 阶段未完成但没有可分配的 Map 任务 (所有 Map 任务都在 InProgress 且未超时)
		reply.Type = WaitTaskType
		return nil // 告知 Worker 等待
	}

	// 如果 Reduce 阶段尚未完成
	if c.ReduceCompletedCount < len(c.ReduceTasks) {
		for i := range c.ReduceTasks {
			if c.ReduceTasks[i].State == Unstarted ||
				(c.ReduceTasks[i].State == InProgress && time.Since(c.ReduceTasks[i].StartTime) > 10*time.Second) {
				// 找到了 Reduce 任务，分配给 Worker
				c.ReduceTasks[i].State = InProgress
				c.ReduceTasks[i].StartTime = time.Now()
				c.ReduceTasks[i].WorkerID = args.WorkerId

				reply.Type = ReduceTaskType
				reply.ReduceTaskNumber = c.ReduceTasks[i].TaskID // Reduce TaskID 就是其在 ReduceTasks 列表中的索引
				reply.NMap = len(c.MapTasks)                     // 可能 Worker 需要知道总 Map 任务数来查找中间文件
				return nil                                       // 任务已分配，返回
			}
		}

		// 如果 Reduce 阶段未完成但没有可分配的 Reduce 任务 (所有 Reduce 任务都在 InProgress 且未超时)
		reply.Type = WaitTaskType
		return nil // 告知 Worker 等待
	}

	// 如果 Map 阶段和 Reduce 阶段都已完成
	// 这意味着整个作业已完成 (尽管我们已经在函数开头检查了 JobCompleted)
	// 但这个地方是 c.MapPhaseCompleted 和 ReduceCompletedCount == c.NReduce 都满足时到达的，
	// 可以在这里设置 JobCompleted = true，并告知 Worker 退出)
	c.JobCompleted = true // 确保标志设置
	reply.Type = ExitTaskType
	return nil // 告知 Worker 退出
}

// TODO: HandleMapTaskComplete

// TODO: HandleReduceTaskComplete

// Example an example RPC handler.
// RPC 的请求参数和回复类型定义在 rpc.go 文件中。
//
// Example 是一个示例 RPC 方法，worker 可以调用此方法。
// 它接收 ExampleArgs 类型的参数，并填充 ExampleReply 类型的回复。
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	// 在这里实现 RPC 的逻辑
	// 示例：回复 Y 等于参数 X 加 1
	reply.Y = args.X + 1
	// 返回 nil 表示 RPC 调用成功
	return nil
}

// start a thread that listens for RPCs from worker.go
// 启动一个 goroutine (线程) 监听来自 worker.go 的 RPC 请求。
//
// server 方法启动 RPC 服务器
func (c *Coordinator) server() {
	// 向 RPC 系统注册协调器对象
	// 注册后，Coordinator 中所有符合 RPC 方法签名的公开方法都可以被远程调用
	rpc.Register(c)
	// 启用 HTTP 协议来处理 RPC 请求
	rpc.HandleHTTP()

	// 创建一个 UNIX 域套接字名称
	sockname := coordinatorSock()
	// 如果该套接字文件已存在，则删除它，以避免绑定错误
	os.Remove(sockname)

	// 监听指定的 UNIX 域套接字
	l, e := net.Listen("unix", sockname)
	// 检查监听是否发生错误
	if e != nil {
		// 如果发生错误，记录致命错误并退出
		log.Fatal("listen error:", e)
	}
	// 在新的 goroutine 中启动 HTTP 服务器，使用之前创建的监听器 l
	// 这使得 RPC 服务器可以在后台运行，不阻塞主线程
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
// main/mrcoordinator.go 会周期性地调用 Done() 函数来判断
// 整个 MapReduce 作业是否已经完成。
//
// Done 方法检查整个 MapReduce 作业是否已经完成
// 当所有 Map 任务和 Reduce 任务都完成后，此方法应返回 true
func (c *Coordinator) Done() bool {
	// ret 变量用于存储作业是否完成的状态，默认为 false
	ret := false

	// Your code here.
	// 在这里实现判断作业是否完成的逻辑。
	// 你需要根据协调器维护的任务状态来判断所有任务是否都已成功完成。

	// 返回作业完成状态
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
// // 创建一个协调器实例。
// // main/mrcoordinator.go 调用此函数。
// // nReduce 是要使用的 reduce 任务的数量。
//
// MakeCoordinator 函数是创建和初始化协调器的工厂函数
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// 创建一个 Coordinator 结构体实例
	c := Coordinator{}

	// 进行协调器的初始化工作。
	// 例如：
	// - 保存输入文件列表 (files)
	// - 保存 reduce 任务数量 (nReduce)
	// - 初始化 Map 和 Reduce 任务的状态
	// - 初始化 worker 的状态
	// - 可能需要启动定时器来检测 worker 是否超时等
	c.JobCompleted = false
	c.NReduce = nReduce
	c.MapCompletedCount = 0
	c.ReduceCompletedCount = 0
	c.MapPhaseCompleted = false
	c.MapTasks = make([]Task, 0, len(files))
	for i, file := range files {
		c.MapTasks = append(c.MapTasks, Task{
			InputFile: file,
			State:     Unstarted,
			TaskID:    i,
			StartTime: time.Now(),
		})
	}
	c.ReduceTasks = make([]Task, 0, nReduce)
	for i := 0; i < nReduce; i++ {
		c.ReduceTasks = append(c.ReduceTasks, Task{
			State:     Unstarted,
			TaskID:    i,
			StartTime: time.Now(),
		})
	}
	// 启动 RPC 服务器，开始监听 worker 的请求
	c.server()

	// 返回协调器实例的指针
	return &c
}
