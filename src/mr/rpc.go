package mr // 定义 mr 包，这个包包含了 MapReduce 的实现细节

// RPC definitions.
// 记住所有名字都要大写开头，以便可以被 RPC 远程调用。
//

import "os"      // 导入 os 包，用于访问操作系统功能，这里用于获取用户 ID
import "strconv" // 导入 strconv 包，用于字符串和基本类型之间的转换，这里用于将用户 ID 转换为字符串

// 这是一个示例，展示如何为 RPC 声明请求参数 (arguments) 和回复 (reply)。

// ExampleArgs 是一个示例的 RPC 请求参数结构体
type ExampleArgs struct {
	X int // 请求参数 X，注意字段名大写，以便在 RPC 中可见
}

// ExampleReply 是一个示例的 RPC 回复结构体
type ExampleReply struct {
	Y int // 回复字段 Y，注意字段名大写，以便在 RPC 中可见
}

// Add your RPC definitions here.
// TODO: 在这里添加你自己的 RPC 定义。
// 根据 MapReduce 协调器和 worker 之间通信的需求，定义其他请求参数和回复结构体。
// 例如，可能有用于请求任务、报告任务完成、获取 Reduce 任务数量等的 RPC 定义。

// worker向coordinator请求任务时提供的参数
type GetTaskArgs struct {
	WorkerId string
}

type TaskType int

const (
	MapTaskType    TaskType = 0 // 任务尚未开始
	ReduceTaskType TaskType = 1 // 任务已被分配给 worker 正在进行
	WaitTaskType   TaskType = 2 // 任务已由 worker 成功完成
	ExitTaskType   TaskType = 3
)

// coordinator需要回复的信息
type GetTaskReply struct {
	// 任务类型：Map, Reduce, Wait (当前无可用任务), Exit (所有任务完成，Worker 可以退出了)
	// 使用枚举类型或者定义常量会比直接用字符串更健壮
	Type TaskType // Map, Reduce, Wait, Exit

	// --- Map 任务相关字段 (当 TaskType 为 Map 时有效) ---
	FileName      string // Map 任务需要处理的输入文件
	MapTaskNumber int    // 这个 Map 任务的编号 (例如，对应第 i 个输入文件)
	NReduce       int    // 总共有多少个 Reduce 任务 (Map Worker 需要这个信息来分区中间结果)

	// --- Reduce 任务相关字段 (当 TaskType 为 Reduce 时有效) ---
	ReduceTaskNumber int // 这个 Reduce 任务的编号 (例如，对应第 j 个 Reduce 桶)
	// Reduce Worker 需要知道所有相关的中间文件。
	// 一种方法是让 Worker 自己根据 ReduceTaskNumber 和总 Map 任务数去目录下找 mr-*-ReduceTaskNumber 的文件。
	// 另一种方法是 Coordinator 在这里直接提供相关中间文件的列表。
	// 前者更简单，后者 Coordinator 职责更重。考虑到实验的简单性，让 Worker 自己找可能更容易实现。
	// 所以这里可能只需要 ReduceTaskNumber。
	// 如果决定让 Worker 自己找，可能 Worker 还需要知道总共有多少 Map 任务来确认所有相关的中间文件都已生成。
	NMap int // 总共有多少个 Map 任务 (可能有助于 Reduce Worker 确认中间文件集)

	// --- Wait 任务相关字段 (当 TaskType 为 Wait 时有效) ---
	// 不需要额外信息

	// --- Exit 任务相关字段 (当 TaskType 为 Exit 时有效) ---
	// 不需要额外信息
}

// 生成一个相对唯一的 UNIX 域套接字名称
// 放在 /var/tmp 目录下，供coordinator使用。
// 不能使用当前目录，因为
// Athena AFS (一种分布式文件系统) 不支持 UNIX 域套接字。
func coordinatorSock() string {
	// 套接字名称的前缀
	s := "/var/tmp/5840-mr-"
	// 将当前用户 ID 转换为字符串并附加到套接字名称中，以确保唯一性
	s += strconv.Itoa(os.Getuid())
	// 返回完整的套接字名称
	return s
}
