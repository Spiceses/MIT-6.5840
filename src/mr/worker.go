package mr // 定义 mr 包，包含 MapReduce worker 的实现

// 导入所需的包
import "fmt"      // 导入 fmt 包，用于格式化输入输出
import "log"      // 导入 log 包，用于记录日志
import "net/rpc"  // 导入 net/rpc 包，用于 Go 语言的 RPC (远程过程调用)
import "hash/fnv" // 导入 hash/fnv 包，用于实现 FNV 哈希算法，这里用于对 key 进行哈希

// Map 函数返回一个 KeyValue 类型的slice。
// KeyValue 结构体表示 Map 任务输出的键值对。
type KeyValue struct {
	Key   string // 键
	Value string // 值
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
// 使用 ihash(key) % NReduce 来为 Map 输出的每个 KeyValue 选择 Reduce 任务编号。
// NReduce 是 Reduce 任务的总数量。
//
// ihash 函数计算给定 key 的哈希值，用于确定其应该分配到哪个 Reduce 任务。
func ihash(key string) int {
	h := fnv.New32a()    // 创建一个新的 FNV-1a 32 位哈希对象
	h.Write([]byte(key)) // 将 key 字符串的字节写入哈希对象
	// 计算哈希值的 32 位整数表示，并取绝对值（通过与 0x7fffffff 进行按位与操作，清除符号位）
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go 调用此函数来启动一个 worker 进程。
//
// Worker 函数是 MapReduce worker 的主执行逻辑。
// mapf 是应用提供的 Map 函数，reducef 是应用提供的 Reduce 函数。
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// TODO: 在这里实现你的 worker 逻辑。
	// 一个 worker 应该不断地向协调器请求任务（Map 或 Reduce），
	// 执行任务（调用 mapf 或 reducef），
	// 并向协调器报告任务完成或遇到的错误。
	for {
		// 向 Coordinator 请求任务
		args := GetTaskArgs{}
		reply := GetTaskReply{}
		ok := call("Coordinator.HandleGetTask", &args, &reply)
		if ok == false {
			// RPC error
			break
		}

		// 处理 Coordinator 的回复

	}

	// 取消注释此行可以向协调器发送一个示例 RPC 调用，用于测试通信。
	// CallExample()

}

// 这是一个示例函数，展示如何向协调器发起一个 RPC 调用。
// RPC 的请求参数和回复类型定义在 rpc.go 文件中。
// CallExample 函数向协调器的 Example RPC 方法发起调用。
func CallExample() {

	// declare an argument structure.
	// 声明一个请求参数结构体
	args := ExampleArgs{}

	// fill in the argument(s).
	// 填充请求参数
	args.X = 99

	// declare a reply structure.
	// 声明一个回复结构体
	reply := ExampleReply{}

	// 发送 RPC 请求，并等待回复。
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	// 检查 RPC 调用是否成功
	if ok {
		// reply.Y should be 100.
		// 如果成功，打印回复中的 Y 字段的值（期望是 100）
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		// 如果调用失败，打印错误信息
		fmt.Printf("call failed!\n")
	}
}

// 向协调器发送一个 RPC 请求，并等待响应。
// 通常返回 true 表示成功。
// 如果发生错误则返回 false。
//
// call 函数是一个通用的助手函数，用于向协调器发起 RPC 调用。
// rpcname 是要调用的 RPC 方法名称（例如 "Coordinator.Example"）。
// args 是指向请求参数结构体的指针。
// reply 是指向用于接收回复的结构体的指针。
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234") // 示例：通过 TCP 连接
	// 获取协调器使用的 UNIX 域套接字名称
	sockname := coordinatorSock()
	// 通过 HTTP 在指定的 UNIX 域套接字上连接协调器
	c, err := rpc.DialHTTP("unix", sockname)
	// 检查连接是否发生错误
	if err != nil {
		// 如果发生错误，记录致命错误并退出
		log.Fatal("dialing:", err)
	}
	// 确保在函数返回前关闭连接
	defer c.Close()

	// 发起 RPC 调用，rpcname 指定方法，args 为参数，reply 为接收回复的结构体
	err = c.Call(rpcname, args, reply)
	// 检查 RPC 调用本身是否发生错误
	if err == nil {
		// 如果没有错误，返回 true 表示成功
		return true
	}

	// 如果调用发生错误，打印错误信息
	fmt.Println(err)
	// 返回 false 表示失败
	return false
}
