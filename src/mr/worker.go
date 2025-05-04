package mr // 定义 mr 包，包含 MapReduce worker 的实现

// 导入所需的包
import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"time"
)                 // 导入 fmt 包，用于格式化输入输出
import "log"      // 导入 log 包，用于记录日志
import "net/rpc"  // 导入 net/rpc 包，用于 Go 语言的 RPC (远程过程调用)
import "hash/fnv" // 导入 hash/fnv 包，用于实现 FNV 哈希算法，这里用于对 key 进行哈希

const IntermediateFileNamePattern = "mr-%d-%d"

var workerId string // Declare package-level variable

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

// WorkerWaitInterval 定义 Worker 等待任务时的间隔常量
const WorkerWaitInterval = 100 * time.Millisecond

// main/mrworker.go 调用此函数来启动一个 worker 进程。
//
// Worker 函数是 MapReduce worker 的主执行逻辑。
// mapf 是应用提供的 Map 函数，reducef 是应用提供的 Reduce 函数。
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	workerId = strconv.Itoa(os.Getpid())

	// 一个 worker 应该不断地向协调器请求任务（Map 或 Reduce），
	// 执行任务（调用 mapf 或 reducef），
	// 并向协调器报告任务完成或遇到的错误。
	for {
		// 向 Coordinator 请求任务
		args := GetTaskArgs{
			WorkerId: workerId,
		}
		reply := GetTaskReply{}
		ok := call("Coordinator.HandleGetTask", &args, &reply)
		if ok == false {
			// RPC error - unable to get a task from the Coordinator
			// This is a fatal error for the worker, as it cannot proceed without a task.
			log.Fatalf("Worker %s: RPC call to Coordinator.HandleGetTask failed", args.WorkerId)
		}

		// 处理 Coordinator 的回复
		switch reply.Type {
		case MapTaskType:
			// 执行map任务
			performMapTask(mapf, reply.FileName, reply.MapTaskNumber, reply.NReduce)
			reportMapCompletion(reply.MapTaskNumber)
		case ReduceTaskType:
			// 执行reduce任务
			performReduceTask(reducef, reply.ReduceTaskNumber, reply.NMap)
			reportReduceCompletion(reply.ReduceTaskNumber)
		case WaitTaskType:
			time.Sleep(WorkerWaitInterval)
		case ExitTaskType:
			return
		}
	}

	// 取消注释此行可以向协调器发送一个示例 RPC 调用，用于测试通信。
	//CallExample()

}

func performMapTask(mapf func(string, string) []KeyValue, filename string, mapTaskNumber int, nReduce int) {
	// 读取文件内容
	content, err := os.ReadFile(filename)
	if err != nil {
		log.Fatalf("无法读取文件 %s: %v", filename, err) // 处理文件读取错误
	}

	// 应用map操作
	kva := mapf(filename, string(content))

	// 按 Reduce 桶分区 KeyValue 对
	// 创建一个临时的缓冲区，用于存放分配到每个 Reduce 桶的 KeyValue 对
	// 我们可以使用一个 []([]mr.KeyValue) 或一个 map[int][]mr.KeyValue
	// []([]mr.KeyValue) 更直接，大小固定为 nReduce
	intermediate := make([][]KeyValue, nReduce)

	for _, kv := range kva {
		// 计算这个 KeyValue 对应该去哪个 Reduce 桶
		reduceTaskNumber := ihash(kv.Key) % nReduce

		// 将 KeyValue 对添加到对应桶的缓冲区
		intermediate[reduceTaskNumber] = append(intermediate[reduceTaskNumber], kv)
	}

	// 4. 将每个桶的 KeyValue 对写入对应的中间文件
	for r := 0; r < nReduce; r++ {
		// 确定最终的中间文件名: mr-MapTaskNumber-ReduceTaskNumber
		finalFileName := fmt.Sprintf(IntermediateFileNamePattern, mapTaskNumber, r)

		// 使用临时文件写入 (原子性保证)
		// 可以构建一个临时文件名的前缀，例如 "mr-MapTaskNumber-ReduceTaskNumber-temp-"
		tempFilePattern := fmt.Sprintf(IntermediateFileNamePattern+"-temp-", mapTaskNumber, r)
		tempFile, err := os.CreateTemp(".", tempFilePattern) // 在系统默认临时目录创建
		if err != nil {
			log.Fatalf("无法创建临时中间文件 %s: %v", tempFilePattern, err)
		}

		// 使用 JSON 编码器写入 KeyValue 对
		encoder := json.NewEncoder(tempFile)
		err = encoder.Encode(intermediate[r]) // 编码整个 []KeyValue 切片
		if err != nil {
			// 写入失败，清理临时文件并报告错误
			tempFile.Close()
			os.Remove(tempFile.Name()) // 尝试删除未完成的临时文件
			log.Fatalf("无法编码并写入中间文件 %s: %v", tempFile.Name(), err)
		}

		// 关闭临时文件 (重要，确保数据被刷新到磁盘)
		err = tempFile.Close()
		if err != nil {
			// 关闭失败，也报告错误
			log.Fatalf("无法关闭临时中间文件 %s: %v", tempFile.Name(), err)
		}

		// 原子重命名临时文件为最终文件名
		err = os.Rename(tempFile.Name(), finalFileName)
		if err != nil {
			// 重命名失败，报告错误
			os.Remove(tempFile.Name()) // 尝试清理
			log.Fatalf("无法重命名临时文件 %s 到 %s: %v", tempFile.Name(), finalFileName, err)
		}

		// log.Printf("Map 任务 %d 成功写入中间文件 %s\n", mapTaskNumber, finalFileName) // 调试日志
	}
}

func performReduceTask(reducef func(string, []string) string, reduceTaskNumber int, nMap int) {
	// 读取中间文件内容, 第i个map任务生成中间文件mr-i-*, 因此我们要读取所有的mr-*-reduceTaskNumber文件
	// 存储当前编号的reduce桶的所有键值对
	var intermediate []KeyValue
	for i := 0; i < nMap; i++ {
		// 每个中间文件都包含一系列的键值对
		mapFileName := fmt.Sprintf(IntermediateFileNamePattern, i, reduceTaskNumber)

		// 读取json文件内容, 获得键值对
		file, err := os.Open(mapFileName)
		if err != nil {
			log.Fatalf("错误：打开文件 '%s' 失败：%v\n", mapFileName, err)
		}
		defer file.Close()

		var data []KeyValue
		decoder := json.NewDecoder(file)
		err = decoder.Decode(&data)
		if err != nil {
			log.Fatalf("无法解码中间文件 %s: %v", file.Name(), err)
		}

		intermediate = append(intermediate, data...)
	}

	// 升序排序intermediate
	sort.Slice(intermediate, func(i, j int) bool {
		return intermediate[i].Key < intermediate[j].Key
	})

	// 创建临时文件, 指定最终生成的文件名
	finalFileName := fmt.Sprintf("mr-out-%d", reduceTaskNumber)
	tempFilePattern := fmt.Sprintf("mr-out-%d-temp-", reduceTaskNumber)
	tempFile, err := os.CreateTemp(".", tempFilePattern)
	if err != nil {
		// 如果连临时文件都创建不了，这个 Reduce 任务也无法继续
		log.Fatalf("ReduceTask %d: 无法创建临时输出文件 %s: %v", reduceTaskNumber, tempFilePattern, err)
	}
	defer tempFile.Close()

	// 遍历排序后的中间数据，按键分组值。
	// 对于每个不同的键，调用 Reduce 函数，传入键和所有关联的值。
	// 将 Reduce 函数的输出（归约后的值）连同键一起写入一个名为 "mr-out-0" 的输出文件。
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		_, err = fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)
		if err != nil {
			// 写入临时文件失败是致命错误
			log.Fatalf("ReduceTask %d: 写入临时输出文件 %s 失败: %v", reduceTaskNumber, tempFile.Name(), err)
			// 在这里 log.Fatalf 会导致程序退出，所以不需要额外的清理代码
		}

		i = j
	}

	// 关闭临时文件 (重要，确保数据被刷新到磁盘)
	err = tempFile.Close()
	if err != nil {
		// 关闭失败，也报告错误
		log.Fatalf("无法关闭临时中间文件 %s: %v", tempFile.Name(), err)
	}

	// 原子重命名临时文件为最终文件名
	err = os.Rename(tempFile.Name(), finalFileName)
	if err != nil {
		// 重命名失败，报告错误
		os.Remove(tempFile.Name()) // 尝试清理
		log.Fatalf("无法重命名临时文件 %s 到 %s: %v", tempFile.Name(), finalFileName, err)
	}
}

func reportMapCompletion(mapTaskNumber int) {
	args := MapTaskCompleteArgs{
		MapTaskNumber: mapTaskNumber,
	}
	reply := MapTaskCompleteReply{}

	ok := call("Coordinator.HandleMapTaskComplete", &args, &reply)
	if !ok {
		// RPC error - unable to get a task from the Coordinator
		// This is a fatal error for the worker, as it cannot proceed without a task.
		log.Fatalf("Worker %s: RPC call to Coordinator.HandleMapTaskComplete failed", workerId)
	}
}

func reportReduceCompletion(reduceTaskNumber int) {
	args := ReduceTaskCompleteArgs{
		ReduceTaskNumber: reduceTaskNumber,
	}
	reply := ReduceTaskCompleteReply{}

	ok := call("Coordinator.HandleReduceTaskComplete", &args, &reply)
	if !ok {
		// RPC error - unable to get a task from the Coordinator
		// This is a fatal error for the worker, as it cannot proceed without a task.
		log.Fatalf("Worker %s: RPC call to Coordinator.HandleReduceTaskComplete failed", workerId)
	}
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
