package main // 定义主包，表示这是一个可执行程序

//
// start the coordinator process, which is implemented
// in ../mr/coordinator.go
//
// go run mrcoordinator.go pg*.txt
//
// Please do not change this file.
//

// 导入项目所需的包
import "6.5840/mr" // 导入自定义的 mr 包，其中包含了协调器 (coordinator) 的实现
import "time"      // 导入 time 包，用于处理时间相关的操作，这里用于让主进程休眠
import "os"        // 导入 os 包，用于访问操作系统功能，这里用于获取命令行参数和退出程序
import "fmt"       // 导入 fmt 包，用于格式化输入输出，这里用于打印使用说明和错误信息

// main 函数是程序的入口点
func main() {
	// 检查命令行参数数量
	// os.Args[0] 是程序名本身，所以至少需要一个额外的参数作为输入文件
	if len(os.Args) < 2 {
		// 如果参数少于2个，说明没有指定输入文件
		// 打印使用说明到标准错误
		fmt.Fprintf(os.Stderr, "Usage: mrcoordinator inputfiles...\n")
		// 退出程序，返回状态码 1 表示错误
		os.Exit(1)
	}

	// 调用 mr 包中的 MakeCoordinator 函数创建协调器实例
	// os.Args[1:] 传递除了程序名之外的所有命令行参数作为输入文件列表
	// 10 可能表示 reducer 的数量，具体取决于 MakeCoordinator 的实现
	m := mr.MakeCoordinator(os.Args[1:], 10)

	// 循环等待协调器完成所有 MapReduce 任务
	// m.Done() 返回 true 表示任务已完成
	for m.Done() == false {
		// 如果任务未完成，则休眠一秒钟，避免过度占用 CPU
		time.Sleep(time.Second)
	}

	// 在所有任务完成后，再休眠一秒钟
	// 这可能是为了确保所有工作进程都已退出或进行清理
	time.Sleep(time.Second)
}
