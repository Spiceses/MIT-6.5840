package main // 定义主包，表示这是一个可执行程序

//
// start a worker process, which is implemented
// in ../mr/worker.go. typically there will be
// multiple worker processes, talking to one coordinator.
//
// go run mrworker.go wc.so
//
// Please do not change this file.
//

// 导入项目所需的包
import "6.5840/mr" // 导入自定义的 mr 包，其中包含了 worker 的实现
import "plugin"    // 导入 plugin 包，用于动态加载 Go 插件 (.so 文件)
import "os"        // 导入 os 包，用于访问操作系统功能，这里用于获取命令行参数和退出程序
import "fmt"       // 导入 fmt 包，用于格式化输入输出，这里用于打印使用说明和错误信息
import "log"       // 导入 log 包，用于记录日志和致命错误

// main 函数是程序的入口点
func main() {
	// 检查命令行参数数量
	// os.Args[0] 是程序名本身，这里期望只有一个额外的参数作为插件文件名
	if len(os.Args) != 2 {
		// 如果参数数量不等于2，说明没有正确指定插件文件
		// 打印使用说明到标准错误
		fmt.Fprintf(os.Stderr, "Usage: mrworker xxx.so\n")
		// 退出程序，返回状态码 1 表示错误
		os.Exit(1)
	}

	// 调用 loadPlugin 函数加载 Map 和 Reduce 函数
	// os.Args[1] 是命令行传入的插件文件名
	mapf, reducef := loadPlugin(os.Args[1])

	// 调用 mr 包中的 Worker 函数启动一个 worker 进程
	// 将加载到的 mapf 和 reducef 函数传递给 worker
	mr.Worker(mapf, reducef)
}

// loadPlugin 函数用于从指定的插件文件 (.so) 中加载应用的 Map 和 Reduce 函数
// 例如：../mrapps/wc.so
func loadPlugin(filename string) (func(string, string) []mr.KeyValue, func(string, []string) string) {
	// 打开插件文件
	p, err := plugin.Open(filename)
	// 检查打开文件时是否发生错误
	if err != nil {
		// 如果发生错误，记录致命错误并退出
		log.Fatalf("cannot load plugin %v", filename)
	}

	// 在插件中查找名为 "Map" 的符号
	xmapf, err := p.Lookup("Map")
	// 检查是否找到 "Map" 符号
	if err != nil {
		// 如果未找到，记录致命错误并退出
		log.Fatalf("cannot find Map in %v", filename)
	}
	// 将找到的 "Map" 符号断言（类型转换）为预期的 Map 函数类型
	mapf := xmapf.(func(string, string) []mr.KeyValue)

	// 在插件中查找名为 "Reduce" 的符号
	xreducef, err := p.Lookup("Reduce")
	// 检查是否找到 "Reduce" 符号
	if err != nil {
		// 如果未找到，记录致命错误并退出
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	// 将找到的 "Reduce" 符号断言（类型转换）为预期的 Reduce 函数类型
	reducef := xreducef.(func(string, []string) string)

	// 返回加载到的 Map 和 Reduce 函数
	return mapf, reducef
}
