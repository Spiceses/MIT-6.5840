package kvsrv

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/tester1"
	"log"
)

type Clerk struct {
	clnt   *tester.Clnt
	server string
}

func MakeClerk(clnt *tester.Clnt, server string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, server: server}
	return ck
}

// Get 获取一个键的当前值和版本。
// 如果键不存在，则返回 ErrNoKey。对于所有其他错误，它会一直尝试。
//
// 你可以使用类似这样的代码发送一个 RPC：
// ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)
//
// args 和 reply 的类型（包括它们是否是指针）
// 必须与 RPC 处理程序函数声明的参数类型匹配。
// 此外，reply 必须作为指针传递。
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	args := rpc.GetArgs{Key: key}
	reply := rpc.GetReply{}

	ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)
	if !ok {
		// 在具有可靠网络的键值服务器时, 通信失败为致命错误
		log.Fatal("call KVServer.Get fail")
	}

	return reply.Value, reply.Version, reply.Err
}

// Put 仅当请求中的版本号与服务器上键的版本号匹配时，才更新键的值。
// 如果版本号不匹配，服务器应返回 ErrVersion。
// 如果键不存在，当 args.Version 为 0 时，Put 安装该值，否则返回 ErrNoKey。
// 如果 Put 在其第一次 RPC 调用时收到 ErrVersion，则 Put 应返回 ErrVersion，因为该 Put 肯定未在服务器上执行。
// 如果服务器在一次重传的 RPC 调用时返回 ErrVersion，那么 Put 必须向应用程序返回 ErrMaybe，
// 因为其早先的 RPC 可能已经被服务器成功处理但回复丢失了，Clerk 不知道 Put 是否被执行了。
//
// 你可以使用类似这样的代码发送一个 RPC：
// ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)
//
// args 和 reply 的类型（包括它们是否是指针）
// 必须与 RPC 处理程序函数声明的参数类型匹配。
// 此外，reply 必须作为指针传递。
func (ck *Clerk) Put(key, value string, version rpc.Tversion) rpc.Err {
	args := rpc.PutArgs{Key: key, Value: value, Version: version}
	reply := rpc.PutReply{}

	ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)
	if !ok {
		log.Fatal("call KVServer.Put fail")
	}

	return reply.Err
}
