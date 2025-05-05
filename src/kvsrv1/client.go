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
	// You may add code here.
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

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC with code like this:
// ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
	return rpc.ErrNoKey
}
