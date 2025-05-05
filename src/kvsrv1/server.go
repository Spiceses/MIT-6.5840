package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex

	kvStore map[string]struct {
		Value   string
		Version rpc.Tversion
	}
}

func MakeKVServer() *KVServer {
	kv := &KVServer{
		kvStore: map[string]struct {
			Value   string
			Version rpc.Tversion
		}{},
	}
	return kv
}

// Get 返回 args.Key 对应的值和版本（如果 args.Key 存在）。
// 否则，Get 返回 ErrNoKey。
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	data, ok := kv.kvStore[args.Key]
	if ok {
		reply.Value = data.Value
		reply.Version = data.Version
		reply.Err = rpc.OK
	} else {
		reply.Value = ""
		reply.Version = 0
		reply.Err = rpc.ErrNoKey
	}
}

// Put 仅当请求中的版本号（args.Version）与服务器上该键的当前版本号匹配时，才更新键的值。
// 如果版本号不匹配，服务器应返回 ErrVersion。
// 如果键不存在，当 args.Version 为 0 时，Put 会创建并安装该键和值，否则返回 ErrNoKey。
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	data, ok := kv.kvStore[args.Key]
	if ok {
		if data.Version == args.Version {
			data.Value = args.Value
			data.Version = args.Version + 1
			kv.kvStore[args.Key] = data
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrVersion
		}
	} else {
		if args.Version == rpc.Tversion(0) {
			data.Value = args.Value
			data.Version = rpc.Tversion(1)
			kv.kvStore[args.Key] = data
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrNoKey
		}
	}
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
