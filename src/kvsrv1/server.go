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

	// Your definitions here.
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
	// Your code here.
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

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
