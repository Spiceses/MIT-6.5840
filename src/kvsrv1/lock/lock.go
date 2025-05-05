package lock

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"log"
	"time"
)

const IDLength = 8
const EmptyID = ""
const AcquireRetryDelay = 50 * time.Millisecond

type Lock struct {
	// IKVClerk 是一个用于键/值 Clerk 的 Go 接口：
	// 该接口隐藏了 ck 的具体 Clerk 类型，但保证 ck 支持 Put 和 Get 方法。
	// 测试器在调用MakeLock()时会传入该 clerk
	ck kvtest.IKVClerk
	// You may add code here
	key string
	id  string

	// AF(ck, key, id) = 一个通过ck访问锁状态的锁, 锁的唯一标识符为id, 锁状态的键为key
}

// 测试者调用 MakeLock() 并传入一个 k/v clerk；你的代码可以通过调用 lk.ck.Put() 或 lk.ck.Get() 来执行 Put 或 Get 操作。
//
// 使用 l 作为键来存储“锁状态”（你需要精确地决定锁状态是什么）。
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{
		ck:  ck,
		key: l,
		id:  kvtest.RandValue(IDLength),
	}
	return lk
}

func (lk *Lock) Acquire() {
	for {
		id, version, err := lk.ck.Get(lk.key)

		// 检查锁是否空闲
		if version == 0 || id == EmptyID {
			// 没有id获取key上的锁, 尝试获取
			err = lk.ck.Put(lk.key, lk.id, version)

			// 检查是否成功获取锁
			if err != rpc.OK {
				// 获取失败, 可能有其它id并发地获取key上的锁, 等待一会再尝试获取
				time.Sleep(AcquireRetryDelay)
				continue
			}

			// 获取成功
			break
		}

		// 检查Clerk.Get调用是否完成
		if err != rpc.OK {
			log.Fatal("Clerk.Get fail")
		}

		// 检查锁是否被占有
		if id != lk.id {
			time.Sleep(AcquireRetryDelay)
			continue
		}

		break
	}
}

func (lk *Lock) Release() {
	id, version, err := lk.ck.Get(lk.key)

	//  检查锁是否空闲或者被其它人占有
	if version == 0 || id == EmptyID || lk.id != id {
		log.Fatal("尝试释放未持有的锁")
	}

	// 检查Clerk.Get调用是否完成
	if err != rpc.OK {
		log.Fatal("Clerk.Get fail")
	}

	// 尝试释放锁
	err = lk.ck.Put(lk.key, EmptyID, version)
	// 可能出现的情况: 第一次Put的回复丢失, 重新发送Put请求, 导致版本号对应不上, 这时Put会发送rpc.ErrMaybe, 此时这个错误可以忽略
	if err == rpc.ErrMaybe {
		return
	}
	// 检查释放锁是否失败
	if err != rpc.OK {
		log.Fatal("释放锁失败")
	}
}
