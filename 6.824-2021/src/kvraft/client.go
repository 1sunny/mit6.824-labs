package kvraft

import (
	"6.824/labrpc"
	"6.824/util"
	"fmt"
	"log"
	"os"
	"sync"
)
import "crypto/rand"
import "math/big"

const clerkDebug = 1

// caller should hold lock
func (ck *Clerk) debug(format string, a ...interface{}) {
	var logger = log.New(os.Stdout, fmt.Sprintf("%s Client[%d:%d:%d] ", util.GetTimeBuf(), ck.cid, ck.seq, ck.leaderId), 0)
	if clerkDebug > 0 {
		logger.Printf(format, a...)
	}
}

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu        sync.Mutex
	numServer int
	cid       int64
	seq       int64
	unSeq     int64 // 未收到响应的最小序号
	leaderId  int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.numServer = len(servers)
	ck.cid = nrand()
	ck.seq = 0
	ck.leaderId = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	ck.seq++
	for i := ck.leaderId; ; {
		args := GetArgs{Key: key, Cid: ck.cid, Seq: ck.seq}
		reply := GetReply{}
		ck.debug("向服务器发送 Get {%+v}", args)
		ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
		ck.debug("收到服务器: %+v", reply)
		if ok == false || reply.Err == ErrWrongLeader {
			i = (i + 1) % ck.numServer
			continue
		}
		ck.leaderId = i
		ck.debug("设置 LeaderId 为: [%d]", ck.leaderId)
		if reply.Err == OK || reply.Err == ErrNoKey {
			ck.debug("Get 返回 {%+v}", reply)
			return reply.Value
		}
		log.Fatalf("Unknown Err")
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.seq++
	for i := ck.leaderId; ; {
		args := PutAppendArgs{Key: key, Value: value, Op: op, Cid: ck.cid, Seq: ck.seq}
		reply := PutAppendReply{}
		ck.debug("向服务器发送 PutAppend {%+v}", args)
		ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
		ck.debug("收到服务器回复: %+v", reply)
		if ok == false || reply.Err == ErrWrongLeader {
			i = (i + 1) % ck.numServer
			continue
		}
		ck.leaderId = i
		ck.debug("设置 LeaderId 为: [%d]", ck.leaderId)
		if reply.Err == OK {
			ck.debug("PutAppend 返回 %+v", reply)
			return
		}
		log.Fatalf("Unknown Err")
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
