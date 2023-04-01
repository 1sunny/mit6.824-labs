package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"../util"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
)

const (
	GET    = "Get"
	PUT    = "Put"
	APPEND = "Append"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type  string
	Key   string
	Value string
	Cid   int64
	Seq   int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	data     map[string]string
	leaderId int
	session  map[int64][]Reply
}

const serverDebug = 1

// caller should hold lock
func (kv *KVServer) debug(format string, a ...interface{}) {
	var logger = log.New(os.Stdout, fmt.Sprintf("%s Server[%d:%d] ", util.GetTimeBuf(), kv.me, kv.leaderId), 0)
	if serverDebug > 0 {
		logger.Printf(format, a...)
	}
}

type Reply struct {
	cid     int64
	seq     int64
	content interface{}
}

func (kv *KVServer) receiveMsg(index, term int, cid, seq int64) bool {
	for {
		msg := <-kv.applyCh
		kv.debug("接收到 Raft 消息: %+v", msg)
		op, ok1 := msg.Command.(Op)
		if !ok1 {
			num, ok2 := msg.Command.(int)
			if !ok2 || num != 0 { // no operation
				log.Fatalf("Unknown Command")
			}
			continue
		}
		if op.Type == PUT {
			kv.data[op.Key] = op.Value
		} else if op.Type == APPEND {
			kv.data[op.Key] += op.Value
		}
		if msg.CommandIndex == index {
			return op.Cid == cid && op.Seq == seq
		}
	}
}

func (kv *KVServer) findReplyInSession(cid, seq int64) *Reply {
	for _, re := range kv.session[cid] {
		if re.cid == cid && re.seq == seq {
			return &re
		}
	}
	return nil
}

func (kv *KVServer) receiveFromRaft(opType string, key string, value string, cid int64, seq int64) string {
	op := Op{Type: opType, Key: key, Value: value, Cid: cid, Seq: seq}
	kv.debug("*** 尝试发送给Raft {%+v}", op)
	index, term, isLeader := kv.rf.Start(op)
	if isLeader == false || kv.receiveMsg(index, term, cid, seq) == false {
		return ErrWrongLeader
	}
	return OK
}

func (kv *KVServer) saveReply(cid int64, seq int64, content interface{}) {
	kv.session[cid] = append(kv.session[cid], Reply{cid: cid, seq: seq, content: content})
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if re := kv.findReplyInSession(args.Cid, args.Seq); re != nil {
		*reply = re.content.(GetReply)
		reply.LeaderId = kv.leaderId
		reply.Err = OK
		kv.debug("收到已执行过的Get消息: %+v, 回复: %+v", args, reply)
		return
	}
	if kv.receiveFromRaft(GET, args.Key, "", args.Cid, args.Seq) == ErrWrongLeader {
		reply.LeaderId = kv.leaderId
		reply.Err = ErrWrongLeader
		kv.debug("不是Leader,拒绝消息: %+v", args)
		return
	}
	value, ok := kv.data[args.Key]
	reply.Value = value
	reply.LeaderId = kv.me
	if !ok {
		reply.Err = ErrNoKey
	} else {
		reply.Err = OK
	}
	kv.saveReply(args.Cid, args.Seq, *reply)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.debug("收到消息: %+v", args)
	if re := kv.findReplyInSession(args.Cid, args.Seq); re != nil {
		*reply = re.content.(PutAppendReply)
		reply.LeaderId = kv.leaderId
		reply.Err = OK
		kv.debug("执行过的消息: %+v", args)
		return
	}
	if kv.receiveFromRaft(args.Op, args.Key, args.Value, args.Cid, args.Seq) == ErrWrongLeader {
		reply.LeaderId = kv.leaderId
		reply.Err = ErrWrongLeader
		kv.debug("不是Leader,不处理: %+v", args)
		return
	}
	reply.LeaderId = kv.me
	reply.Err = OK
	kv.debug("处理消息: %+v, 并回复: %+v", args, reply)
	kv.saveReply(args.Cid, args.Seq, *reply)
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	util.DPrintf("server: %+v *******", servers)
	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.data = make(map[string]string)
	kv.leaderId = -1
	kv.session = make(map[int64][]Reply)
	return kv
}
