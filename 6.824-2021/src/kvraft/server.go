package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/util"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const serverDebug = 1

// caller should hold lock
func (kv *KVServer) debug(format string, a ...interface{}) {
	var logger = log.New(os.Stdout, fmt.Sprintf("%s Server[%d] ", util.GetTimeBuf(), kv.me), 0)
	if serverDebug > 0 {
		logger.Printf(format, a...)
	}
}

const (
	GET            = "Get"
	PUT            = "Put"
	APPEND         = "Append"
	RequestTimeOut = (raft.ElectionTimeOutMin + raft.ElectionTimeOutMax) / 2
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

type Reply struct {
	seq   int64
	value string
	err   Err
}

type WaitingInfo struct {
	cid       int64
	seq       int64
	term      int
	replyChan chan Reply
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	data        map[string]string
	waitingInfo map[int]WaitingInfo
	//  one table entry per client, rather than one per RPC
	//  each client has only one RPC outstanding at a time
	//  each client numbers RPCs sequentially
	//  when server receives client RPC #10,
	//    it can forget about client's lower entries
	//    since this means client won't ever re-send older RPCs
	dupTable map[int64]Reply
	lastTerm int
}

func (kv *KVServer) exec(op *Op) (value string, err Err) {
	if op.Type == GET {
		v, ok := kv.data[op.Key]
		if ok == false {
			return "", ErrNoKey
		}
		return v, OK
	} else if op.Type == PUT {
		kv.data[op.Key] = op.Value
		kv.debug("PUT %v = %v", op.Key, kv.data[op.Key])
	} else if op.Type == APPEND {
		kv.data[op.Key] += op.Value
		kv.debug("APPEND %v = %v", op.Key, kv.data[op.Key])
	}
	return "", OK
}

func (kv *KVServer) applyOp(msg *raft.ApplyMsg) {
	kv.mu.Lock()
	kv.debug("接收到 Raft apply 消息: {%+v}", msg)
	op, ok := msg.Command.(Op)
	if ok == false { // NoOp
		op = Op{}
	}
	cid, seq := op.Cid, op.Seq
	var reply Reply
	if seq != 0 {
		// ** 同一Client ** 执行过的操作不再执行
		if seq > kv.dupTable[cid].seq { // fix BUG: if op.Seq > kv.maxSeqDone[cid] {
			reply.seq = seq
			reply.value, reply.err = kv.exec(&op)
			// save reply
			kv.dupTable[cid] = reply
		} else if seq == kv.dupTable[cid].seq {
			reply = kv.dupTable[cid]
		} else {
			util.Assert(false, "No Reply found !!!")
		}
	}
	rfTerm, isLeader := kv.rf.GetState()
	// 尝试获取等待该 index 的请求信息
	info, ok := kv.waitingInfo[msg.CommandIndex]
	if ok {
		delete(kv.waitingInfo, msg.CommandIndex)
		kv.debug("删除键值: [%d:%+v], ", msg.CommandIndex, info)
		if rfTerm != msg.Term || isLeader == false ||
			info.cid != cid || info.seq != seq { // leader has changed
			reply = Reply{err: ErrWrongLeader}
		}
		kv.debug("发送回复到 index: %v, chan: %v, : %v", msg.CommandIndex, info.replyChan, reply)
		select {
		case info.replyChan <- reply:
		case <-time.After(10 * time.Millisecond): // 防止请求由于超时已经返回,没有接收端而导致阻塞
			kv.debug("可能由于超时,请求: %+v 已经返回", info)
		}
	}
	kv.mu.Unlock()
}

func (kv *KVServer) receiveRaftMsg() {
	for kv.killed() == false {
		msg := <-kv.applyCh
		if msg.CommandValid {
			kv.applyOp(&msg)
		} else {

		}
	}
}

func (kv *KVServer) handleRequest(opType, key, value string, cid, seq int64) (v string, err Err) {
	kv.mu.Lock()
	kv.debug("收到消息: {type: %v, key: %v, value: %v, cid: %v, seq: %v}", opType, key, value, cid, seq)
	if reply := kv.dupTable[cid]; reply.seq == seq {
		kv.debug("处理过的消息: {type: %v, key: %v, value: %v, cid: %v, seq: %v}", opType, key, value, cid, seq)
		kv.mu.Unlock()
		return reply.value, reply.err
	}

	op := Op{Type: opType, Key: key, Value: value, Cid: cid, Seq: seq}
	kv.debug("*** 尝试发送给Raft {%+v}", op)
	index, term, leader := kv.rf.Start(op)
	if leader == false {
		kv.mu.Unlock()
		return "", ErrWrongLeader
	}
	replyCh := make(chan Reply)
	kv.waitingInfo[index] = WaitingInfo{cid: cid, seq: seq, term: term, replyChan: replyCh}
	kv.mu.Unlock()

	var retv string
	var rete Err
	select {
	case reply := <-replyCh:
		retv, rete = reply.value, reply.err
	case <-time.After(RequestTimeOut * time.Millisecond):
		rete = ErrWrongLeader
	}
	kv.mu.Lock()
	kv.debug("回复消息: {type: %v, key: %v, value: %v, cid: %v, seq: %v}: {value: %v, err: %v}", opType, key, value, cid, seq, retv, rete)
	kv.mu.Unlock()
	return retv, rete
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	reply.Value, reply.Err = kv.handleRequest(GET, args.Key, "", args.Cid, args.Seq)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	_, reply.Err = kv.handleRequest(args.Op, args.Key, args.Value, args.Cid, args.Seq)
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

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.data = make(map[string]string)
	kv.waitingInfo = make(map[int]WaitingInfo)
	kv.dupTable = make(map[int64]Reply)
	kv.lastTerm = 1

	go kv.receiveRaftMsg()
	return kv
}
