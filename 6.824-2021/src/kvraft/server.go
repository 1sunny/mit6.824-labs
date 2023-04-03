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
	data         map[string]string
	waitingInfo  map[int]WaitingInfo
	replySession map[int64][]Reply // cid 已经执行过的回复
	maxSeqDone   map[int64]int64   // cid 最后执行的 seq 号
	lastTerm     int
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
	} else if op.Type == APPEND {
		kv.data[op.Key] += op.Value
	}
	return "", OK
}

func (kv *KVServer) receiveApplyMsg() {
	for kv.killed() == false {
		msg := <-kv.applyCh
		kv.mu.Lock()
		kv.debug("接收到 Raft 消息: {%+v}", msg)
		if msg.CommandValid {
			op, ok := msg.Command.(Op)
			if ok == false { // NoOp
				//continue // fix BUG
				op = Op{}
			}
			cid, seq := op.Cid, op.Seq
			var re Reply
			if seq != 0 {
				// ** 同一Client ** 执行过的操作不再执行
				if seq > kv.maxSeqDone[cid] { // fix BUG: if op.Seq > kv.maxSeqDone[cid] {
					kv.maxSeqDone[cid] = seq
					re.seq = seq
					re.value, re.err = kv.exec(&op)
					// save reply
					kv.replySession[cid] = append(kv.replySession[cid], re)
				} else {
					pReply := kv.findReplyInSession(cid, seq)
					if pReply == nil {
						log.Fatal("pReply == nil")
					}
					re = *pReply
				}
			}
			// 尝试获取等待该 index 的请求信息
			info, ok := kv.waitingInfo[msg.CommandIndex]
			if ok {
				kv.debug("info: %+v", info)
				delete(kv.waitingInfo, msg.CommandIndex)
				kv.debug("删除键: [%d]", msg.CommandIndex)
				reply := re
				if info.cid != cid || info.seq != seq { // leader has changed
					reply = Reply{err: ErrWrongLeader}
				}
				kv.debug("发送回复到 index: %v, chan: %v, : %v", msg.CommandIndex, info.replyChan, reply)
				select {
				case info.replyChan <- reply:
				}
			}
			raftTerm, _ := kv.rf.GetState()
			if kv.lastTerm < raftTerm { // term has changed, may be leader has changed too
				kv.lastTerm = raftTerm
				var toDelete []int
				for k, v := range kv.waitingInfo {
					kv.debug("K: %+v, V: %+v", k, v)
					if v.term < raftTerm {
						kv.debug("term 发送回复到 index: %v, chan: %v, : %v", k, v.replyChan, Reply{err: ErrWrongLeader})
						select {
						case v.replyChan <- Reply{err: ErrWrongLeader}: // fix BUG: info.replyChan -> v.replyChan
						}
						toDelete = append(toDelete, k)
					}
				}
				for _, index := range toDelete {
					delete(kv.waitingInfo, index)
					kv.debug("删除键: [%d]", index)
				}
			}
		} else {

		}
		kv.mu.Unlock()
	}
}

func (kv *KVServer) findReplyInSession(cid, seq int64) *Reply {
	for _, re := range kv.replySession[cid] {
		if re.seq == seq {
			return &re
		}
	}
	return nil
}

func (kv *KVServer) handleRequest(opType, key, value string, cid, seq int64) (v string, err Err) {
	kv.mu.Lock()
	kv.debug("收到消息: {type: %v, key: %v, value: %v, cid: %v, seq: %v}", opType, key, value, cid, seq)
	if re := kv.findReplyInSession(cid, seq); re != nil {
		kv.debug("处理过的消息: {type: %v, key: %v, value: %v, cid: %v, seq: %v}", opType, key, value, cid, seq)
		kv.mu.Unlock()
		return re.value, re.err
	}

	op := Op{Type: opType, Key: key, Value: value, Cid: cid, Seq: seq}
	kv.debug("*** 尝试发送给Raft {%+v}", op)
	index, term, leader := kv.rf.Start(op)
	if leader == false {
		kv.mu.Unlock()
		return "", ErrWrongLeader
	}
	replyCh := make(chan Reply)
	kv.debug("开启 index: %v, chan: %v", index, replyCh)
	kv.waitingInfo[index] = WaitingInfo{cid: cid, seq: seq, term: term, replyChan: replyCh}
	kv.mu.Unlock()

	var retv string
	var rete Err
	select {
	case re := <-replyCh:
		retv, rete = re.value, re.err
	}
	kv.mu.Lock()
	close(replyCh)
	kv.debug("关闭 index: %v, chan: %v", index, replyCh)
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
	kv.replySession = make(map[int64][]Reply)
	kv.maxSeqDone = make(map[int64]int64)
	kv.lastTerm = 1

	go kv.receiveApplyMsg()
	return kv
}
