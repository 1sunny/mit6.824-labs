package shardkv

import (
	"6.824/labrpc"
	"6.824/shardctrler"
	"6.824/util"
	"bytes"
	"fmt"
	"log"
	"os"
	"sync/atomic"
	"time"
)
import "6.824/raft"
import "sync"
import "6.824/labgob"

const shardKvDebug = 1

// caller should hold lock
func (kv *ShardKV) debug(format string, a ...interface{}) {
	if shardKvDebug > 0 {
		var logger = log.New(os.Stdout, fmt.Sprintf("%s ShardKV[%d:%d] ", util.GetTimeBuf(), kv.gid, kv.me), 0)
		logger.Printf(format, a...)
	}
}

const (
	RequestTimeOut  = (raft.ElectionTimeOutMin + raft.ElectionTimeOutMax) / 2 * time.Millisecond
	SnapShotTimeOut = raft.HeartBeatTimeOut * 4
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
	Seq   int64
	Value string
	Err   Err
}

type WaitingInfo struct {
	cid       int64
	seq       int64
	term      int
	replyChan chan Reply
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	dead   int32
	scc    *shardctrler.Clerk
	config shardctrler.Config

	data        [shardctrler.NShards]map[string]string
	waitingInfo map[int]WaitingInfo
	//  one table entry per client, rather than one per RPC
	//  each client has only one RPC outstanding at a time
	//  each client numbers RPCs sequentially
	//  when server receives client RPC #10,
	//    it can forget about client's lower entries
	//    since this means client won't ever re-send older RPCs
	dupTable    map[int64]Reply
	lastApplied int
}

func (kv *ShardKV) exec(op *Op) (value string, err Err) {
	shard := key2shard(op.Key)
	if op.Type == Get {
		v, ok := kv.data[shard][op.Key]
		if ok == false {
			return "", ErrNoKey
		}
		kv.debug("Shard: %v, Get %v = %v", shard, op.Key, kv.data[shard][op.Key])
		return v, OK
	} else if op.Type == Put {
		kv.data[shard][op.Key] = op.Value
		kv.debug("Shard: %v, Put %v = %v", shard, op.Key, kv.data[shard][op.Key])
	} else if op.Type == Append {
		kv.data[shard][op.Key] += op.Value
		kv.debug("Shard: %v, Append %v = %v", shard, op.Key, kv.data[shard][op.Key])
	}
	return "", OK
}

func (kv *ShardKV) applyOp(msg *raft.ApplyMsg) {
	kv.mu.Lock()
	kv.debug("接收到 Raft apply 消息: {%+v}", msg)
	util.Assert(msg.CommandIndex == kv.lastApplied+1, "msg.CommandIndex != kv.lastApplied + 1")
	kv.lastApplied = msg.CommandIndex
	op, ok := msg.Command.(Op)
	if ok == false { // NoOp
		op = Op{}
	}
	cid, seq := op.Cid, op.Seq
	var reply Reply
	if seq != 0 {
		kv.debug("seq: %v, kv.dupTable[%v].Seq: %v", seq, cid, kv.dupTable[cid].Seq)
		// 因为分片后 seq并不连续,所以这里使用 >
		if seq > kv.dupTable[cid].Seq {
			reply.Seq = seq
			reply.Value, reply.Err = kv.exec(&op)
			// save reply
			kv.dupTable[cid] = reply
		} else if seq == kv.dupTable[cid].Seq {
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
		if kv.config.Shards[key2shard(op.Key)] != kv.gid {
			reply = Reply{Err: ErrWrongGroup}
		}
		if rfTerm != msg.Term || isLeader == false ||
			info.cid != cid || info.seq != seq { // leader has changed
			reply = Reply{Err: ErrWrongLeader}
		}
		select {
		case info.replyChan <- reply:
		case <-time.After(10 * time.Millisecond): // 防止请求由于超时已经返回,没有接收端而导致阻塞
			kv.debug("可能由于超时,请求: %+v 已经返回", info)
		}
	}
	kv.mu.Unlock()
}

func (kv *ShardKV) applySnapShot(msg *raft.ApplyMsg) {
	kv.mu.Lock()
	util.Assert(kv.lastApplied <= msg.SnapshotIndex, "kv.lastApplied >  msg.SnapshotIndex")
	r := bytes.NewBuffer(msg.Snapshot)
	d := labgob.NewDecoder(r)
	e1 := d.Decode(&kv.dupTable)
	e2 := d.Decode(&kv.data)
	util.Assert(e1 == nil && e2 == nil, "applySnapShot Error")
	kv.lastApplied = msg.SnapshotIndex
	kv.debug("Apply SnapShot, data: %+v", kv.data)
	kv.debug("Apply SnapShot, dupTable: %+v", kv.dupTable)
	kv.debug("Apply SnapShot, lastApplied: %v", kv.lastApplied)
	kv.mu.Unlock()
}

func (kv *ShardKV) raftReceiver() {
	for kv.killed() == false {
		msg := <-kv.applyCh
		if msg.CommandValid {
			kv.applyOp(&msg)
		} else if msg.SnapshotValid {
			kv.applySnapShot(&msg)
		} else {
			util.Assert(false, "Message Invalid")
		}
	}
}

func (kv *ShardKV) KVState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.dupTable)
	e.Encode(kv.data)
	return w.Bytes()
}

func (kv *ShardKV) snapshoter() {
	for kv.killed() == false && kv.maxraftstate != -1 {
		time.Sleep(SnapShotTimeOut)
		kv.mu.Lock()
		kv.debug("Check Snapshot ?")
		if kv.rf.Persister.RaftStateSize() > kv.maxraftstate {
			kv.debug("Do Snapshot !")
			kv.rf.Snapshot(kv.lastApplied, kv.KVState())
			kv.debug("SnapShot, data: %+v", kv.data)
			kv.debug("SnapShot, dupTable: %+v", kv.dupTable)
			kv.debug("SnapShot, lastApplied: %v", kv.lastApplied)
		}
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) matchShard(key string) bool {
	return kv.config.Shards[key2shard(key)] == kv.gid
}

func (kv *ShardKV) handleRequest(opType, key, value string, cid, seq int64) (v string, err Err) {
	kv.mu.Lock()
	if kv.matchShard(key) == false {
		kv.mu.Unlock()
		return "", ErrWrongGroup
	}
	if reply := kv.dupTable[cid]; reply.Seq == seq {
		kv.debug("处理过的消息: {type: %v, key: %v, value: %v, cid: %v, seq: %v}", opType, key, value, cid, seq)
		kv.mu.Unlock()
		return reply.Value, reply.Err
	}

	op := Op{Type: opType, Key: key, Value: value, Cid: cid, Seq: seq}
	index, term, leader := kv.rf.Start(op)
	if leader == false {
		kv.mu.Unlock()
		return "", ErrWrongLeader
	}
	kv.debug("*** 收到 {%+v}, 发给了 Raft", op)
	replyCh := make(chan Reply)
	kv.waitingInfo[index] = WaitingInfo{cid: cid, seq: seq, term: term, replyChan: replyCh}
	kv.mu.Unlock()

	var retv string
	var rete Err
	select {
	case reply := <-replyCh:
		retv, rete = reply.Value, reply.Err
	case <-time.After(RequestTimeOut):
		rete = ErrWrongLeader
	}
	kv.mu.Lock()
	kv.debug("回复消息: {type: %v, key: %v, value: %v, cid: %v, seq: %v}: {value: %v, err: %v}", opType, key, value, cid, seq, retv, rete)
	kv.mu.Unlock()
	return retv, rete
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	reply.Value, reply.Err = kv.handleRequest(Get, args.Key, "", args.Cid, args.Seq)
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	_, reply.Err = kv.handleRequest(args.Op, args.Key, args.Value, args.Cid, args.Seq)
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&kv.dead, 1)
	kv.mu.Lock()
	kv.debug("下线")
	kv.mu.Unlock()
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) fetchCfg() {
	for kv.killed() == false {
		time.Sleep(50 * time.Millisecond)
		cfg := kv.scc.Query(-1)
		kv.mu.Lock()
		kv.config = cfg
		kv.mu.Unlock()
	}
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	kv.scc = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh, fmt.Sprintf("KV[%v]", gid))

	for i := 0; i < shardctrler.NShards; i++ {
		kv.data[i] = make(map[string]string)
	}
	kv.waitingInfo = make(map[int]WaitingInfo)
	kv.dupTable = make(map[int64]Reply)

	go kv.fetchCfg()
	go kv.raftReceiver()
	go kv.snapshoter()
	return kv
}
