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
	RequestTimeOut     = (raft.ElectionTimeOutMin + raft.ElectionTimeOutMax) / 2 * time.Millisecond
	FetchConfigTimeOut = 80 * time.Millisecond
	PullShardTimeout   = 100 * time.Millisecond
	DeleteShardTimeout = 100 * time.Millisecond
)

const (
	ToServe  = "ToServe"
	ToPull   = "ToPull"
	ToBePull = "ToBePull"
	ToDelete = "ToDelete"
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
	index     int // raft index 唯一表示一个请求
	term      int
	replyChan chan Reply
}

type Shard struct {
	Data   map[string]string
	Status string
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
	dead       int32
	scc        *shardctrler.Clerk
	config     shardctrler.Config
	prevConfig shardctrler.Config

	shards      [shardctrler.NShards]*Shard
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

func (kv *ShardKV) ToRaft(cmd Cmd, reply *Reply) {
	index, term, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.debug("提交到Raft: %v", cmd)

	kv.mu.Lock()
	replyCh := make(chan Reply)
	kv.waitingInfo[index] = WaitingInfo{index: index, term: term, replyChan: replyCh}
	kv.mu.Unlock()

	select {
	case res := <-replyCh:
		reply.Value, reply.Err = res.Value, res.Err
	case <-time.After(RequestTimeOut):
		reply.Err = ErrTimeout
	}
	// 异步为了提高吞吐量，这里不需要阻塞客户端请求
	go func() {
		kv.mu.Lock()
		delete(kv.waitingInfo, index)
		close(replyCh)
		kv.mu.Unlock()
	}()
}

func (kv *ShardKV) applyRaftMsg(msg *raft.ApplyMsg) {
	kv.debug("接收到 Raft apply 消息: {%+v}", msg)
	util.Assert(msg.CommandIndex == kv.lastApplied+1, "msg.CommandIndex != kv.lastApplied + 1")
	kv.lastApplied = msg.CommandIndex
	cmd, ok := msg.Command.(Cmd)
	if ok == false { // NoOp
		return
	}

	var reply Reply
	switch cmd.OpType {
	case Operation:
		op := cmd.Data.(Op)
		reply = kv.applyOperation(&op)
	case Config:
		nextConfig := cmd.Data.(shardctrler.Config)
		reply = kv.applyConfig(&nextConfig)
	case Insert:
		pullReply := cmd.Data.(ShardReply)
		reply = kv.applyInsert(&pullReply)
	case Delete:
		args := cmd.Data.(ShardArgs)
		reply = kv.applyDelete(&args)
	default:
		panic("未知操作类型")
	}
	// 只让 Leader回复
	if term, isLeader := kv.rf.GetState(); isLeader && msg.Term == term {
		info, ok := kv.waitingInfo[msg.CommandIndex]
		if ok {
			util.Assert(info.term == term, "info.term != currentTerm")
			select {
			case info.replyChan <- reply:
				kv.debug("回复 %+v", reply)
			case <-time.After(10 * time.Millisecond):
				kv.debug("可能由于超时,请求: %+v 已经返回", info)
			}
		}
	}

	kv.trySnapshot()
}

func (kv *ShardKV) raftReceiver() {
	for kv.killed() == false {
		msg := <-kv.applyCh
		kv.mu.Lock()
		if msg.CommandValid {
			kv.applyRaftMsg(&msg)
		} else if msg.SnapshotValid {
			kv.applySnapShot(&msg)
		} else {
			panic("Message Invalid")
		}
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) canServe(shardID int) bool {
	return kv.config.Shards[shardID] == kv.gid && (kv.shards[shardID].Status == ToServe || kv.shards[shardID].Status == ToDelete)
}

func (kv *ShardKV) Command(op *Op, reply *Reply) {
	kv.mu.Lock()
	util.Assert(op.Seq >= kv.dupTable[op.Cid].Seq, "op.Seq < kv.dupTable[op.Cid].Seq")
	if op.Seq == kv.dupTable[op.Cid].Seq {
		reply.Value, reply.Err = kv.dupTable[op.Cid].Value, kv.dupTable[op.Cid].Err
		kv.mu.Unlock()
		return
	}
	// 如果当前分片无法提供服务，则返回 ErrWrongGroup 让客户端获取最新配置，
	if kv.canServe(key2shard(op.Key)) == false {
		reply.Err = ErrWrongGroup
		kv.debug("ErrWrongGroup, Config: %+v", kv.config)
		kv.printShardStatus()
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	kv.ToRaft(Cmd{OpType: Operation, Data: *op}, reply)
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	var re Reply
	kv.Command(&Op{Type: Get, Key: args.Key, Value: "", Cid: args.Cid, Seq: args.Seq}, &re)
	reply.Value, reply.Err = re.Value, re.Err
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	var re Reply
	kv.Command(&Op{Type: args.Op, Key: args.Key, Value: args.Value, Cid: args.Cid, Seq: args.Seq}, &re)
	_, reply.Err = re.Value, re.Err
}

func (kv *ShardKV) exec(op *Op, shardID int) (value string, err Err) {
	if op.Type == Get {
		v, ok := kv.shards[shardID].Data[op.Key]
		if ok == false {
			return "", ErrNoKey
		}
		kv.debug("Shard: %v, Get %v = %v", shardID, op.Key, kv.shards[shardID].Data[op.Key])
		return v, OK
	} else if op.Type == Put {
		kv.shards[shardID].Data[op.Key] = op.Value
		kv.debug("Shard: %v, Put %v = %v", shardID, op.Key, kv.shards[shardID].Data[op.Key])
	} else if op.Type == Append {
		kv.shards[shardID].Data[op.Key] += op.Value
		kv.debug("Shard: %v, Append %v = %v", shardID, op.Key, kv.shards[shardID].Data[op.Key])
	}
	return "", OK
}

func (kv *ShardKV) applyOperation(op *Op) Reply {
	var reply Reply
	shardID := key2shard(op.Key)
	if kv.canServe(shardID) {
		if op.Seq == kv.dupTable[op.Cid].Seq {
			reply = kv.dupTable[op.Cid]
		} else if op.Seq > kv.dupTable[op.Cid].Seq {
			reply.Seq = op.Seq
			reply.Value, reply.Err = kv.exec(op, shardID)
			kv.dupTable[op.Cid] = reply
		} else {
			panic("No Reply found !!!") // 客户端不会重新发送已经得到回复的seq
		}
	} else {
		reply = Reply{Err: ErrWrongGroup}
	}
	return reply
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

func (kv *ShardKV) gid2ShardIDs(status string) map[int][]int {
	gid2shardIDs := make(map[int][]int)
	for shardID, v := range kv.shards {
		if v.Status == status {
			// gid := kv.config.Shards[shardID]
			gid := kv.prevConfig.Shards[shardID] // fix BUG
			gid2shardIDs[gid] = append(gid2shardIDs[gid], shardID)
		}
	}
	return gid2shardIDs
}

type ShardArgs struct {
	ConfigNum int
	ShardIDs  []int
}

type ShardReply struct {
	ConfigNum int
	Shards    map[int]Shard
	DupTable  map[int64]Reply
	Err       Err
}

func (sd *Shard) deepCopy() Shard {
	var shard Shard
	shard.Data = make(map[string]string)
	for k, v := range sd.Data {
		shard.Data[k] = v
	}
	return shard
}

func (kv *ShardKV) GetShard(args *ShardArgs, reply *ShardReply) {
	// only pull shards from leader
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	/**
	现在 group1和group2 在config4 分别负责一个 shard, 当 group1 重启后按顺序得到config,当得到config3 时就会向group2请求分片2,但现在 分片2 是 group2 在服务,这样就出现2个group对某个分片同时服务?
	config 0: 0 0
	config 1: 1 1
	config 2: 1 2
	config 3: 1 1
	config 4: 1 2
	当一个group重启后,从config.num=1开始获取config,获取到config后放入Raft,此时不用担心Server会根据获取到的前一个config和当前获取到的config开始去请求这个过时config中属于自己的Shard从而导致同一时刻有两个group服务同一个Shard(因为这个Config还未应用,只是放入了Raft),因为重启后会回放日志,日志中有重启之前这个Server经历过的config,当看到重启后重新获取到的过时config时会因为config.num将这些过时config直接丢弃(所以这些config根本没机会去Pull Shard)
	S=重启的Server
	为什么用 < ?
	因为在S下线这段时间,其它的Server可能已经成功经历了几次config变化(不需要这个S参与 -> 不需要从S Pull Shard, 所以他们可以在不需要这个S的情况下推进Config),所以S的config可能 < 其它Server
	当S重启后获取到它下线前config(C0)的下一版本的config(C1)后,它可以比较这两个config差异然后去Pull它需要的Shard,这时不用担心有其它Server正在服务这个Shard,因为这个Shard在这个C1属于S, 那么如果其它Server的config中也服务这个Shard,那么它们是需要等待这个S, 其它Server只是得到了Config,但不能真正开始服务这个Shard,因为在服务之前需要先从S Pull这个Shard, 但S会因为 config.num < args.num 拒绝其它Server的 Pull请求 即 (需要等待S服务完 -> S的config追上它们).
	*/
	if kv.config.Num < args.ConfigNum {
		reply.Err = ErrNotReady
		return
	}

	reply.Shards = make(map[int]Shard)
	for _, shardID := range args.ShardIDs {
		reply.Shards[shardID] = kv.shards[shardID].deepCopy()
	}

	reply.DupTable = make(map[int64]Reply)
	for cid, re := range kv.dupTable {
		reply.DupTable[cid] = re
	}

	reply.ConfigNum, reply.Err = args.ConfigNum, OK
	kv.debug("GetShard 返回 %+v", reply)
}

// 当applyConfig后,该Server负责的shard的状态会被改为ToPull,该协程检测到后就会进行拉取
func (kv *ShardKV) pullShard() {
	kv.mu.Lock()
	if kv.config.Num == 0 {
		kv.mu.Unlock()
		return
	}
	gid2shardIDs := kv.gid2ShardIDs(ToPull)
	kv.debug("gid2shardIDs: %+v", gid2shardIDs)
	var wg sync.WaitGroup
	for gid, shardIDs := range gid2shardIDs {
		wg.Add(1)
		go func(servers []string, configNum int, shardIDs []int, g int) {
			defer wg.Done()
			args := ShardArgs{configNum, shardIDs}
			for _, server := range servers { // 向这个group所有server发送请求,只有Leader回复
				var reply ShardReply
				kv.debug("向 [%v] 发送GetShard: %+v", g, args)
				if kv.make_end(server).Call("ShardKV.GetShard", &args, &reply) && reply.Err == OK {
					kv.debug("收到GetShard回复: %+v", reply)
					kv.ToRaft(Cmd{OpType: Insert, Data: reply}, &Reply{})
				}
			}
		}(kv.prevConfig.Groups[gid], kv.config.Num, shardIDs, gid)
	}
	kv.mu.Unlock()
	wg.Wait()
}

// Raft同步拉取到的Shard的日志后,该协程进行应用,将ToPull状态的Shard改为ToDelete以清理上一个负责该Shard的Server中的数据
func (kv *ShardKV) applyInsert(pullReply *ShardReply) Reply {
	res := Reply{Err: OK}
	kv.debug("applyInsert: %+v", pullReply)
	if pullReply.ConfigNum == kv.config.Num {
		for shardId, shard := range pullReply.Shards {
			myShard := kv.shards[shardId]
			if myShard.Status == ToPull {
				for k := range myShard.Data {
					delete(myShard.Data, k)
				}
				for k, v := range shard.Data {
					myShard.Data[k] = v
				}
				myShard.Status = ToDelete
			} else {
				// 重复收到这个 pullReply
				break
			}
		}
		// 将对方的 dupTable合并,对方 dupTable中保存了对于拿到的Shard的请求记录,以进行重复检测
		for cid, reply := range pullReply.DupTable {
			if myReply, ok := kv.dupTable[cid]; !ok || myReply.Seq < reply.Seq {
				kv.dupTable[cid] = reply
			}
		}
	} else {
		res = Reply{Err: ErrOutDated}
	}
	kv.printShardStatus()
	return res
}

func (kv *ShardKV) deleteShard() {
	kv.mu.Lock()
	gid2shardIDs := kv.gid2ShardIDs(ToDelete)
	var wg sync.WaitGroup
	for gid, shardIDs := range gid2shardIDs {
		wg.Add(1)
		go func(servers []string, configNum int, shardIDs []int) {
			defer wg.Done()
			args := ShardArgs{configNum, shardIDs}
			for _, server := range servers {
				var reply ShardReply
				if kv.make_end(server).Call("ShardKV.DeleteShard", &args, &reply) && reply.Err == OK {
					kv.ToRaft(Cmd{OpType: Delete, Data: args}, &Reply{})
				}
			}
		}(kv.prevConfig.Groups[gid], kv.config.Num, shardIDs)
	}
	kv.mu.Unlock()
	wg.Wait()
}

func (kv *ShardKV) DeleteShard(args *ShardArgs, shardReply *ShardReply) {
	// only delete shards when role is leader
	if _, isLeader := kv.rf.GetState(); !isLeader {
		shardReply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	if kv.config.Num > args.ConfigNum {
		shardReply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	var reply Reply
	kv.ToRaft(Cmd{OpType: Delete, Data: *args}, &reply)

	shardReply.Err = reply.Err
}

func NewShard() *Shard {
	return &Shard{Data: make(map[string]string), Status: ToServe}
}

func (kv *ShardKV) applyDelete(args *ShardArgs) Reply {
	kv.debug("applyDelete: %+v", args)
	if args.ConfigNum == kv.config.Num {
		for _, shardId := range args.ShardIDs {
			shard := kv.shards[shardId]
			if shard.Status == ToDelete {
				shard.Status = ToServe
			} else if shard.Status == ToBePull {
				kv.shards[shardId] = NewShard()
			} else {
				break
			}
		}
	}
	kv.printShardStatus()
	return Reply{Err: OK}
}

func (kv *ShardKV) fetchConfig() {
	canFetchNextConfig := true
	kv.mu.Lock()
	for _, shard := range kv.shards {
		if shard.Status != ToServe { // ToDelete时也不能获取下一个Config
			canFetchNextConfig = false
			break
		}
	}
	currentConfigNum := kv.config.Num
	kv.mu.Unlock()
	if canFetchNextConfig {
		nextConfig := kv.scc.Query(currentConfigNum + 1)
		if nextConfig.Num == currentConfigNum+1 {
			kv.debug("得到Config: %+v", nextConfig)
			kv.ToRaft(Cmd{OpType: Config, Data: nextConfig}, &Reply{})
		}
	}
}

func (kv *ShardKV) applyConfig(nextConfig *shardctrler.Config) Reply {
	if nextConfig.Num == kv.config.Num+1 {
		kv.debug("applyConfig: %+v", nextConfig)
		for shardID, gid := range nextConfig.Shards {
			if kv.config.Shards[shardID] == 0 {
				continue
			}
			if kv.config.Shards[shardID] != kv.gid && gid == kv.gid {
				kv.shards[shardID].Status = ToPull
			} else if kv.config.Shards[shardID] == kv.gid && gid != kv.gid {
				kv.shards[shardID].Status = ToBePull
			}
		}
		kv.prevConfig = kv.config
		kv.config = *nextConfig
		kv.debug("prevConfig: %+v, config: %+v", kv.prevConfig, kv.config)
		kv.printShardStatus()
		return Reply{Err: OK}
	}
	return Reply{Err: ErrOutDated}
}

func (kv *ShardKV) KVState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.dupTable)
	e.Encode(kv.prevConfig)
	e.Encode(kv.config)
	e.Encode(kv.shards)
	return w.Bytes()
}

func (kv *ShardKV) trySnapshot() {
	if kv.maxraftstate != -1 {
		kv.debug("Check Snapshot ?")
		if kv.rf.Persister.RaftStateSize() > kv.maxraftstate {
			kv.debug("Do Snapshot !")
			kv.rf.Snapshot(kv.lastApplied, kv.KVState())
			kv.debug("SnapShot, dupTable: %+v", kv.dupTable)
			kv.debug("SnapShot, prevConfig: %+v", kv.prevConfig)
			kv.debug("SnapShot, currentConfig: %+v", kv.config)
			kv.debug("SnapShot, shards: %+v", kv.shards)
			kv.debug("SnapShot, lastApplied: %v", kv.lastApplied)
		}
	}
}

func (kv *ShardKV) applySnapShot(msg *raft.ApplyMsg) {
	util.Assert(kv.lastApplied <= msg.SnapshotIndex, "kv.lastApplied >  msg.SnapshotIndex")
	r := bytes.NewBuffer(msg.Snapshot)
	d := labgob.NewDecoder(r)
	e1 := d.Decode(&kv.dupTable)
	e2 := d.Decode(&kv.prevConfig)
	e3 := d.Decode(&kv.config)
	e4 := d.Decode(&kv.shards)
	util.Assert(e1 == nil && e2 == nil && e3 == nil && e4 == nil, "applySnapShot Error")
	kv.lastApplied = msg.SnapshotIndex
	kv.debug("Apply, dupTable: %+v", kv.dupTable)
	kv.debug("Apply, prevConfig: %+v", kv.prevConfig)
	kv.debug("Apply, currentConfig: %+v", kv.config)
	kv.debug("Apply, shards: %+v", kv.shards)
	kv.debug("Apply, lastApplied: %v", kv.lastApplied)
}

func (kv *ShardKV) printShardStatus() {
	var status []string
	for _, v := range kv.shards {
		status = append(status, v.Status)
	}
	kv.debug("Status: %+v", status)
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
	labgob.Register(Cmd{})
	labgob.Register(Reply{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(ShardArgs{})
	labgob.Register(ShardReply{})

	applyCh := make(chan raft.ApplyMsg)

	kv := &ShardKV{
		mu:           sync.Mutex{},
		me:           me,
		rf:           raft.Make(servers, me, persister, applyCh, fmt.Sprintf("KV[%v]", gid)),
		applyCh:      applyCh,
		make_end:     make_end,
		gid:          gid,
		ctrlers:      ctrlers,
		maxraftstate: maxraftstate,
		dead:         0,
		scc:          shardctrler.MakeClerk(ctrlers),
		prevConfig:   shardctrler.Config{},
		config:       shardctrler.Config{},
		shards:       [shardctrler.NShards]*Shard{},
		waitingInfo:  make(map[int]WaitingInfo),
		dupTable:     make(map[int64]Reply),
		lastApplied:  0,
	}

	for i := 0; i < shardctrler.NShards; i++ {
		kv.shards[i] = NewShard()
	}

	go kv.raftReceiver()
	go kv.ticker(kv.fetchConfig, FetchConfigTimeOut)
	go kv.ticker(kv.pullShard, PullShardTimeout)
	go kv.ticker(kv.deleteShard, DeleteShardTimeout)

	return kv
}
