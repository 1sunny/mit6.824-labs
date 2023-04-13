package shardctrler

import (
	"6.824/raft"
	"6.824/util"
	"fmt"
	"log"
	"os"
	"sort"
	"sync/atomic"
	"time"
)
import "6.824/labrpc"
import "sync"
import "6.824/labgob"

const shardCtrlerDebug = 1

// caller should hold lock
func (sc *ShardCtrler) debug(format string, a ...interface{}) {
	if shardCtrlerDebug > 0 {
		var logger = log.New(os.Stdout, fmt.Sprintf("%s Ctrl[%d] ", util.GetTimeBuf(), sc.me), 0)
		logger.Printf(format, a...)
	}
}

const (
	RequestTimeOut = (raft.ElectionTimeOutMin + raft.ElectionTimeOutMax) / 2 * time.Millisecond
)

type Reply struct {
	Seq         int64
	WrongLeader bool
	Cfg         Config
	Err         Err
}

type WaitingInfo struct {
	cid       int64
	seq       int64
	term      int
	replyChan chan Reply
}

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	dead        int32 // set by Kill()
	waitingInfo map[int]WaitingInfo
	dupTable    map[int64]Reply
	configs     []Config // indexed by config num
}

type Op struct {
	// Your data here.
	Type       string
	Cid        int64
	Seq        int64
	Servers    map[int][]string // join
	GIDs       []int            // leave
	Shard, GID int              // move
	Num        int              // query
}

func (sc *ShardCtrler) exec(op *Op) (err Err, cf Config) {
	if op.Type == Query {
		if op.Num < 0 || op.Num >= len(sc.configs) {
			return OK, sc.configs[len(sc.configs)-1]
		}
		return OK, sc.configs[op.Num]
	}
	prevCfg := sc.configs[len(sc.configs)-1]
	cfg := Config{
		Num:    len(sc.configs),
		Shards: prevCfg.Shards,
		Groups: make(map[int][]string),
	}
	for k, v := range prevCfg.Groups {
		servers := make([]string, len(v))
		copy(servers, v)
		cfg.Groups[k] = servers
	}
	sc.debug("执行 %v 前: Shards: %v, Groups: %+v", op.Type, prevCfg.Shards, prevCfg.Groups)
	if op.Type == Join {
		for k, v := range op.Servers {
			cfg.Groups[k] = v
		}
		cfg.handOut()
	} else if op.Type == Leave {
		for _, gid := range op.GIDs {
			delete(cfg.Groups, gid)
			for i, v := range cfg.Shards {
				if v == gid {
					cfg.Shards[i] = 0
				}
			}
		}
		cfg.handOut()
	} else if op.Type == Move {
		cfg.Shards[op.Shard] = op.GID
	} else {
		util.Assert(false, "Unknown Op Type")
	}
	sc.debug("执行 %v 后: Shards: %v, Groups: %+v", op.Type, cfg.Shards, cfg.Groups)
	sc.configs = append(sc.configs, cfg)
	return OK, Config{}
}

func (cfg *Config) handOut() {
	if len(cfg.Groups) == 0 {
		for i := range cfg.Shards {
			cfg.Shards[i] = 0
		}
		return
	}

	mn, mod := NShards/len(cfg.Groups), NShards%len(cfg.Groups)
	cnt := make(map[int]int)
	var shards []int
	for i, gid := range cfg.Shards {
		if gid == 0 || cnt[gid] == mn+1 || (cnt[gid] == mn && mod == 0) {
			shards = append(shards, i)
		} else {
			cnt[gid]++
			if cnt[gid] == mn+1 {
				mod--
			}
		}
	}

	keys := make([]int, 0, len(cfg.Groups))
	for k := range cfg.Groups {
		keys = append(keys, k)
	}
	sort.Ints(keys)

	curr := 0
	for _, gid := range keys {
		for cnt[gid] < mn {
			cnt[gid]++
			cfg.Shards[shards[curr]] = gid
			curr = curr + 1
		}
		if mod > 0 && cnt[gid] == mn {
			mod--
			cfg.Shards[shards[curr]] = gid
			curr = curr + 1
		}
	}
}

func (sc *ShardCtrler) applyOp(msg *raft.ApplyMsg) {
	sc.mu.Lock()
	sc.debug("接收到 Raft apply 消息: {%+v}", msg)
	op, ok := msg.Command.(Op)
	if ok == false { // NoOp
		op = Op{}
	}
	cid, seq := op.Cid, op.Seq
	var reply Reply
	if seq != 0 {
		sc.debug("seq: %v, kv.dupTable[%v].Seq: %v", seq, cid, sc.dupTable[cid].Seq)
		// ** 同一Client ** 执行过的操作不再执行
		if seq == sc.dupTable[cid].Seq+1 {
			reply.WrongLeader, reply.Seq = false, seq
			reply.Err, reply.Cfg = sc.exec(&op)
			// save reply
			sc.dupTable[cid] = reply
		} else if seq == sc.dupTable[cid].Seq {
			reply = sc.dupTable[cid]
		} else {
			util.Assert(false, "No Reply found !!!")
		}
	}
	rfTerm, isLeader := sc.rf.GetState()
	// 尝试获取等待该 index 的请求信息
	info, ok := sc.waitingInfo[msg.CommandIndex]
	if ok {
		delete(sc.waitingInfo, msg.CommandIndex)
		if rfTerm != msg.Term || isLeader == false ||
			info.cid != cid || info.seq != seq { // leader has changed
			reply = Reply{WrongLeader: true, Err: ErrWrongLeader}
		}
		select {
		case info.replyChan <- reply:
		case <-time.After(10 * time.Millisecond): // 防止请求由于超时已经返回,没有接收端而导致阻塞
			sc.debug("可能由于超时,请求: %+v 已经返回", info)
		}
	}
	sc.mu.Unlock()
}

func (sc *ShardCtrler) raftReceiver() {
	for sc.killed() == false {
		msg := <-sc.applyCh
		if msg.CommandValid {
			sc.applyOp(&msg)
		} else {
			util.Assert(false, "Message Invalid")
		}
	}
}

func (sc *ShardCtrler) handleRequest(
	opType string, cid, seq int64,
	servers map[int][]string, // join
	gIDs []int, // leave
	shard, gID int, // move
	num int, // query
) (wl bool, e Err, cf Config) {
	sc.mu.Lock()
	op := Op{Type: opType, Cid: cid, Seq: seq,
		Servers: servers,
		GIDs:    gIDs,
		Shard:   shard, GID: gID,
		Num: num,
	}
	if reply := sc.dupTable[cid]; reply.Seq == seq {
		sc.debug("处理过的操作: %+v", op)
		sc.mu.Unlock()
		return false, reply.Err, reply.Cfg
	}
	index, term, leader := sc.rf.Start(op)
	if leader == false {
		sc.mu.Unlock()
		return true, ErrWrongLeader, Config{}
	}
	sc.debug("*** 收到: %+v, 发给了 Raft", op)
	replyCh := make(chan Reply)
	sc.waitingInfo[index] = WaitingInfo{cid: cid, seq: seq, term: term, replyChan: replyCh}
	sc.mu.Unlock()

	var retwl bool
	var rete Err
	var retCf Config
	select {
	case reply := <-replyCh:
		retwl, rete, retCf = reply.WrongLeader, reply.Err, reply.Cfg
	case <-time.After(RequestTimeOut):
		retwl, rete = true, ErrWrongLeader
	}
	sc.mu.Lock()
	sc.debug("回复消息: %+v: {wl: %v, e: %v, cf: %+v}", op, retwl, rete, retCf)
	sc.mu.Unlock()
	return retwl, rete, retCf
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	reply.WrongLeader, reply.Err, _ = sc.handleRequest(Join, args.Cid, args.Seq, args.Servers, nil, -1, -1, -1)
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	reply.WrongLeader, reply.Err, _ = sc.handleRequest(Leave, args.Cid, args.Seq, nil, args.GIDs, -1, -1, -1)
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	reply.WrongLeader, reply.Err, _ = sc.handleRequest(Move, args.Cid, args.Seq, nil, nil, args.Shard, args.GID, -1)
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	reply.WrongLeader, reply.Err, reply.Config = sc.handleRequest(Query, args.Cid, args.Seq, nil, nil, -1, -1, args.Num)
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&sc.dead, 1)
	sc.mu.Lock()
	sc.debug("下线")
	sc.mu.Unlock()
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh, "Ctrl")

	// Your code here.
	sc.dupTable = make(map[int64]Reply)
	sc.waitingInfo = make(map[int]WaitingInfo)

	go sc.raftReceiver()
	return sc
}
