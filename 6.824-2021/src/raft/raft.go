package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.824/labgob"
	"6.824/util"
	"bytes"
	"fmt"
	"log"
	"os"
	"sort"
	"time"

	//	"bytes"
	"sync"
	"sync/atomic"

	//	"6.824/labgob"
	"6.824/labrpc"
)

const raftDebug = 0

// caller should hold rf.lock
func (rf *Raft) debug(format string, a ...interface{}) {
	if raftDebug > 0 {
		var logger = log.New(os.Stdout, fmt.Sprintf("%s %v[%d:%v:%d:%d] ", util.GetTimeBuf(), rf.name, rf.me, rf.state, rf.currentTerm, rf.commitIndex), 0)
		logger.Printf(format, a...)
	}
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	Term int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

func (p ApplyMsg) String() string {
	if p.CommandValid {
		return fmt.Sprintf("%v:%v ", p.CommandIndex, p.Command)
	}
	return fmt.Sprintf("%v:%v ", p.SnapshotIndex, p.SnapshotTerm)
}

type State string

const (
	FOLLOWER              State = "Follower"
	CANDIDATE             State = "Candidate"
	LEADER                State = "Leader"
	DEAD                  State = "Dead"
	NoOp                        = 0
	ElectionTimeOutMax          = 500
	ElectionTimeOutMin          = 300
	HeartBeatTimeOut            = (ElectionTimeOutMax - ElectionTimeOutMin) / 4 * time.Millisecond
	ElectionCheckInterval       = 20 * time.Millisecond
)

type Log struct {
	Term int
	Cmd  interface{}
}

func (p Log) String() string {
	return fmt.Sprintf("%d:%+v ", p.Term, p.Cmd)
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	Persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	numPeer int
	cond    *sync.Cond
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm  int // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor     int // candidateId that received vote in current term (or null if none)
	state        State
	lastBeatTime time.Time
	randDuration int
	// log
	logs []Log

	commitIndex int // index of highest log entry known to be committed
	lastApplied int // index of highest log entry applied to state machine
	// leader
	nextIndex      []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex     []int // for each server, index of highest log entry known to be replicated on server
	matchIndexCopy []int
	applyCh        chan ApplyMsg
	// 2D
	snapShotIndex int
	snapShotTerm  int
	snapshotMu    sync.Mutex
	name          string
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	term := rf.currentTerm
	isLeader := rf.state == LEADER
	rf.mu.Unlock()
	return term, isLeader
}

func (rf *Raft) raftState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.snapShotIndex)
	e.Encode(rf.snapShotTerm)
	return w.Bytes()
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
// --- comment from 2023 lab ---
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	rf.debug("持久数据 currentTerm: [%d], votedFor: [%d], 日志长度:[%d]", rf.currentTerm, rf.votedFor, len(rf.logs))
	rf.Persister.SaveRaftState(rf.raftState())
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []Log
	e1 := d.Decode(&currentTerm)
	e2 := d.Decode(&votedFor)
	e3 := d.Decode(&logs)
	var snapShotIndex int
	var snapShotTerm int
	e4 := d.Decode(&snapShotIndex)
	e5 := d.Decode(&snapShotTerm)
	util.Assert(e1 == nil && e2 == nil && e3 == nil && e4 == nil && e5 == nil, "readPersist Error")

	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.logs = logs
	rf.snapShotIndex = snapShotIndex
	rf.snapShotTerm = snapShotTerm
	rf.debug("读取数据 currentTerm: [%d], votedFor: [%d], snapShotIndex: [%v], snapShotTerm: [%v],"+
		" logs: [%v]", rf.currentTerm, rf.votedFor, rf.snapShotIndex, rf.snapShotTerm, rf.logs)
	// fix BUG:不能在这里读取 snapshot,否则 appCh一直不会被读取,阻塞
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate’s term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry
	LastLogTerm  int // term of candidate’s last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

func (rf *Raft) restartElectionTimer() {
	rf.randDuration = util.GetRandomNum(ElectionTimeOutMin, ElectionTimeOutMax)
	rf.lastBeatTime = time.Now()
}

func (rf *Raft) timeout() bool {
	return time.Since(rf.lastBeatTime).Milliseconds() > int64(rf.randDuration)
}

func (rf *Raft) toFollower(newTerm int) {
	rf.debug("状态 [%v] -> [%v], 任期 [%d] -> [%d]", rf.state, FOLLOWER, rf.currentTerm, newTerm)
	rf.currentTerm = newTerm
	rf.state = FOLLOWER
	rf.votedFor = -1 // fix BUG
}

func (rf *Raft) toCandidate() {
	if rf.killed() {
		return
	}
	/* 可以直接变为 candidate, 如果sleep时接收到心跳,那么会重置 lastBeatTime,不会走到这里,
	如果刚要接收到心跳就走到这里,那么也没有关系,currentTerm没有被更新,只是发送投票请求得不到票而已
	*/
	// Candidates (§5.2): On conversion to candidate, start election:
	rf.state = CANDIDATE
	// Candidates (§5.2): Increment currentTerm, 这里currentTerm会不会被改变过了,由于其它节点先成为leader并发送心跳给当前节点
	rf.currentTerm++
	// Candidates (§5.2):  Vote for self
	rf.votedFor = rf.me // !
	// Candidates (§5.2): Reset election timer
	rf.restartElectionTimer()
	rf.debug("在任期 [%d] 开始竞选", rf.currentTerm)
	rf.persist()
	// Candidates (§5.2): Send RequestVote RPCs to all other servers
	rf.sendRequestVote() // fix BUG: 在解锁之前发送投票RPC,以免锁被选举计时器再拿去,而投票RPC拿不到,相当于没投票就进入了下一次睡眠
}

func (rf *Raft) toLeader() {
	rf.state = LEADER
	// However, if S1 replicates an en-try from its current term on a majority of the servers
	// before crashing, as in (e), then this entry is committed (S5 cannot win an election)
	rf.logs = append(rf.logs, Log{Term: rf.currentTerm, Cmd: NoOp})
	rf.persist()
	rf.debug("*** 成为任期 [%d] 的Leader", rf.currentTerm)
	// 2B 当 leader 刚上任时，它会初始化所有的 nextIndex 值为最后一条日志的下一个索引
	rf.debug("初始化所有 nextIndex 为 [%d]", rf.LastLogIndex())
	for i := range rf.peers {
		rf.nextIndex[i] = rf.LastLogIndex() // 因为最新的 NoOp其它节点肯定没有,所以-1
		rf.matchIndex[i] = 0                // fix BUG: lab3时因为这个在 TestManyPartitionsManyClients3A 出现了错误!
	}
	rf.matchIndex[rf.me] = rf.LastLogIndex()
}

func (rf *Raft) isLogUpToDateThanMe(argsLastLogTerm, argsLogIndex int) bool {
	// lastLogIndex := len(rf.logs) - 1
	lastLogIndex := rf.LastLogIndex()
	lastLogTerm := rf.LastLogTerm()
	// fix BUG: add args.LastLogTerm == rf.logs[lastLogIndex].Term
	return argsLastLogTerm > lastLogTerm || (argsLastLogTerm == lastLogTerm && argsLogIndex >= lastLogIndex)
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// currentTerm, for candidate to update itself
	reply.Term = rf.currentTerm
	// Receiver implementation: #1 Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}
	defer rf.persist()
	// All Servers: If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	if args.Term > rf.currentTerm {
		rf.toFollower(args.Term)
	}
	// Receiver implementation: #2 If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	/* args.Term >= rf.currentTerm */
	if rf.votedFor == -1 && rf.isLogUpToDateThanMe(args.LastLogTerm, args.LastLogIndex) {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.restartElectionTimer() // 投票后重置定时器
	} else {
		reply.VoteGranted = false
	}
	// rf.debug("%v 票给 [%d] 任期 [%d]", reply.VoteGranted, args.CandidateId, args.Term)
}

func (rf *Raft) sendRequestVote() {
	count := 1
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(x int) {
			rf.mu.Lock()
			oldTerm := rf.currentTerm
			args := RequestVoteArgs{
				Term:         oldTerm,
				CandidateId:  rf.me,
				LastLogIndex: rf.LastLogIndex(),
				LastLogTerm:  rf.LastLogTerm(),
			}
			rf.debug("在任期 [%d] 给 [%d] 发送 {%+v}", oldTerm, x, args)
			rf.mu.Unlock()
			reply := RequestVoteReply{}
			// --
			if !rf.peers[x].Call("Raft.RequestVote", &args, &reply) {
				return
			}
			// --
			rf.mu.Lock()
			//rf.debug("在任期 [%d] 收到 [%d] 回复: {%+v}", oldTerm, x, reply)
			defer rf.mu.Unlock()
			// All Servers: If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
			if reply.Term > rf.currentTerm {
				rf.toFollower(reply.Term)
				rf.persist()
				return
			}

			if !reply.VoteGranted {
				return
			}
			//rf.debug("在任期: [%d] 得到了 [%d] 的投票", oldTerm, x)
			count++
			// Candidates (§5.2): If votes received from majority of servers: become leader
			// 如果之前被变为 follower,则竞选失败,因为任期现在可能已经变化了
			if count > rf.numPeer/2 && rf.state == CANDIDATE && rf.currentTerm == oldTerm {
				rf.toLeader()
			}
		}(i)
	}
}

func (rf *Raft) startElection() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.state == LEADER || !rf.timeout() {
			rf.mu.Unlock()
			time.Sleep(ElectionCheckInterval)
			rf.mu.Lock()
		}
		rf.toCandidate()
		rf.mu.Unlock()
		// Candidates (§5.2): If election timeout elapses: start new election
	}
}

func (rf *Raft) applyLogs() {
	snapshot := rf.Persister.ReadSnapshot()
	if snapshot != nil && len(snapshot) > 0 {
		// 一旦 raft 丢弃了之前的日志，状态机就会担负起另外两种新的责任。
		// 如果 server 重启，在状态机可以 apply raft 日志之前需要从磁盘加载这些数据
		msg := ApplyMsg{
			SnapshotValid: true,
			Snapshot:      snapshot,
			SnapshotTerm:  rf.snapShotTerm,
			SnapshotIndex: rf.snapShotIndex,
		}
		rf.applyCh <- msg
		rf.mu.Lock()
		rf.lastApplied = rf.snapShotIndex // fix BUG
		rf.commitIndex = rf.snapShotIndex // fix BUG
		rf.debug("发送了 ApplyMsg: %v, lastApplied=[%v], commitIndex=[%v]", msg, rf.lastApplied, rf.commitIndex)
		rf.mu.Unlock()
	}
	for !rf.killed() {
		rf.mu.Lock()
		// 由于 Wait 第一次恢复时 c.L 并未锁定，因此调用者一般不能假定 Wait 返回时条件为真。取而代之，调用者应当把 Wait 放入循环中：
		for rf.lastApplied == rf.commitIndex {
			rf.cond.Wait() // 调用时释放锁，被唤醒后获取锁
		}
		rf.lastApplied++
		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.Logs(rf.lastApplied).Cmd, // TODO 接收到snapshot后记得更新 lastApplied
			CommandIndex: rf.lastApplied,
			Term:         rf.Logs(rf.lastApplied).Term,
		}
		rf.debug("提交 [%v]", msg)
		rf.mu.Unlock() // fix BUG: 先释放锁,applyCh可能会阻塞 ?
		rf.applyCh <- msg
	}
}

type AppendEntriesArgs struct {
	Term         int   // leader’s term
	LeaderId     int   // so follower can redirect clients
	PrevLogIndex int   // index of log entry immediately preceding new ones
	PrevLogTerm  int   // term of PrevLogIndex entry
	Entries      []Log // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int   // leader’s commitIndex
}

type AppendEntriesReply struct {
	Term              int  // currentTerm, for leader to update itself
	Success           bool // true if follower contained entry matching PrevLogIndex and PrevLogTerm
	LastLogIndex      int
	XTerm             int
	FirstIndexOfXTerm int
}

func (rf *Raft) receiveLogs(args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	/* Receiver implementation: #2 - #5 */
	// Receiver implementation: #2 Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	if rf.LastLogIndex() < args.PrevLogIndex {
		reply.XTerm = -1
		reply.LastLogIndex = rf.LastLogIndex()
		reply.Success = false
		return false
	}
	if args.PrevLogIndex < rf.snapShotIndex { // 无法对比 term 且 可能会造成和 snapshot重复
		rf.debug("args.PrevLogIndex < rf.snapShotIndex !!!!!!!")
		reply.XTerm = -1
		reply.LastLogIndex = rf.snapShotIndex
		reply.Success = false
		return false
	}
	// snapShotIndex <= args.PrevLogIndex <= rf.LastLogIndex()
	prevLogTerm := rf.PrevLogTerm(args.PrevLogIndex + 1) // args.PrevLogIndex+1 > snapShotIndex
	if prevLogTerm != args.PrevLogTerm {
		reply.XTerm = prevLogTerm
		reply.FirstIndexOfXTerm = args.PrevLogIndex
		for reply.FirstIndexOfXTerm-1 > rf.snapShotIndex && rf.Logs(reply.FirstIndexOfXTerm-1).Term == prevLogTerm {
			reply.FirstIndexOfXTerm--
			//if reply.FirstIndexOfXTerm <= 0 {
			//	log.Fatalf("reply.FirstIndexOfXTerm <= 0")
			//}
		}
		reply.Success = false
		return false
	}
	reply.Success = true
	// 如果跟随者有领导者发送的所有条目，跟随者必须不截断其日志。
	// 必须保留领导发送的条目后面的任何元素。这是因为我们可能从领导者那里接收到一个过时的 AppendEntry RPC，
	// 截断日志将意味着“收回”我们可能已经告诉领导者我们的日志中的条目。
	// Receiver implementation: #3 If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it (§5.3)
	oldLogLen := len(rf.logs)
	j := 0
	for i := args.PrevLogIndex + 1; i <= rf.LastLogIndex(); i++ { // rf.LastLogIndex() >= args.PrevLogIndex
		if j >= len(args.Entries) { // fix BUG
			break
		}
		// fix BUG: if rf.logs[i].Term != args.Entries[j].Term {
		// fix BUG: if j >= len(args.Entries || rf.logs[i].Term != args.Entries[j].Term {
		/* snapShotIndex <= args.PrevLogIndex <= rf.LastLogIndex(): i > snapShotIndex */
		if rf.Logs(i).Term != args.Entries[j].Term {
			rf.logs = rf.logs[:rf.L2A(i)] // fix BUG rf.logs = rf.logs[:i-1]
			break
		}
		j++
	}
	// Receiver implementation: #4 Append any new entries not already in the log
	if j != len(args.Entries) {
		rf.logs = append(rf.logs, args.Entries[j:]...)
	}
	rf.debug("日志长度: [%d] -> [%d]", oldLogLen, len(rf.logs))
	return true
}

func (rf *Raft) updateCommitIndex(args *AppendEntriesArgs) {
	// Receiver implementation: #5 If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	oldCommitIndex := rf.commitIndex
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit > args.PrevLogIndex+len(args.Entries) { // fix BUG: if args.LeaderCommit > len(rf.logs)-1 {
			rf.commitIndex = args.PrevLogIndex + len(args.Entries) // fix BUG: rf.commitIndex = len(rf.logs) - 1
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		// args.LeaderCommit 不应该一定比args.PrevLogIndex+len(args.Entries)小吗 ?
		// Leader可能分段发送日志导致 args.LeaderCommit > args.PrevLogIndex+len(args.Entries) ?
		util.Assert(rf.commitIndex >= oldCommitIndex, fmt.Sprintf("commitIndex: [%d] -> [%d]", oldCommitIndex, rf.commitIndex))
		rf.debug("commitIndex: [%d] -> [%d]", oldCommitIndex, rf.commitIndex)
		rf.cond.Broadcast()
	}
}

func (rf *Raft) receiveHeartBeat(term int) {
	/* restart timer */
	rf.restartElectionTimer()
	if term > rf.currentTerm {
		// All Servers: If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
		rf.toFollower(term)
	} else { // args.Term = rf.currentTerm
		util.Assert(rf.state != LEADER, "rf.state == LEADER")
		/* 另一个 server 被确定为 leader。在等待投票的过程中，candidate 可能收到来自其他 server 的 AppendEntries RPC，声明它才是 leader。如果 RPC 中的 term 大于等于candidate的current term，candidate就会认为这个leader是合法的并转为follower状态。如果 RPC 中的 term 比自己当前的小，将会拒绝这个请求并保持 candidate 状态。*/
		if rf.state == CANDIDATE {
			// Candidates (§5.2): If AppendEntries RPC received from new leader: convert to follower
			rf.toFollower(term)
		}
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.snapshotMu.Lock()
	defer rf.snapshotMu.Unlock()
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.debug("在任期: [%d] 收到 [%d] 的心跳 [%+v]", rf.currentTerm, args.LeaderId, args)
	reply.Term = rf.currentTerm
	// Receiver implementation: #1 Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}
	defer rf.persist()

	rf.receiveHeartBeat(args.Term)

	if rf.receiveLogs(args, reply) == false {
		return
	}
	rf.updateCommitIndex(args)
}

func (rf *Raft) majorityMatchIndex() int {
	copy(rf.matchIndexCopy, rf.matchIndex)
	//rf.debug("排序前: %v", rf.matchIndexCopy)
	sort.Ints(rf.matchIndexCopy)
	//rf.debug("排序后: %v", rf.matchIndexCopy)
	return rf.matchIndexCopy[rf.numPeer/2]
}

func (rf *Raft) fastBackUp(peer int, reply *AppendEntriesReply) {
	oldNextIndex := rf.nextIndex[peer]
	if reply.XTerm == -1 {
		rf.nextIndex[peer] = reply.LastLogIndex + 1
	} else {
		lastIndexOfTerm := -1
		for j := 1; j < len(rf.logs); j++ {
			if rf.logs[j].Term == reply.XTerm {
				lastIndexOfTerm = rf.A2L(j)
			} else if rf.logs[j].Term > reply.XTerm {
				break
			}
		}
		if lastIndexOfTerm == -1 {
			rf.nextIndex[peer] = reply.FirstIndexOfXTerm
		} else {
			rf.nextIndex[peer] = lastIndexOfTerm + 1
		}
	}
	rf.debug("nextIndex[%d]: [%d] -> [%d]", peer, oldNextIndex, rf.nextIndex[peer])
	util.Assert(rf.nextIndex[peer] >= 1, "rf.nextIndex[x] < 1")
}

func (rf *Raft) leaderUpdateCommitIndex() {
	// Leaders: If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N,
	// and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4)
	N := rf.majorityMatchIndex()
	oldCommitIndex := rf.commitIndex
	// rf.Logs(N) 不需要检查越界,因为此时日志还未 commit
	if N > rf.commitIndex && rf.Logs(N).Term == rf.currentTerm { // fix BUG: if indexes[rf.numPeer/2] > rf.commitIndex {
		rf.commitIndex = N
		rf.cond.Broadcast()
	}
	rf.debug("commitIndex: [%d] -> [%d] ", oldCommitIndex, rf.commitIndex)
}

func (rf *Raft) appendEntries() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(x int) {
			rf.mu.Lock()
			// fix BUG: 如果它不是Leader了(发送RPC后term太小被拒绝),然后term更新,状态变为 Follower,就没有资格继续发送了
			// 否则可能会覆盖一些已有的提交
			if rf.state != LEADER || rf.killed() {
				rf.mu.Unlock()
				return
			}
			oldTerm := rf.currentTerm
			oldMatchIndex := rf.matchIndex[x]
			if rf.nextIndex[x] <= rf.snapShotIndex { // 对端需要的日志已经被删除了,发送 InstallSnapShot RPC
				snapShotIndex := rf.snapShotIndex
				args := InstallSnapShotArgs{
					Term:              oldTerm,
					LeaderId:          rf.me,
					LastIncludedIndex: snapShotIndex,
					LastIncludedTerm:  rf.snapShotTerm,
					Data:              rf.Persister.snapshot,
				}
				reply := InstallSnapShotReply{}
				rf.debug("发送 InstallSnapShot: %+v", args)
				rf.mu.Unlock()
				if !rf.peers[x].Call("Raft.InstallSnapShot", &args, &reply) {
					return
				}
				rf.mu.Lock()
				defer rf.mu.Unlock()
				rf.debug("收到 InstallSnapShot回复: %+v", reply)
				if reply.Term > rf.currentTerm {
					rf.toFollower(reply.Term)
					rf.persist()
					return
				}
				if oldTerm != rf.currentTerm {
					rf.debug("InstallSnapShot oldTerm != rf.currentTerm的回复 {%+v}", reply)
					return
				}
				rf.debug("nextIndex[%d]: [%d] -> [%d], matchIndex[%d]: [%d] -> [%d]",
					x, rf.nextIndex[x], snapShotIndex+1, x, rf.matchIndex[x], snapShotIndex)
				// fix BUG: 更新 index
				rf.matchIndex[x] = snapShotIndex
				rf.nextIndex[x] = snapShotIndex + 1
				rf.leaderUpdateCommitIndex()
				return
			}
			/* rf.nextIndex[x] > rf.snapShotIndex */
			prevIndex := rf.nextIndex[x] - 1
			entries := make([]Log, rf.LastLogIndex()-rf.nextIndex[x]+1)
			copy(entries, rf.logs[rf.L2A(rf.nextIndex[x]):])
			args := AppendEntriesArgs{
				Term:         oldTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevIndex,
				PrevLogTerm:  rf.PrevLogTerm(rf.nextIndex[x]),
				Entries:      entries,
				LeaderCommit: rf.commitIndex,
			}
			reply := AppendEntriesReply{}
			rf.debug("在任期: [%d] 发给 [%d], Entries: [%+v]", oldTerm, x, args)
			rf.mu.Unlock()
			// --- fix BUG: 判断 RPC是否成功,不成功直接返回,否则后面会出错
			if !rf.peers[x].Call("Raft.AppendEntries", &args, &reply) {
				return
			}
			// ---
			rf.mu.Lock()
			rf.debug("在任期: [%d] 收到 [%d] 回复 [%+v]", rf.currentTerm, x, reply)
			defer rf.mu.Unlock()
			// 可以避免睡眠后不是Leader的情况
			if reply.Term > rf.currentTerm {
				rf.toFollower(reply.Term)
				rf.persist()
				// 领导者发送了一个 AppendEntries RPC，但RPC被拒绝了，但不是因为日志不一致（这只有在我们的任期结束时才会发生,由于新的领导者产生了?????）
				// 那么你应该立即辞职下台，而不是更新 nextIndex。 如果你这样做，如果你立即连任，你可能与 nextIndex 的重置产生竞争。
				return
			}
			/**
			term混淆是指服务器被来自旧term的 RPC 混淆。
			通常，在收到 RPC 时这不是问题，因为图 2 中的规则准确说明了当您看到旧term时应该做什么。
			但是，图 2 通常不会讨论当您收到旧的 RPC 回复时应该做什么。
			根据经验，我们发现到目前为止最简单的做法是先记录回复中的term（它可能高于您当前的term），
			然后将当前term与您在原始 RPC 中发送的term进行比较，如果两者不同，则丢弃回复并返回。
			*/
			// Old RPC !!!!!!! matchIndex 是单调递增的
			if oldTerm != rf.currentTerm || prevIndex+len(entries) < rf.matchIndex[x] {
				rf.debug("接收到过时的回复 {%+v}", reply)
				return
			}
			if reply.Success {
				/* 假设在发送 RPC 和收到回复之间您的状态没有改变。一个很好的例子是当您收到对 RPC 的响应时设置 matchIndex = nextIndex - 1 或 matchIndex = len(log)这是不安全的，因为自从您发送 RPC 后，这两个值都可能已更新。相反，正确的做法是根据您最初在 RPC 中发送的参数将 matchIndex 更新为 prevLogIndex + len(entries[]) */
				rf.debug("nextIndex[%d]: [%d] -> [%d], matchIndex[%d]: [%d] -> [%d]",
					x, prevIndex+1, prevIndex+len(entries)+1,
					x, oldMatchIndex, prevIndex+len(entries))
				rf.matchIndex[x] = prevIndex + len(entries)
				rf.nextIndex[x] = rf.matchIndex[x] + 1

				rf.leaderUpdateCommitIndex()
			} else {
				rf.fastBackUp(x, &reply)
			}
		}(i)
	}
}

// Leaders: Upon election: send initial emptyAppendEntries RPCs (heartbeat) to each server;
// repeat during idle periods to prevent election timeouts (§5.2)
func (rf *Raft) appendEntriesTicker() {
	for !rf.killed() {
		// The tester requires that the leader send heartbeat RPCs no more than ten times per second.
		// Because the tester limits you to 10 heartbeats per second, you will have to use an election timeout larger
		// than the paper’s 150 to 300 milliseconds, but not too large, because then you may fail to elect a leader within five seconds.
		time.Sleep(HeartBeatTimeOut)
		rf.mu.Lock()
		if rf.state == LEADER {
			rf.appendEntries()
		}
		rf.mu.Unlock() // fix BUG: 调用 appendEntries 将协程打开后再释放锁,否则锁可能被选举定时器拿走
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.debug("Snapshot ")
	if index <= rf.snapShotIndex {
		rf.debug("Snapshot index 过时返回")
		return
	}
	rf.debug("Old: snapShotIndex: [%v], snapShotTerm: [%v]", rf.snapShotIndex, rf.snapShotTerm)
	rf.debug("Old logs: [%v]", rf.logs)
	/* index > rf.snapShotIndex */
	rf.snapShotTerm = rf.Logs(index).Term
	rf.logs = rf.logs[rf.L2A(index):]
	rf.snapShotIndex = index
	rf.Persister.SaveStateAndSnapshot(rf.raftState(), snapshot)
	rf.debug("New: snapShotIndex: [%v], snapShotTerm: [%v]", rf.snapShotIndex, rf.snapShotTerm)
	rf.debug("New logs: [%v]", rf.logs)
}

type InstallSnapShotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	// lastConfig
	// offset
	Data []byte
	// done
}

type InstallSnapShotReply struct {
	Term       int
	MatchIndex int
}

func (rf *Raft) LastLogIndex() int {
	return rf.snapShotIndex + len(rf.logs) - 1
}

func (rf *Raft) LastLogTerm() int {
	return rf.logs[len(rf.logs)-1].Term
}

// array index to log index
func (rf *Raft) A2L(i int) int {
	return i + rf.snapShotIndex
}

// log index to array index
func (rf *Raft) L2A(i int) int {
	return i - rf.snapShotIndex
}

// caller must ensure i > snapShotIndex
func (rf *Raft) PrevLogTerm(i int) int {
	util.Assert(i > rf.snapShotIndex && i <= rf.LastLogIndex()+1, fmt.Sprintf("[%v] <= rf.snapShotIndex: [%v] || [%v] > rf.LastLogIndex()+1:[%v]", i, rf.snapShotIndex, i, rf.LastLogIndex()+1))
	return rf.logs[rf.L2A(i-1)].Term
}

// caller must ensure i > snapShotIndex
func (rf *Raft) Logs(i int) Log {
	util.Assert(i > rf.snapShotIndex && i <= rf.LastLogIndex(), fmt.Sprintf("[%v] <= rf.snapShotIndex: [%v] || [%v] > rf.LastLogIndex():[%v]", i, rf.snapShotIndex, i, rf.LastLogIndex()))
	return rf.logs[rf.L2A(i)]
}

func (rf *Raft) InstallSnapShot(args *InstallSnapShotArgs, reply *InstallSnapShotReply) {
	rf.snapshotMu.Lock()
	defer rf.snapshotMu.Unlock()
	rf.mu.Lock()
	rf.debug("收到 InstallSnapShot: %+v", args)
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm { // #1
		rf.debug("InstallSnapShot Term 过时返回")
		rf.mu.Unlock()
		return
	}

	rf.receiveHeartBeat(args.Term)

	if args.LastIncludedIndex <= rf.snapShotIndex {
		rf.debug("InstallSnapShot LastIncludedIndex 过时返回")
		rf.mu.Unlock()
		return
	}
	rf.debug("Old: snapShotIndex: [%v], snapShotTerm: [%v],"+
		"lastApplied: [%v], commitIndex: [%v]", rf.snapShotIndex, rf.snapShotTerm, rf.lastApplied, rf.commitIndex)
	rf.debug("logs: [%v]", rf.logs)

	if args.LastIncludedIndex <= rf.LastLogIndex() &&
		/* args.LastIncludedIndex > rf.snapShotIndex */
		rf.Logs(args.LastIncludedIndex).Term == args.LastIncludedTerm {
		// 这里不要更新 lastApplied 和 commitIndex, 让 AppendEntries 更新,
		// 因为日志虽然有但是可能没有提交或已经提交得比snapshot多,如果将两者直接更新为 LastIncludedTerm,
		// 紧接着在 AppendEntries 可能会提交 LastIncludedTerm + 1, apply out of order
		// 如果对应 log位置 term不同,直接丢弃所有日志,应用 snapshot
		rf.mu.Unlock()
		return
	}
	// rf.Persister.SaveStateAndSnapshot(rf.raftState(), args.Data)
	/* args.LastIncludedIndex >= rf.LastLogIndex() */
	rf.logs = []Log{{Term: args.LastIncludedTerm}} // #7
	rf.snapShotIndex, rf.snapShotTerm = args.LastIncludedIndex, args.LastIncludedTerm
	rf.Persister.SaveStateAndSnapshot(rf.raftState(), args.Data) // #2-#5, fix BUG: 应该在持久化状态更新后再保存
	rf.lastApplied, rf.commitIndex = args.LastIncludedIndex, args.LastIncludedIndex
	rf.debug("New: snapShotIndex: [%v], snapShotTerm: [%v],"+
		"lastApplied: [%v], commitIndex: [%v]", rf.snapShotIndex, rf.snapShotTerm, rf.lastApplied, rf.commitIndex)
	rf.debug("logs: [%v]", rf.logs)
	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	rf.debug("解锁后将发送 ApplyMsg: %v", msg)
	rf.mu.Unlock()
	// fix BUG: unlock后可能被AppendEntries拿到锁,接收到后面的log,造成后面的log先于msg提交(apply out of order)
	rf.applyCh <- msg // #8
	/*(这一步没有加锁,否则会死锁(service调用SnapShot需要获取锁,不会读取applyCh,
		而我们要发送了snapshot才会释放锁))已经有其它日志提交了(AppendEntries)并
	被service读取(apply out of order),如果service再读到snapshot并应用,那么造成了回滚现象*/
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
//func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
//	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
//	return ok
//}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != LEADER {
		return -1, -1, false
	}
	// Leaders: If command received from client: append entry to local log, respond after entry applied to state machine (§5.3)
	rf.logs = append(rf.logs, Log{
		Term: rf.currentTerm,
		Cmd:  command,
	})
	rf.matchIndex[rf.me] = rf.LastLogIndex()
	rf.debug("*** 在任期 [%d] 收到命令index:[%d]: %+v, 日志长度:[%d]", rf.currentTerm, rf.LastLogIndex(), command, len(rf.logs))
	rf.persist()
	return rf.LastLogIndex(), rf.currentTerm, true
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.mu.Lock()
	rf.state = DEAD
	rf.debug("下线")
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg, name string) *Raft {
	rf := &Raft{
		mu:             sync.Mutex{},
		peers:          peers,
		Persister:      persister,
		me:             me,
		dead:           0,
		numPeer:        len(peers),
		cond:           nil,
		currentTerm:    0,
		votedFor:       -1,
		state:          FOLLOWER,
		lastBeatTime:   time.Now(),
		randDuration:   0,
		logs:           []Log{{Term: 0, Cmd: 0}},
		commitIndex:    0,
		lastApplied:    0,
		nextIndex:      make([]int, len(peers)),
		matchIndex:     make([]int, len(peers)),
		matchIndexCopy: make([]int, len(peers)),
		applyCh:        applyCh,
		name:           name,
	}
	rf.cond = sync.NewCond(&rf.mu) // fix BUG: initialize early avoid nil pointer when Broadcast
	if me != 0 {
		rf.restartElectionTimer() // 0 start election first
	}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.startElection()
	go rf.appendEntriesTicker()
	go rf.applyLogs()
	return rf
}
