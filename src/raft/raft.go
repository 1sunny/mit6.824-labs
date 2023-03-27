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
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

func (p ApplyMsg) String() string {
	return fmt.Sprintf("CommandIndex:%d, Valid:%v", p.CommandIndex, p.CommandValid)
}

type State string

const (
	FOLLOWER  State = "Follower"
	CANDIDATE State = "Candidate"
	LEADER    State = "Leader"
	DEAD      State = "Dead"
	NoOp            = 0
)

type Log struct {
	Term int
	Cmd  interface{}
}

func (p Log) String() string {
	return fmt.Sprintf("%d ", p.Term)
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
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
	randDuration time.Duration
	// log
	logs []Log

	commitIndex int // index of highest log entry known to be committed
	lastApplied int // index of highest log entry applied to state machine
	// leader
	nextIndex  []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server
	applyCh    chan ApplyMsg
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

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	DPrintf("[%d] 持久数据 currentTerm: [%d], votedFor: [%d], logs: [%v]", rf.me, rf.currentTerm, rf.votedFor, rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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
	if err := d.Decode(&currentTerm); err != nil {
		DPrintf("读取currentTerm失败: [%v]", err)
		return
	}
	if err := d.Decode(&votedFor); err != nil {
		DPrintf("读取votedFor失败: [%v]", err)
		return
	}
	if err := d.Decode(&logs); err != nil {
		DPrintf("读取logs失败: [%v]", err)
		return
	}
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.logs = logs
	DPrintf("[%d] 读取数据 currentTerm: [%d], votedFor: [%d], logs: [%v]", rf.me, rf.currentTerm, rf.votedFor, rf.logs)
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
// 1. Reply false if term < currentTerm (§5.1)
// 2. If votedFor is null or candidateId, and candidate’s log is at
//    least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
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
	Term         int  // currentTerm, for leader to update itself
	Success      bool // true if follower contained entry matching PrevLogIndex and PrevLogTerm
	LastLogIndex int
	XTerm        int
	XIndex       int
}

func (rf *Raft) restartElectionTimer() {
	rf.randDuration = getRandomNum(150, 300)
	rf.lastBeatTime = time.Now()
}

func (rf *Raft) timeout() bool {
	return time.Since(rf.lastBeatTime) > rf.randDuration*time.Millisecond
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
		DPrintf("[%d] 状态 [%v] -> [%v], 任期 [%d] -> [%d]", rf.me, rf.state, FOLLOWER, rf.currentTerm, args.Term)
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		// !!!
		rf.votedFor = -1
		rf.restartElectionTimer()
	}
	// Receiver implementation: #2 If votedFor is null or candidateId,
	// and candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	lastIdx := len(rf.logs) - 1
	/* args.Term >= rf.currentTerm */
	if rf.votedFor == -1 &&
		// fix BUG: add args.LastLogTerm == rf.logs[lastIdx].Term
		(args.LastLogTerm > rf.logs[lastIdx].Term || (args.LastLogTerm == rf.logs[lastIdx].Term && args.LastLogIndex >= lastIdx)) {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		// 投票后重置定时器
		rf.restartElectionTimer()
		DPrintf("[%d] 投票给 [%d] 任期 [%d]", rf.me, args.CandidateId, args.Term)
	} else {
		DPrintf("[%d] 拒绝  [%d] 任期 [%d]", rf.me, args.CandidateId, args.Term)
		reply.VoteGranted = false
	}
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and *** return immediately ***. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. *** even if the Raft instance has been killed,***
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
	term := rf.currentTerm
	index := len(rf.logs)
	rf.matchIndex[rf.me] = len(rf.logs)
	// Leaders: If command received from client: append entry to local log, respond after entry applied to state machine (§5.3)
	rf.logs = append(rf.logs, Log{
		Term: term,
		Cmd:  command,
	})
	DPrintf("[%d] 在任期 [%d] 收到命令index: [%d], 所有日志: [%v]", rf.me, rf.currentTerm, index, rf.logs)
	rf.persist()
	return index, term, true
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
	DPrintf("[%d] 下线", rf.me)
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
// Make() *** must return quickly ***, so it should *** start goroutines
// for any long-running work ***.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.numPeer = len(peers)
	// 2A
	rf.currentTerm = 0
	rf.state = FOLLOWER
	rf.restartElectionTimer()
	rf.votedFor = -1
	// 2B
	rf.nextIndex = make([]int, rf.numPeer)
	rf.matchIndex = make([]int, rf.numPeer)
	rf.logs = make([]Log, 1)
	rf.applyCh = applyCh
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go startElection(rf)
	go applyLogs(rf)
	return rf
}

func applyLogs(rf *Raft) {
	rf.cond = sync.NewCond(&rf.mu)
	for !rf.killed() {
		rf.mu.Lock()
		// 由于 Wait 第一次恢复时 c.L 并未锁定，因此调用者一般不能假定 Wait 返回时条件为真。
		// 取而代之，调用者应当把 Wait 放入循环中：
		for rf.lastApplied == rf.commitIndex {
			rf.cond.Wait() // 调用时释放锁，被唤醒后获取锁
		}
		rf.lastApplied++
		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.logs[rf.lastApplied].Cmd,
			CommandIndex: rf.lastApplied,
		}
		rf.applyCh <- msg
		DPrintf("[%d] 提交 [%v]", rf.me, msg)
		rf.mu.Unlock()
	}
}

func getRandomNum(a, b int) time.Duration {
	rand.Seed(time.Now().UnixNano())
	return time.Duration(a + rand.Intn(b-a+1))
}

func startElection(rf *Raft) {
	for !rf.killed() {
		rf.mu.Lock()
		// 等待选举计时器超时
		for rf.state == LEADER || !rf.timeout() {
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond) // 也许更短的睡眠时间更能防止计时器同时超时
			rf.mu.Lock()
		}
		if rf.killed() {
			return
		}
		/* 可以直接变为 candidate, 如果sleep时接收到心跳,那么会重置 lastBeatTime,不会走到这里,
		如果刚要接收到心跳就走到这里,那么也没有关系,currentTerm没有被更新,只是发送投票请求得不到票而已
		*/
		// Candidates (§5.2): On conversion to candidate, start election:
		oldState := rf.state
		rf.state = CANDIDATE
		DPrintf("[%d] 选举计时器超时, 任期: [%v], 状态 [%v] -> [%v]", rf.me, rf.currentTerm, oldState, rf.state)
		// Candidates (§5.2): Increment currentTerm, 这里currentTerm会不会被改变过了,由于其它节点先成为leader并发送心跳给当前节点
		rf.currentTerm++
		// Candidates (§5.2):  Vote for self
		rf.votedFor = rf.me // !
		rf.persist()
		count := 1
		DPrintf("[%d] 在任期 [%d] 开始竞选", rf.me, rf.currentTerm)
		// Candidates (§5.2): Reset election timer
		rf.restartElectionTimer()
		term := rf.currentTerm
		lastLogIndex := len(rf.logs) - 1
		lastLogTerm := rf.logs[len(rf.logs)-1].Term
		rf.mu.Unlock()

		// Candidates (§5.2): Send RequestVote RPCs to all other servers
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go func(x, t, lastIdx, lastTerm int) {
				args := RequestVoteArgs{
					Term:         t,
					CandidateId:  rf.me,
					LastLogIndex: lastIdx,
					LastLogTerm:  lastTerm,
				}
				reply := RequestVoteReply{}
				// --
				if !rf.sendRequestVote(x, &args, &reply) {
					return
				}
				// --
				rf.mu.Lock()
				defer rf.mu.Unlock()
				DPrintf("[%d] 在任期 [%d] 收到 [%d] 回复: [%+v]", rf.me, t, x, reply)
				// All Servers: If RPC request or response contains term T > currentTerm:
				// set currentTerm = T, convert to follower (§5.1)
				if reply.Term > rf.currentTerm {
					DPrintf("[%d] 任期 [%d] -> [%d], 状态 [%v] -> [%v]", rf.me, rf.currentTerm, reply.Term, rf.state, FOLLOWER)
					rf.currentTerm = reply.Term
					rf.state = FOLLOWER
					rf.votedFor = -1
					rf.persist()
					rf.restartElectionTimer()
					return
				} else {
					DPrintf("[%d] 在任期 [%d] 收到 [%d] 的回复已失效", rf.me, t, x)
				}

				if !reply.VoteGranted {
					return
				}
				DPrintf("[%d] 在任期: [%d] 得到了 [%d] 的投票", rf.me, t, x)
				count++
				// Candidates (§5.2): If votes received from majority of servers: become leader
				if count > rf.numPeer/2 {
					// 如果之前被变为 follower,则竞选失败,因为任期现在可能已经变化了
					if rf.state == CANDIDATE && rf.currentTerm == t {
						rf.state = LEADER
						// However, if S1 replicates an en-try from its current term on a majority of the servers
						// before crashing, as in (e), then this entry is committed (S5 cannot win an election)
						rf.logs = append(rf.logs, Log{
							Term: rf.currentTerm,
							Cmd:  NoOp,
						}) // 不需要持久化
						DPrintf("[%d] 成为任期 [%d] 的Leader", rf.me, t)
						// 2B 当 leader 刚上任时，它会初始化所有的 nextIndex 值为最后一条日志的下一个索引
						DPrintf("[%d] 初始化所有 nextIndex 为 [%d]", rf.me, len(rf.logs))
						for i := range rf.peers {
							rf.nextIndex[i] = len(rf.logs)
						}
						rf.matchIndex[rf.me] = len(rf.logs) - 1
						// Leaders: Upon election: send initial emptyAppendEntries RPCs (heartbeat) to each server; repeat during idle periods to prevent election timeouts (§5.2)
						go sendAppendEntries(rf)
					}
				}
			}(i, term, lastLogIndex, lastLogTerm)
		}
		// Candidates (§5.2): If election timeout elapses: start new election
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[%d] 在任期: [%d] 收到 [%d] 的心跳 [%+v]", rf.me, rf.currentTerm, args.LeaderId, args)
	reply.Term = rf.currentTerm
	// Receiver implementation: #1 Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}
	defer rf.persist()
	/* restart timer */
	rf.restartElectionTimer()
	if args.Term > rf.currentTerm {
		DPrintf("[%d] 任期 [%d] -> [%d] 状态: [%v] -> [%v]", rf.me, rf.currentTerm, args.Term, rf.state, FOLLOWER)
		// All Servers: If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
		rf.currentTerm = args.Term
		// Candidates (§5.2): If AppendEntries RPC received from new leader: convert to follower
		rf.state = FOLLOWER
		rf.votedFor = -1
	} else { // args.Term = rf.currentTerm
		if rf.state == LEADER {
			log.Fatal("rf.state == LEADER")
		}
		/**
		另一个 server 被确定为 leader。在等待投票的过程中，candidate 可能收到来自其他
		server 的 AppendEntries RPC，声明它才是 leader。如果 RPC 中的 term 大于等于
		candidate的current term，candidate就会认为这个leader是合法的并转为follower
		状态。如果 RPC 中的 term 比自己当前的小，将会拒绝这个请求并保持 candidate 状态。
		*/
		if rf.state == CANDIDATE {
			// Candidates (§5.2): If AppendEntries RPC received from new leader: convert to follower
			rf.state = FOLLOWER
		}
	}
	/* Receiver implementation: #2 - #5 */
	logLen := len(rf.logs)
	prevIndex := args.PrevLogIndex
	// Receiver implementation: #2 Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	if logLen-1 < prevIndex {
		reply.XTerm = -1
		reply.LastLogIndex = logLen - 1
		reply.Success = false
		return
	}
	if rf.logs[prevIndex].Term != args.PrevLogTerm {
		reply.XTerm = rf.logs[prevIndex].Term
		reply.XIndex = prevIndex
		for rf.logs[reply.XIndex-1].Term == rf.logs[prevIndex].Term {
			reply.XIndex--
		}
		reply.Success = false
		return
	}
	reply.Success = true
	// 如果跟随者有领导者发送的所有条目，跟随者必须不截断其日志。
	// 必须保留领导发送的条目后面的任何元素。这是因为我们可能从领导者那里接收到一个过时的 AppendEntry RPC，
	// 截断日志将意味着“收回”我们可能已经告诉领导者我们的日志中的条目。
	// Receiver implementation: #3 If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it (§5.3)
	DPrintf("[%d] 变化前长度: [%d], logs: [%v] ", rf.me, len(rf.logs), rf.logs)
	j := 0
	for i := prevIndex + 1; i < logLen; i++ {
		// BUG if rf.logs[i].Term != args.Entries[j].Term {
		if j >= len(args.Entries) || rf.logs[i].Term != args.Entries[j].Term {
			// BUG rf.logs = rf.logs[:i-1]
			rf.logs = rf.logs[:i]
			break
		}
		j++
	}
	// Receiver implementation: #4 Append any new entries not already in the log
	if j != len(args.Entries) {
		rf.logs = append(rf.logs, args.Entries[j:]...)
		DPrintf("[%d] 变化后长度: [%d], logs: [%v] ", rf.me, len(rf.logs), rf.logs)
	}
	// Receiver implementation: #5 If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit > len(rf.logs)-1 {
			DPrintf("[%d] commitIndex: [%d] -> [%d]", rf.me, rf.commitIndex, len(rf.logs)-1)
			rf.commitIndex = len(rf.logs) - 1
		} else {
			DPrintf("[%d] commitIndex: [%d] -> [%d]", rf.me, rf.commitIndex, args.LeaderCommit)
			rf.commitIndex = args.LeaderCommit
		}
		rf.cond.Broadcast()
	}
}

func sendAppendEntries(rf *Raft) {
	for !rf.killed() {
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
				prevIndex := rf.nextIndex[x] - 1
				prevTerm := rf.logs[prevIndex].Term
				entries := make([]Log, len(rf.logs)-1-(prevIndex+1)+1)
				copy(entries, rf.logs[prevIndex+1:]) // ?
				args := AppendEntriesArgs{
					Term:         oldTerm,
					LeaderId:     rf.me,
					PrevLogIndex: prevIndex,
					PrevLogTerm:  prevTerm,
					Entries:      entries,
					LeaderCommit: rf.commitIndex,
				}
				reply := AppendEntriesReply{}
				DPrintf("[%d] 在任期: [%d] 发给 [%d], Entries: [%+v]", rf.me, oldTerm, x, args)
				rf.mu.Unlock()
				// --- BUG 判断 RPC是否成功,不成功直接返回,否则后面会出错
				if !rf.peers[x].Call("Raft.AppendEntries", &args, &reply) {
					return
				}
				// ---
				rf.mu.Lock()
				defer rf.mu.Unlock()
				DPrintf("[%d] 在任期: [%d] 收到 [%d] 回复 [%+v]", rf.me, rf.currentTerm, x, reply)
				// All Servers: If RPC request or response contains term T > currentTerm:
				// set currentTerm = T, convert to follower (§5.1)
				if reply.Term > rf.currentTerm {
					DPrintf("[%d] 任期 [%d] -> [%d], 状态 [%v] -> [%v]", rf.me, rf.currentTerm, reply.Term, rf.state, FOLLOWER)
					rf.currentTerm = reply.Term
					rf.state = FOLLOWER
					rf.votedFor = -1
					rf.persist()
					rf.restartElectionTimer()
					// 领导者发送了一个 AppendEntries RPC，但RPC被拒绝了，
					// 但不是因为日志不一致（这只有在我们的任期结束时才会发生,由于新的领导者产生了?????）
					// 那么你应该立即辞职下台，而不是更新 nextIndex。
					// 如果你这样做，如果你立即连任，你可能与 nextIndex 的重置产生竞争。
					return
				}
				/**
				term混淆是指服务器被来自旧term的 RPC 混淆。
				通常，在收到 RPC 时这不是问题，因为图 2 中的规则准确说明了当您看到旧term时应该做什么。
				但是，图 2 通常不会讨论当您收到旧的 RPC 回复时应该做什么。
				根据经验，我们发现到目前为止最简单的做法是先记录回复中的term（它可能高于您当前的term），
				然后将当前term与您在原始 RPC 中发送的term进行比较，如果两者不同，则丢弃回复并返回。
				*/
				if oldTerm != rf.currentTerm {
					return
				}
				if reply.Success {
					/**
					假设在发送 RPC 和收到回复之间您的状态没有改变。
					一个很好的例子是当您收到对 RPC 的响应时设置 matchIndex = nextIndex - 1 或 matchIndex = len(log)
					这是不安全的，因为自从您发送 RPC 后，这两个值都可能已更新。
					相反，正确的做法是根据您最初在 RPC 中发送的参数将 matchIndex 更新为 prevLogIndex + len(entries[])
					*/
					// Leaders: If successful: update nextIndex and matchIndex for follower (§5.3)
					DPrintf("[%d] nextIndex[%d]: [%d](RPC) -> [%d](rf) -> [%d],"+
						"matchIndex[%d]: [%d](RPC) ->  [%d](rf) -> [%d]",
						rf.me,
						x, prevIndex+1, rf.nextIndex[x], prevIndex+len(entries)+1,
						x, oldMatchIndex, rf.matchIndex[x], prevIndex+len(entries))
					rf.matchIndex[x] = prevIndex + len(entries)
					rf.nextIndex[x] = rf.matchIndex[x] + 1
					// Leaders: If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N,
					// and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4)
					var indexes []int
					for _, v := range rf.matchIndex {
						indexes = append(indexes, v)
					}
					DPrintf("排序前indexes: [%v]", indexes)
					sort.Ints(indexes)
					DPrintf("排序后indexes: [%v]", indexes)
					if indexes[rf.numPeer/2] > rf.commitIndex {
						DPrintf("[%d] commitIndex: [%d] -> [%d] ", rf.me, rf.commitIndex, indexes[rf.numPeer/2])
						rf.commitIndex = indexes[rf.numPeer/2]
						// All Servers: If commitIndex > lastApplied: increment lastApplied,
						// apply log[lastApplied] to state machine (§5.3)
						rf.cond.Broadcast()
					} else {
						DPrintf("[%d] commitIndex保持: [%d]", rf.me, rf.commitIndex)
					}
				} else {
					// Fast Backup
					if reply.XTerm == -1 {
						rf.nextIndex[x] = reply.LastLogIndex + 1
					} else {
						lastIndexOfTerm := -1
						for j := 1; j < len(rf.logs); j++ {
							if rf.logs[j].Term == reply.XTerm {
								lastIndexOfTerm = j
							} else if rf.logs[j].Term > reply.XTerm {
								break
							}
						}
						if lastIndexOfTerm == -1 {
							rf.nextIndex[x] = reply.XIndex
						} else {
							rf.nextIndex[x] = lastIndexOfTerm + 1
						}
					}
					// Leaders: If AppendEntries fails because of log inconsistency: decrement nextIndex and retry (§5.3)
					DPrintf("[%d] nextIndex[%d]: [%d] -> [%d]", rf.me, x, rf.nextIndex[x]+1, rf.nextIndex[x])
					if rf.nextIndex[x] < 1 {
						DPrintf("[%d] nextIndex[%d] < 1", rf.me, x)
						log.Fatal(" rf.nextIndex[x] < 1 ")
					}
				}
			}(i)
		}
		// The tester requires that the leader send heartbeat RPCs
		// no more than ten times per second.
		time.Sleep(100 * time.Millisecond)
	}
}
