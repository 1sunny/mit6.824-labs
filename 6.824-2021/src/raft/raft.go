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
		var logger = log.New(os.Stdout, fmt.Sprintf("%s [%d:%v:%d:%d] ", util.GetTimeBuf(), rf.me, rf.state, rf.currentTerm, rf.commitIndex), 0)
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
	return fmt.Sprintf("%d:%v ", p.CommandIndex, p.Command)
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
	HeartBeatTimeOut            = 50
	ElectionCheckInterval       = 20
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
	randDuration int
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
	rf.debug("持久数据 currentTerm: [%d], votedFor: [%d], 日志长度:[%d]", rf.currentTerm, rf.votedFor, len(rf.logs))
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
		log.Printf("读取currentTerm失败: [%v]", err)
		return
	}
	if err := d.Decode(&votedFor); err != nil {
		log.Printf("读取votedFor失败: [%v]", err)
		return
	}
	if err := d.Decode(&logs); err != nil {
		log.Printf("读取logs失败: [%v]", err)
		return
	}
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.logs = logs
	rf.debug("读取数据 currentTerm: [%d], votedFor: [%d], logs: [%v]", rf.currentTerm, rf.votedFor, rf.logs)
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
	rf.debug("初始化所有 nextIndex 为 [%d]", len(rf.logs))
	for i := range rf.peers {
		rf.nextIndex[i] = len(rf.logs) - 1 // 因为最新的 NoOp其它节点肯定没有,所以-1
	}
	rf.matchIndex[rf.me] = len(rf.logs) - 1
}

func (rf *Raft) isLogUpToDateThanMe(lastLogTerm, lastLogIndex int) bool {
	lastIdx := len(rf.logs) - 1
	// fix BUG: add args.LastLogTerm == rf.logs[lastIdx].Term
	return lastLogTerm > rf.logs[lastIdx].Term || (lastLogTerm == rf.logs[lastIdx].Term && lastLogIndex >= lastIdx)
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
	rf.debug("%v 票给 [%d] 任期 [%d]", reply.VoteGranted, args.CandidateId, args.Term)
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
	term := rf.currentTerm
	index := len(rf.logs)
	rf.matchIndex[rf.me] = len(rf.logs)
	// Leaders: If command received from client: append entry to local log, respond after entry applied to state machine (§5.3)
	rf.logs = append(rf.logs, Log{
		Term: term,
		Cmd:  command,
	})
	rf.debug("*** 在任期 [%d] 收到命令index:[%d], 日志长度:[%d], 所有日志: [%v]", rf.currentTerm, index, len(rf.logs), rf.logs)
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
	rf.debug("下线")
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

	}
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
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		mu:           sync.Mutex{},
		peers:        peers,
		persister:    persister,
		me:           me,
		dead:         0,
		numPeer:      len(peers),
		cond:         nil,
		currentTerm:  0,
		votedFor:     -1,
		state:        FOLLOWER,
		lastBeatTime: time.Now(),
		randDuration: 0,
		logs:         []Log{{Term: 0, Cmd: 0}},
		commitIndex:  0,
		lastApplied:  0,
		nextIndex:    make([]int, len(peers)),
		matchIndex:   make([]int, len(peers)),
		applyCh:      applyCh,
	}
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

func (rf *Raft) applyLogs() {
	rf.cond = sync.NewCond(&rf.mu)
	for !rf.killed() {
		rf.mu.Lock()
		// 由于 Wait 第一次恢复时 c.L 并未锁定，因此调用者一般不能假定 Wait 返回时条件为真。取而代之，调用者应当把 Wait 放入循环中：
		for rf.lastApplied == rf.commitIndex {
			rf.cond.Wait() // 调用时释放锁，被唤醒后获取锁
		}
		rf.lastApplied++
		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.logs[rf.lastApplied].Cmd,
			CommandIndex: rf.lastApplied,
			Term:         rf.logs[rf.lastApplied].Term,
		}
		rf.debug("提交 [%v]", msg)
		rf.mu.Unlock() // fix BUG: 先释放锁,applyCh可能会阻塞 ?
		rf.applyCh <- msg
	}
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
				LastLogIndex: len(rf.logs) - 1,
				LastLogTerm:  rf.logs[len(rf.logs)-1].Term,
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
			rf.debug("在任期 [%d] 收到 [%d] 回复: {%+v}", oldTerm, x, reply)
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
			rf.debug("在任期: [%d] 得到了 [%d] 的投票", oldTerm, x)
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
			time.Sleep(ElectionCheckInterval * time.Millisecond)
			rf.mu.Lock()
		}
		rf.toCandidate()
		rf.mu.Unlock()
		// Candidates (§5.2): If election timeout elapses: start new election
	}
}

func (rf *Raft) receiveLogs(args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	/* Receiver implementation: #2 - #5 */
	prevIndex := args.PrevLogIndex
	// Receiver implementation: #2 Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	if len(rf.logs)-1 < prevIndex {
		reply.XTerm = -1
		reply.LastLogIndex = len(rf.logs) - 1
		reply.Success = false
		return false
	}
	if rf.logs[prevIndex].Term != args.PrevLogTerm {
		reply.XTerm = rf.logs[prevIndex].Term
		reply.FirstIndexOfXTerm = prevIndex
		for rf.logs[reply.FirstIndexOfXTerm-1].Term == rf.logs[prevIndex].Term {
			reply.FirstIndexOfXTerm--
			if reply.FirstIndexOfXTerm <= 0 {
				log.Fatalf("reply.FirstIndexOfXTerm <= 0")
			}
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
	rf.debug("变化前长度: [%d]", len(rf.logs))
	j := 0
	for i := prevIndex + 1; i < len(rf.logs); i++ {
		if j >= len(args.Entries) { // fix BUG
			break
		}
		// fix BUG: if rf.logs[i].Term != args.Entries[j].Term {
		// fix BUG: if j >= len(args.Entries || rf.logs[i].Term != args.Entries[j].Term {
		if rf.logs[i].Term != args.Entries[j].Term {
			rf.logs = rf.logs[:i] // fix BUG rf.logs = rf.logs[:i-1]
			break
		}
		j++
	}
	// Receiver implementation: #4 Append any new entries not already in the log
	if j != len(args.Entries) {
		rf.logs = append(rf.logs, args.Entries[j:]...)
	}
	rf.debug("变化后长度: [%d]", len(rf.logs))
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
		if rf.commitIndex < oldCommitIndex { // commitIndex 减小
			log.Fatalf("commitIndex: [%d] -> [%d]", oldCommitIndex, rf.commitIndex)
		}
		rf.debug("commitIndex: [%d] -> [%d]", oldCommitIndex, rf.commitIndex)
		rf.cond.Broadcast()
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
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
	/* restart timer */
	rf.restartElectionTimer()
	if args.Term > rf.currentTerm {
		// All Servers: If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
		rf.toFollower(args.Term)
	} else { // args.Term = rf.currentTerm
		if rf.state == LEADER {
			log.Fatal("rf.state == LEADER")
		}
		/* 另一个 server 被确定为 leader。在等待投票的过程中，candidate 可能收到来自其他 server 的 AppendEntries RPC，声明它才是 leader。如果 RPC 中的 term 大于等于candidate的current term，candidate就会认为这个leader是合法的并转为follower状态。如果 RPC 中的 term 比自己当前的小，将会拒绝这个请求并保持 candidate 状态。*/
		if rf.state == CANDIDATE {
			// Candidates (§5.2): If AppendEntries RPC received from new leader: convert to follower
			rf.toFollower(args.Term)
		}
	}
	if rf.receiveLogs(args, reply) == false {
		return
	}
	rf.updateCommitIndex(args)
}

func (rf *Raft) majorityMatchIndex() int {
	indexes := make([]int, rf.numPeer)
	copy(indexes, rf.matchIndex)
	sort.Ints(indexes)
	return indexes[rf.numPeer/2]
}

func (rf *Raft) fastBackUp(peer int, reply *AppendEntriesReply) {
	oldNextIndex := rf.nextIndex[peer]
	if reply.XTerm == -1 {
		rf.nextIndex[peer] = reply.LastLogIndex + 1
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
			rf.nextIndex[peer] = reply.FirstIndexOfXTerm
		} else {
			rf.nextIndex[peer] = lastIndexOfTerm + 1
		}
	}
	rf.debug("nextIndex[%d]: [%d] -> [%d]", peer, oldNextIndex, rf.nextIndex[peer])
	if rf.nextIndex[peer] < 1 {
		log.Fatal(" rf.nextIndex[x] < 1 ")
	}
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
			prevIndex := rf.nextIndex[x] - 1
			prevTerm := rf.logs[prevIndex].Term
			entries := make([]Log, len(rf.logs)-1-(prevIndex+1)+1)
			copy(entries, rf.logs[prevIndex+1:])
			args := AppendEntriesArgs{
				Term:         oldTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevIndex,
				PrevLogTerm:  prevTerm,
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
				rf.debug("nextIndex[%d]: [%d](RPC) -> [%d](rf) -> [%d],"+
					"matchIndex[%d]: [%d](RPC) ->  [%d](rf) -> [%d]",
					x, prevIndex+1, rf.nextIndex[x], prevIndex+len(entries)+1,
					x, oldMatchIndex, rf.matchIndex[x], prevIndex+len(entries))
				rf.matchIndex[x] = prevIndex + len(entries)
				rf.nextIndex[x] = rf.matchIndex[x] + 1

				// Leaders: If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N,
				// and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4)
				N := rf.majorityMatchIndex()
				oldCommitIndex := rf.commitIndex
				if N > rf.commitIndex && rf.logs[N].Term == rf.currentTerm { // fix BUG: if indexes[rf.numPeer/2] > rf.commitIndex {
					rf.commitIndex = N
					rf.cond.Broadcast()
				}
				rf.debug("commitIndex: [%d] -> [%d] ", oldCommitIndex, rf.commitIndex)
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
		time.Sleep(HeartBeatTimeOut * time.Millisecond)
		rf.mu.Lock()
		if rf.state == LEADER {
			rf.appendEntries()
		}
		rf.mu.Unlock() // fix BUG: 调用 appendEntries 将协程打开后再释放锁,否则锁可能被选举定时器拿走
	}
}
