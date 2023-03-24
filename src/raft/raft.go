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
	"log"
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

// import "bytes"
// import "../labgob"

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
type State string

const (
	FOLLOWER  State = "Follower"
	CANDIDATE State = "Candidate"
	LEADER    State = "Leader"
)

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
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm  int // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor     int // candidateId that received vote in current term (or null if none)
	state        State
	lastBeatTime time.Time
	// log
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
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
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
	Term         int // leader’s term
	LeaderId     int // so follower can redirect clients
	PrevLogIndex int // index of log entry immediately preceding new ones
	PrevLogTerm  int // term of PrevLogIndex entry
	LeaderCommit int // leader’s commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching PrevLogIndex and PrevLogTerm
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
	// Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}
	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	if args.Term > rf.currentTerm {
		DPrintf("[%d] 状态 [%v] -> [%v], 任期 [%d] -> [%d]", rf.me, rf.state, FOLLOWER, rf.currentTerm, args.Term)
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		// !!!
		rf.votedFor = -1
	}
	// If votedFor is null or candidateId, TODO and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	if rf.votedFor == -1 {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		// 投票后重置定时器
		rf.lastBeatTime = time.Now()
		DPrintf("[%d] 投票给 [%d] 任期 [%d]", rf.me, args.CandidateId, args.Term)
	} else {
		// ...
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
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
	DPrintf("[%d] 下线", rf.me)
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
	rf.currentTerm = 0
	rf.state = FOLLOWER
	rf.lastBeatTime = time.Now()
	rf.votedFor = -1
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go startElection(rf)
	return rf
}

func getRandomNum(a, b int) int {
	rand.Seed(time.Now().UnixNano())
	return a + rand.Intn(b-a+1)
}

func startElection(rf *Raft) {
	for !rf.killed() {
		rf.mu.Lock()
		DPrintf("[%d] state: [%v], term: [%d]", rf.me, rf.state, rf.currentTerm)
		if rf.state == LEADER {
			rf.mu.Unlock()
			return
		}
		// 等待选举计时器超时
		timeout := time.Duration(getRandomNum(150, 300))
		for time.Since(rf.lastBeatTime) < timeout*time.Millisecond {
			rf.mu.Unlock()
			time.Sleep(30 * time.Millisecond)
			rf.mu.Lock()
		}
		// sleep后已经成为 leader
		if rf.state == LEADER {
			rf.mu.Unlock()
			return
		}
		DPrintf("[%d] 选举计时器超时", rf.me)
		/* 可以直接变为 candidate, 如果sleep时接收到心跳,那么会重置 lastBeatTime,不会走到这里,
		如果刚要接收到心跳就走到这里,那么也没有关系,currentTerm没有被更新,只是发送投票请求得不到票而已
		*/
		// Candidates (§5.2):
		rf.state = CANDIDATE
		DPrintf("[%d] state: [%v], term: [%d]", rf.me, rf.state, rf.currentTerm)
		// Increment currentTerm, 这里currentTerm会不会被改变过了,由于其它节点先成为leader并发送心跳给当前节点
		rf.currentTerm++
		// Vote for self
		rf.votedFor = rf.me // !
		total := len(rf.peers)
		count := 1
		DPrintf("[%d] 在任期 [%d] 开始竞选", rf.me, rf.currentTerm)
		// Reset election timer
		rf.lastBeatTime = time.Now()
		// Send RequestVote RPCs to all other servers
		term := rf.currentTerm
		rf.mu.Unlock()

		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go func(x int, t int) {
				args := RequestVoteArgs{
					Term:         t,
					CandidateId:  rf.me,
					LastLogIndex: 0,
					LastLogTerm:  0,
				}
				reply := RequestVoteReply{}
				rf.sendRequestVote(x, &args, &reply)

				rf.mu.Lock()
				defer rf.mu.Unlock()
				// If RPC request or response contains term T > currentTerm:
				// set currentTerm = T, convert to follower (§5.1)
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.state = FOLLOWER
					rf.votedFor = -1
					DPrintf("[%d] 任期 [%d] -> [%d]", rf.me, rf.currentTerm, reply.Term)
					return
				}

				if !reply.VoteGranted {
					return
				}
				DPrintf("[%d] 得到了 [%d] 在任期 [%d] 的投票", rf.me, x, t)
				count++
				if count > total/2 {
					// 如果之前被变为 follower,则竞选失败,因为任期现在可能已经变化了
					if rf.state == CANDIDATE && rf.currentTerm == t {
						DPrintf("[%d] 成为任期 [%d] 的Leader", rf.me, t)
						rf.state = LEADER
						go sendAppendEntries(rf)
					}
				}
			}(i, term)
		}
		// If election timeout elapses: start new election
		DPrintf("startElection end")
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//DPrintf("in AppendEntries")
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[%d] 收到了 [%d] 在任期 [%d] 的心跳", rf.me, args.LeaderId, args.Term)
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}
	if args.Term > rf.currentTerm {
		DPrintf("[%d] 任期 [%d] -> [%d] 状态: [%v] -> [%v]", rf.me, rf.currentTerm, args.Term, rf.state, FOLLOWER)
		// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
		rf.currentTerm = args.Term
		// Candidates (§5.2): If AppendEntries RPC received from new leader: convert to follower
		rf.state = FOLLOWER
		rf.votedFor = -1
	} else {
		if rf.state == LEADER {
			// 应该需要比较日志
			log.Fatal("rf.state == LEADER")
		}
		if rf.state == CANDIDATE {
			// Candidates (§5.2): If AppendEntries RPC received from new leader: convert to follower
			rf.state = FOLLOWER
		}
	}
	/* restart timer */
	rf.lastBeatTime = time.Now()
	// TODO 2-5
}

func sendAppendEntries(rf *Raft) {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state != LEADER {
			rf.mu.Unlock()
			return
		}
		term := rf.currentTerm
		rf.mu.Unlock()
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go func(x int, t int) {
				args := AppendEntriesArgs{
					Term:         t,
					LeaderId:     rf.me,
					PrevLogIndex: 0,
					PrevLogTerm:  0,
					LeaderCommit: 0,
				}
				reply := AppendEntriesReply{}
				rf.peers[x].Call("Raft.AppendEntries", &args, &reply)
			}(i, term)
		}
		// The tester requires that the leader send heartbeat RPCs
		// no more than ten times per second.
		time.Sleep(100 * time.Millisecond)
	}
}
