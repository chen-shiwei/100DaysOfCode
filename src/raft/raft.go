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
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)
import "labrpc"

// import "bytes"
// import "labgob"

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

func (rf *Raft) BecomeTo(state State) {

	if rf.state == state {
		return
	}
	rf.state = state
	switch state {
	case Follower:
		rf.Log("切换身份为:%s", state.String())
		rf.heartbeat.Stop()
		rf.election.Reset(electionDuration())
		rf.votedFor = -1
	case Leader:
		rf.Log("切换身份为:%s", state.String())
		rf.election.Stop()
		rf.broadcastHeartbeat()
		rf.heartbeat.Reset(HeartbeatInterval)
	case Candidate:
		rf.Log("切换身份为:%s", state.String())
		rf.startElection()
	}

}

type State int

func (s State) String() string {
	switch s {
	case Leader:
		return "Leader"
	case Candidate:
		return "Candidate"
	case Follower:
		return "Follower"
	default:
		return ""
	}
}

const (
	_ State = iota
	Leader
	Follower
	Candidate
)

var (
	HeartbeatInterval = time.Millisecond * 120
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	done chan struct{}

	heartbeat *time.Timer
	election  *time.Timer
	state     State
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// 所有服务器上持久存在的
	currentTerm int // 服务器最后一次知道的任期号（初始化为 0，持续递增）
	votedFor    int // 在当前获得选票的候选人的 Id
	logs        []struct {
		term int // 接受到的任期号
		//entries
	} // 日志条目集；每一个条目包含一个用户状态机执行的指令，和收到时的任期号
	// 所有服务器上经常变的
	commitIndex int // 已知的最大的已经被提交的日志条目的索引值
	lastApplied int // 最后被应用到状态机的日志条目索引值（初始化为 0，持续递增）
	// 在领导人里经常改变的 （选举后重新初始化）
	//nextIndex[] / /对于每一个服务器，需要发送给他的下一个日志条目的索引值（初始化为领导人最后索引值加一）
	//matchIndex[] // 对于每一个服务器，已经复制给他的日志的最高索引值
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool

	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.state == Leader
	return term, isleader
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
	Term         int //	候选人的任期号
	CandidateId  int //	请求选票的候选人的 Id
	LastLogIndex int //	候选人的最后日志条目的索引值
	LastLogTerm  int //	候选人最后日志条目的任期号
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // 当前任期号，以便于候选人去更新自己的任期号
	VoteGranted bool // 候选人赢得了此张选票时为真
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2A, 2B).
	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && rf.votedFor != -1) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		rf.Log("拒绝投票给%d", args.CandidateId)
		return
	}

	rf.Log("投票给server[%d]", args.CandidateId)

	reply.VoteGranted = true
	rf.votedFor = args.CandidateId
	rf.currentTerm = args.Term

	rf.election.Reset(electionDuration())
	rf.BecomeTo(Follower)

	return
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

type AppendEntriesArgs struct {
	Term         int   // 领导人的任期号
	LeaderId     int   // 领导人的 Id，以便于跟随者重定向请求
	PrevLogIndex int   // 新的日志条目紧随之前的索引值
	PrevLogTerm  int   // PrevLogIndex 条目的任期号
	Entries      []Log // 	准备存储的日志条目（表示心跳时为空；一次性发送多个是为了提高效率）
	LeaderCommit int   // 领导人已经提交的日志的索引值
}
type AppendEntriesReply struct {
	Term    int  //	当前的任期号，用于领导人去更新自己
	Success bool //	跟随者包含了匹配上 PrevLogIndex 和 PrevLogTerm 的日志时为真
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 接收者实现：

	// 如果 Term < currentTerm 就返回 false （5.1 节）
	// todo
	// 如果日志在 PrevLogIndex 位置处的日志条目的任期号和 PrevLogTerm 不匹配，则返回 false （5.3 节）
	// 如果已经存在的日志条目和新的产生冲突（索引值相同但是任期号不同），删除这一条和之后所有的 （5.3 节）
	// 附加日志中尚未存在的任何新条目
	// 如果 LeaderCommit > commitIndex，令 commitIndex 等于 LeaderCommit 和 新日志条目索引值中较小的一个
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		rf.Log("收到server[%d]心跳请求,它的任期号%d当前任期号", args.LeaderId, args.Term)
		return
	}

	t := electionDuration()
	rf.Log("收到server[%d]心跳请求 (%d/%d) 获取的选举为%s", args.LeaderId, args.Term, rf.currentTerm, t.String())
	reply.Success = true
	rf.currentTerm = args.Term
	rf.election.Reset(t)
	rf.BecomeTo(Follower)

	//rf.timer.Reset(time.)
	//if args.PrevLogIndex !=  {
	//
	//}

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// should be called with lock
func (rf *Raft) broadcastHeartbeat() {
	args := AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
	}

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {

			var reply AppendEntriesReply
			if rf.sendAppendEntries(server, &args, &reply) {
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.BecomeTo(Follower)
				}
				rf.mu.Unlock()
			} else {
				rf.mu.Lock()
				rf.Log("send request vote to %d failed", server)
				rf.mu.Unlock()

			}
		}(i)
	}
}

// startElection 开始选举
func (rf *Raft) startElection() {
	// 自增选举届数
	rf.Log("开始新Term+1选举")

	rf.currentTerm += 1
	// 启动选举计时
	rf.election.Reset(electionDuration())

	var (
		receiveVoteCount int32
		args             = RequestVoteArgs{
			Term:        rf.currentTerm,
			CandidateId: rf.me,
		}
	)
	// 开始请求投票
	for i := range rf.peers {
		// 为自己投票
		if i == rf.me {
			rf.votedFor = rf.me
			atomic.AddInt32(&receiveVoteCount, 1)
			continue
		}
		go func(serverID int) {

			reply := RequestVoteReply{}
			if rf.sendRequestVote(serverID, &args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				// 收到投票 并且自己还是Candidate
				if reply.VoteGranted && rf.state == Candidate {
					atomic.AddInt32(&receiveVoteCount, 1)
					rf.Log("目前收到选举票数 %d/%d", atomic.LoadInt32(&receiveVoteCount), len(rf.peers))
					// 收到的选票数超过一半
					if atomic.LoadInt32(&receiveVoteCount) > int32(len(rf.peers)/2) {
						rf.BecomeTo(Leader)
						rf.Log("选举成功 %d/%d，成为 Leader", atomic.LoadInt32(&receiveVoteCount), len(rf.peers))
					}
				} else {
					// 如果任期号小于对方，成为跟随者
					if reply.Term > rf.currentTerm {
						//rf.mu.Lock()
						rf.currentTerm = reply.Term
						//rf.mu.Unlock()
						rf.Log("任期号大于 server[%d] 成为对方跟随者", serverID)
						rf.BecomeTo(Follower)
					}
				}
			} else {
				rf.mu.Lock()
				rf.Log("send server[%d] req vote fail", serverID)
				rf.mu.Unlock()
			}

		}(i)
	}

}

type Log struct {
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
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.election.Stop()
	rf.heartbeat.Stop()
	rf.done <- struct{}{}
	rf.Log("退出 success")
	rf = &Raft{}

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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.done = make(chan struct{})

	rf.currentTerm = 0
	rf.heartbeat = time.NewTimer(HeartbeatInterval) // 初始化leader心跳计时
	rf.election = time.NewTimer(electionDuration()) // 初始化选举计时
	rf.votedFor = -1
	rf.state = Follower
	// 测试人员要求领导者每秒发送心跳RPC的次数不超过十次

	rf.Log("启动服务 已有服务数量:%d", len(peers))

	// Your initialization code here (2A, 2B, 2C).

	go func() {
		for {
			select {
			case <-rf.election.C:
				if rf.state == Follower {
					rf.mu.Lock()
					rf.Log("心跳超时，进入选举")
					rf.BecomeTo(Candidate)
					rf.mu.Unlock()
				} else {
					rf.mu.Lock()
					rf.startElection()
					rf.mu.Unlock()
				}

			case <-rf.heartbeat.C:
				rf.mu.Lock()
				if rf.state == Leader {
					rf.broadcastHeartbeat()
					rf.heartbeat.Reset(HeartbeatInterval)
				}
				rf.mu.Unlock()
			case <-rf.done:
				rf.Log("退出 timer")
			}
		}
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func (rf *Raft) Log(format string, a ...interface{}) {
	pre := fmt.Sprintf("Term[%d],Server[%d %s]  ", rf.currentTerm, rf.me, rf.state.String())
	DPrintf(pre+format, a...)
}

// electionDuration return 选举超时范围为150到300毫秒
func electionDuration() time.Duration {
	source := rand.NewSource(time.Now().UnixNano())
	return time.Duration(rand.New(source).Intn(150)+150) * time.Millisecond
}
