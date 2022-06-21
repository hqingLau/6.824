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
	//	"bytes"
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
)

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

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//

// =====================================================
// 测试完之后再电脑上测试下：
// warning: only one CPU, which may conceal locking bugs

const (
	StateLeader int32 = iota
	StateFollower
	StateCandidate
)

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int32               // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 2A
	recvHeartBeat int32      // 如果收到心跳为1，与election timeout配置
	currentTerm   int64      // currentTerm of log
	state         int32      // state: follower, :candidate, :Leader
	logs          []LogEntry //日志
	voteFor       int32      // 是否已经投票,没投为-1

	// 2B
	applyCh     chan ApplyMsg
	commitIdx   int32 //已经commit的最新log index, 用于判断发送给applyCh
	lastApplyed int32 // 已经应用的最新的，2b这里就是最新发给applych的
	// 卧槽，加上这里整个2B结构就变了
	nextIndex  []int // other peer next log sync Index
	matchIndex []int // other peer log already match
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool

	rf.mu.Lock()
	term = int(rf.currentTerm)
	isleader = (rf.state == StateLeader)
	rf.mu.Unlock()
	// Your code here (2A).
	// fmt.Printf("%v: leader %v\n", rf.me, isleader)
	return term, isleader
}

func (rf *Raft) Commit(commitIdx *int, reply *int32) {
	fmt.Printf("rf.me %v commit %v\n", rf.me, *commitIdx)
	rf.mu.Lock()
	idx := *commitIdx
	curid := idx
	for curid > 0 && rf.logs[curid-1].Committed == 0 {
		curid--
	}
	rf.mu.Unlock()
	// fmt.Printf("curid: %v,idx: %v\n", curid, idx)
	for i := curid; i <= idx; i++ {
		if i < 1 {
			continue
		}
		rf.mu.Lock()
		rf.logs[i-1].Committed = 1
		fmt.Printf("rf.me %v logs %v\n", rf.me, rf.logs)
		rf.commitIdx = int32(max(int(rf.commitIdx), i))
		rf.mu.Unlock()

	}
	rf.persist()
	atomic.StoreInt32(reply, 1)
}

func (rf *Raft) sendApplyCh() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.lastApplyed < rf.commitIdx {
			msg := ApplyMsg{CommandValid: true, Command: rf.logs[rf.lastApplyed].Command, CommandIndex: int(rf.lastApplyed + 1)}
			rf.mu.Unlock()
			rf.applyCh <- msg

			fmt.Printf("rf.me %v: sendChan: %v\n", rf.me, msg)
			rf.mu.Lock()
			rf.lastApplyed++
		} else {
			rf.mu.Unlock()
			time.Sleep(time.Millisecond * 50)
			rf.mu.Lock()
		}
		rf.mu.Unlock()
	}
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.logs)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	rf.persister.SaveRaftState(w.Bytes())
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var logs []LogEntry
	var currentTerm int64
	var voteFor int32

	if d.Decode(&logs) != nil || d.Decode(&currentTerm) != nil ||
		d.Decode(&voteFor) != nil {
		fmt.Errorf("err:decode from persistor")
	} else {
		rf.mu.Lock()
		rf.currentTerm = currentTerm
		rf.logs = logs
		rf.voteFor = voteFor
		rf.mu.Unlock()
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

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	CurrentTerm int64 // currentTerm of log
	LastLogTerm int64 // leader election restriction: candidate last log entry should greater local.
	LogLen      int   // if lastLogTerm equals, compare logLen, candidate >= local
	Me          int32 // idx in peers
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Ok int32 // 0:vote fail,1:vote success
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// 别人向自己请求投票，要查看自己的状态，是否可以投票，判断
	// 它的term和长度
	// candidate才能投票，而candidate要由follower超时转换而成
	// curState := atomic.LoadInt32(&rf.state)
	reply.Ok = 0
	rf.mu.Lock()
	fmt.Printf("原来%d votefor %d\n", rf.me, rf.voteFor)
	//defer  rf.persist()
	defer rf.mu.Unlock()
	defer rf.persist()

	fmt.Printf("me: %d, argsTerm:%d  rf: %+v\n", args.Me, args.CurrentTerm, rf)
	if rf.state != StateCandidate || args.CurrentTerm <= rf.currentTerm {
		// fmt.Printf("---- false%d:%d rf.voteFor: %v term:%v\n", rf.me, rf.currentTerm, rf.voteFor, args.CurrentTerm)
		return
	}
	lastLogTerm := int64(0)
	if len(rf.logs) > 0 {
		lastLogTerm = rf.logs[len(rf.logs)-1].Term
	}
	if args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LogLen >= len(rf.logs)) {
		if rf.voteFor == -1 {
			rf.voteFor = args.Me
			rf.state = StateFollower // 投票完变回follower，如果选举失败它会超时变回Condidate
			reply.Ok = 1
		}
	}

	if reply.Ok == 1 {
		fmt.Printf("%d rf.voteFor: %v state:%v\n", rf.me, rf.voteFor, rf)
	}

}

type LogEntry struct {
	Command   interface{}
	Term      int64
	Committed int32
}

// *********************** AppendEntries ***********************
type AppendEntriesArgs struct {
	Info         string // extra info
	Term         int64  // leader's term
	Leader       int32  // leader id
	Entries      []LogEntry
	LeaderCommit int64 // leader's commitIndex
	LastLogIdx   int   // 前一条日志的ID
	LastTerm     int   // 前一条日志的Term
}

type AppendEntriesReply struct {
	Ok int32 // 0:fail 1,ok,-1添加不成功，-2 leader 不是它了
}

// args.Term = int64(term)
// args.LastLogIdx = lastLogIdx
// if lastLogIdx >= 1 {
// 	args.LastTerm = int(rf.logs[lastLogIdx-1].Term)
// }
// args.Leader = server
// args.Entries = []LogEntry{}
// args.Entries = append(args.Entries, rf.logs[lastLogIdx+1])

// 只有follower才会调用AppendEntries
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// 只有leader能调用AppendEntries
	// 所以收到之后，就是收到了心跳或者log，重置election timeout
	atomic.StoreInt32(&reply.Ok, 0)
	rf.mu.Lock()
	//
	defer rf.mu.Unlock()
	defer rf.persist()
	defer fmt.Printf("%d（term: %v）: log: %+v\n", rf.me, rf.currentTerm, rf.logs)

	if args.Info == "heart" {
		if rf.currentTerm > args.Term {
			rf.state = StateFollower
			return
		}
		atomic.StoreInt32(&reply.Ok, 1)
		rf.recvHeartBeat = 1
		rf.voteFor = -1
		rf.currentTerm = args.Term

		if rf.me != args.Leader {
			rf.state = StateFollower
		}
		atomic.StoreInt32(&reply.Ok, 1)
		return
	}

	if rf.currentTerm > args.Term {
		// -2 , 不为leader，变回follower
		atomic.StoreInt32(&reply.Ok, -2)
		return
	}

	rf.currentTerm = int64(max(int(rf.currentTerm), int(args.Term)))

	// // leader的term比follower还小，leader失效，返回
	// if args.LastLogIdx < 0 {
	// 	// 此次添加不可能成功，退出
	// 	fmt.Println("=-=-=-=-======================")
	// 	atomic.StoreInt32(&reply.Ok, -1)
	// 	return
	// }
	// fmt.Printf("rf.me: %v, len rf logs: %v, args.LastLogIdx %d\n", rf.me, len(rf.logs), args.LastLogIdx)
	// command entry
	argsLastLogIdx := args.LastLogIdx
	if args.LastLogIdx > len(rf.logs) {
		atomic.StoreInt32(&reply.Ok, -1)
		return
	}

	if argsLastLogIdx > 0 && (rf.logs[args.LastLogIdx-1].Term != int64(args.LastTerm)) {
		atomic.StoreInt32(&reply.Ok, -1)
		return
	}
	for i := 0; i < len(args.Entries); i++ {
		if len(rf.logs) > argsLastLogIdx+i {
			rf.logs[args.LastLogIdx+i].Term = args.Entries[i].Term
			rf.logs[args.LastLogIdx+i].Command = args.Entries[i].Command

		} else {
			rf.logs = append(rf.logs, LogEntry{args.Entries[i].Command, args.Entries[i].Term, 0})

		}
	}
	atomic.StoreInt32(&reply.Ok, 1)

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
// 添加log的接口
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()

	index = len(rf.logs) + 1

	term = int(rf.currentTerm)
	isLeader = (!rf.killed()) && (rf.state == StateLeader)
	rf.mu.Unlock()

	if isLeader {
		// time.Sleep(time.Millisecond*150)
		rf.mu.Lock()
		fmt.Printf("rf.me %d receive command %v\n", rf.me, command)
		// 加上，后台慢慢同步
		if len(rf.logs) > 0 && rf.logs[len(rf.logs)-1].Command == nil {
			rf.logs = rf.logs[:len(rf.logs)-1]
		}
		rf.logs = append(rf.logs, LogEntry{Command: command, Term: int64(term), Committed: 0})
		rf.persist()
		rf.mu.Unlock()
		// rf.persist()
	} else {
		fmt.Printf("nooooooo rf.me %d receive command %v %v\n", rf.me, command, isLeader)
	}

	return index, term, isLeader
}

func min(a, b int) int {
	if a > b {
		return b
	}
	return a
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func (rf *Raft) syncLogs() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state != StateLeader {
			rf.mu.Unlock()
			break
		}
		if len(rf.logs) == int(rf.commitIdx) {
			rf.mu.Unlock()
			time.Sleep(time.Millisecond * 20)
			continue
		}
		// 同步成功的peer列表，先把master加进来
		// nextIndex[i] index of the next log entry to send to i
		// matchIndex[i] index of highest log entry known to be replicate on server
		commitList := []int{int(rf.me)}
		commitListMutex := sync.Mutex{}
		lenlogs := len(rf.logs)
		lenpeers := len(rf.peers)
		for ii := 0; ii < lenpeers; ii++ {
			i := ii
			if int32(i) == rf.me {
				continue
			}

			go func() {

				for {
					rf.mu.Lock()
					if rf.state != StateLeader {
						rf.mu.Unlock()
						return
					}
					if rf.nextIndex[i] > lenlogs+1 {
						rf.mu.Unlock()
						break
					}
					// fmt.Printf("rf.me %v (term:%v), rf.nextIndex: %+v\n", rf.me, rf.currentTerm, rf.nextIndex)
					args := AppendEntriesArgs{}
					reply := AppendEntriesReply{0}
					args.Entries = []LogEntry{}
					for k := rf.nextIndex[i] - 1; k < len(rf.logs); k++ {
						args.Entries = append(args.Entries, rf.logs[k])
					}
					if len(args.Entries) == 0 {
						rf.mu.Unlock()
						break
					}
					args.Leader = rf.me
					args.LeaderCommit = int64(rf.matchIndex[i])
					args.Term = rf.currentTerm
					args.LastLogIdx = rf.nextIndex[i] - 1
					args.LastTerm = 0
					if args.LastLogIdx > 0 {
						args.LastTerm = int(rf.logs[args.LastLogIdx-1].Term)
					}
					rf.mu.Unlock()
					rf.peers[i].Call("Raft.AppendEntries", &args, &reply)
					replyOk := atomic.LoadInt32(&reply.Ok)
					if replyOk == 1 {
						commitListMutex.Lock()
						commitList = append(commitList, i)
						commitListMutex.Unlock()
						rf.mu.Lock()
						rf.nextIndex[i] = lenlogs + 1
						rf.mu.Unlock()
						return
					} else if replyOk == -1 {
						rf.mu.Lock()
						rf.nextIndex[i]--
						rf.mu.Unlock()
					} else if replyOk == -2 {
						rf.mu.Lock()
						rf.state = StateFollower
						rf.mu.Unlock()
						return
					} else {
						return
					}
				}

			}()
		}
		rf.mu.Unlock()

		c := 0
		for {
			randTime := 20
			time.Sleep(time.Millisecond * time.Duration(randTime))
			rf.mu.Lock()
			lencommitList := len(commitList)
			rf.mu.Unlock()
			c++
			if c == 5 || lencommitList > len(rf.peers)/2 {
				break
			}
		}

		commitListMutex.Lock()
		lencommitList := len(commitList)
		commitListMutex.Unlock()
		if lencommitList > len(rf.peers)/2 {
			rf.mu.Lock()
			for _, idd := range commitList {
				commitid := idd
				go func() {
					reply := int32(0)
					matchidx := lenlogs
					rf.peers[commitid].Call("Raft.Commit", &matchidx, &reply)
					if atomic.LoadInt32(&reply) == 1 {
						rf.mu.Lock()
						rf.matchIndex[commitid] = max(lenlogs, int(rf.matchIndex[commitid]))
						rf.mu.Unlock()
					}
				}()
			}
			rf.mu.Unlock()
		}
	}
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
	// fmt.Printf("kill %v ==================\n", rf.me)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	// fmt.Printf("rf.killed(): %v\n", rf.killed())
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		// Because the tester limits you to 10 heartbeats per second
		// 暂时设置过期时间（200,1200）ms
		rf.mu.Lock()
		curState := rf.state
		rf.mu.Unlock()
		// fmt.Printf("%v: state: %v\n", rf.me, rf.state)
		lastResp := len(rf.peers)
		curResp := int32(len(rf.peers))
		if curState == StateLeader {

			time.Sleep(time.Millisecond * 100)
			lastResp = int(curResp)

			if lastResp <= len(rf.peers)/2 {
				if lastResp <= len(rf.peers)/2 {
					rf.mu.Lock()
					rf.state = StateFollower
					rf.recvHeartBeat = 0
					rf.mu.Unlock()
					// time.Sleep(time.Millisecond * 100)
					continue
				}

			}
			curResp = int32(0)
			// 群发心跳
			args := new(AppendEntriesArgs)
			replies := make([]AppendEntriesReply, len(rf.peers))
			rf.mu.Lock()
			if rf.state != StateLeader {
				rf.mu.Unlock()
				continue
			}
			rf.state = StateLeader
			// 称为leader, 通知别人
			args.Term = rf.currentTerm
			args.Info = "heart"
			args.Leader = rf.me
			args.Entries = []LogEntry{}
			args.Entries = append(args.Entries, LogEntry{nil, rf.currentTerm, 0})
			rf.mu.Unlock()
			for idx, _ := range rf.peers {
				i := idx
				if int32(i) == rf.me {
					continue
				}
				go func() {

					replies[i] = AppendEntriesReply{-1}
					rf.peers[i].Call("Raft.AppendEntries", args, &replies[i])
					if atomic.LoadInt32(&replies[i].Ok) == 1 {
						atomic.AddInt32(&curResp, 1)
					}

				}()
			}

		} else if curState == StateFollower {
			// 目前应该看不到candidate状态
			randTime := rand.Intn(100)
			time.Sleep(time.Millisecond * time.Duration(randTime))
			c := 1
			for {
				randTime := 10
				time.Sleep(time.Millisecond * time.Duration(randTime))
				rf.mu.Lock()
				heart := rf.recvHeartBeat
				rf.mu.Unlock()
				c++
				if c == 10 || heart == 1 {
					break
				}
			}

			rf.mu.Lock()
			heartBeat := rf.recvHeartBeat
			if heartBeat == 1 {
				// 收到心跳了
				// fmt.Printf("%d recv heartBeat\n", rf.me)
				rf.recvHeartBeat = 0
			} else {
				// 有段时间没收到心跳了，成为candidate
				fmt.Printf("%d 超时称为candidate\n", rf.me)
				rf.state = StateCandidate
				rf.voteFor = -1
			}
			rf.persist()
			rf.mu.Unlock()
		} else {
			// candidate, 请求成为leader
			// c := 1
			// for {
			// 	randTime := rand.Intn(20)
			// 	time.Sleep(time.Millisecond * time.Duration(randTime))
			// 	rf.mu.Lock()
			// 	heart := rf.recvHeartBeat
			// 	rf.mu.Unlock()
			// 	c++
			// 	if c == 20 || heart == 1 {
			// 		break
			// 	}
			// }
			randTime := rand.Intn(500)
			time.Sleep(time.Millisecond * time.Duration(randTime))

			rf.mu.Lock()
			if rf.recvHeartBeat == 1 {
				// 收到心跳了
				fmt.Println(rf.me, "候选人竞选之前收到心跳,变回follower")
				rf.recvHeartBeat = 0
				rf.state = StateFollower
				rf.mu.Unlock()
				continue
			}
			args := new(RequestVoteArgs)
			replies := make([]RequestVoteReply, len(rf.peers))
			peerCount := len(rf.peers)
			args.CurrentTerm = rf.currentTerm + 1
			args.LastLogTerm = 0
			if len(rf.logs) > 0 {
				args.LastLogTerm = rf.logs[len(rf.logs)-1].Term
			}
			args.LogLen = len(rf.logs)
			args.Me = rf.me

			voteCount := int32(0)
			if rf.voteFor == -1 {
				rf.voteFor = rf.me
				voteCount++

				for idx, _ := range rf.peers {
					i := idx
					if i == int(rf.voteFor) {
						continue
					}
					replies[idx] = RequestVoteReply{Ok: 0}
					go func() {
						rf.peers[i].Call("Raft.RequestVote", args, &replies[i])
						atomic.AddInt32(&voteCount, replies[i].Ok)
					}()
				}
				rf.persist()
				rf.mu.Unlock()

				c := 0
				for {
					randTime := 10
					time.Sleep(time.Millisecond * time.Duration(randTime))
					if int(atomic.LoadInt32(&voteCount)) > peerCount/2 {
						break
					}
					c++
					if c == 10 {
						break
					}
				}

				// time.Sleep(time.Millisecond * 50)
			} else {
				rf.mu.Unlock()
			}

			fmt.Printf("%d voteCount: %v\n", rf.me, voteCount)
			if int(atomic.LoadInt32(&voteCount)) > peerCount/2 {
				fmt.Println("leader: ", rf.me)

				// 群发心跳
				args := new(AppendEntriesArgs)
				replies := make([]AppendEntriesReply, len(rf.peers))
				rf.mu.Lock()
				rf.state = StateLeader
				if rf.state != StateLeader {
					rf.mu.Unlock()
					continue
				}
				rf.state = StateLeader
				// 称为leader, 通知别人
				rf.currentTerm = rf.currentTerm + 1

				args.Term = rf.currentTerm
				args.Leader = rf.me
				args.Info = "heart"
				args.Entries = []LogEntry{}
				args.Entries = append(args.Entries, LogEntry{nil, rf.currentTerm, 0})

				// rf.persist()
				for idx, _ := range rf.peers {
					i := idx
					go func() {
						replies[i] = AppendEntriesReply{-1}
						rf.peers[i].Call("Raft.AppendEntries", args, &replies[i])
						// time.Sleep(time.Millisecond * 100)
						// if atomic.LoadInt32(&replies[i].Ok) == -2 {
						// 	rf.mu.Lock()
						// 	rf.state = StateFollower
						// 	rf.mu.Unlock()
						// }
					}()
				}
				go func() {
					rf.mu.Lock()
					for i := 0; i < len(rf.peers); i++ {
						rf.nextIndex[i] = len(rf.logs) + 1
						rf.matchIndex[i] = 0
					}
					// fmt.Printf("leader选举后rf.nextIndex: %v\n", rf.nextIndex)
					if len(rf.logs) > 0 && rf.logs[len(rf.logs)-1].Command == nil {
						rf.logs = rf.logs[:len(rf.logs)-1]
					}
					rf.logs = append(rf.logs, LogEntry{nil, rf.currentTerm, 0})
					rf.mu.Unlock()
					rf.syncLogs()
				}()
				rf.persist()
				rf.mu.Unlock()
			} else {
				// fmt.Println(rf.me, " 本轮选举失败")
				// 自己失败了，别的仍可能成功，所以要等待超时
				// 没有选举成功，成为follower，继续等待超时
				rf.mu.Lock()
				rf.voteFor = -1 // 竞选者先为自己投票，如果不成，自己变为-1
				rf.persist()
				rf.mu.Unlock()

			}
		}

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
	rf := &Raft{}
	rf.mu = sync.Mutex{}
	rf.peers = peers
	rf.persister = persister
	rf.me = int32(me)
	rf.dead = 0
	rf.voteFor = -1
	rf.logs = []LogEntry{}
	rf.state = StateFollower
	rf.recvHeartBeat = 0
	rf.applyCh = applyCh
	rf.commitIdx = 0
	rf.lastApplyed = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.sendApplyCh()

	return rf
}
