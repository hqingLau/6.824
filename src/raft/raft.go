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
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
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
	applyCh       chan ApplyMsg
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

type ApplyMsgIdList []int

func (rf *Raft) SendApplyCh(entries ApplyMsgIdList, reply *int) {
	// 这里顺序乱掉了，还没改，估计要判断一下前者的状态，然后做个等待。
	// hqinglau@centos:~/6.824/src/raft$ cat a.txt | grep "2: ch msg"
	// 2: ch msg: {true 10 1 false [] 0 0}
	// 2: ch msg: {true 30 3 false [] 0 0}
	// 2: ch msg: {true 1000 2 false [] 0 0}
	// hqinglau@centos:~/6.824/src/raft$ cat a.txt | grep "3: ch msg"
	// 3: ch msg: {true 10 1 false [] 0 0}
	// 3: ch msg: {true 1000 2 false [] 0 0}
	// 3: ch msg: {true 30 3 false [] 0 0}
	for _, idx := range entries {
		rf.mu.Lock()
		logEntry := rf.logs[idx-1]
		if rf.logs[idx-1].Committed == 1 {
			rf.mu.Unlock()
			continue
		}
		msg := ApplyMsg{CommandValid: true, Command: logEntry.Command, CommandIndex: idx}

		fmt.Printf("%d: ch msg: %v\n", rf.me, msg)
		rf.mu.Unlock()
		rf.applyCh <- msg
		rf.mu.Lock()
		rf.logs[idx-1].Committed = 1
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
	// fmt.Printf("原来%d votefor %d\n", rf.me, rf.voteFor)
	defer rf.mu.Unlock()
	if rf.state == StateFollower || args.CurrentTerm <= rf.currentTerm {
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
			reply.Ok = 1
		}
	}

	if reply.Ok == 1 {
		// fmt.Printf("%d rf.voteFor: %v state:%v log:%v\n", rf.me, rf.voteFor, rf.state, rf.logs)
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
	Ok int32 // 0:fail 1,ok
}

// args.Term = int64(term)
// args.LastLogIdx = lastLogIdx
// if lastLogIdx >= 1 {
// 	args.LastTerm = int(rf.logs[lastLogIdx-1].Term)
// }
// args.Leader = server
// args.Entries = []LogEntry{}
// args.Entries = append(args.Entries, rf.logs[lastLogIdx+1])

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// 只有leader能调用AppendEntries
	// 所以收到之后，就是收到了心跳或者log，重置election timeout
	atomic.StoreInt32(&reply.Ok, 0)
	rf.mu.Lock()

	defer rf.mu.Unlock()

	// leader的term比follower还小，leader失效，返回
	if rf.currentTerm > args.Term || args.LastLogIdx < 0 {
		// 此次添加不可能成功，退出
		atomic.StoreInt32(&reply.Ok, -1)
		return
	}

	rf.currentTerm = args.Term
	rf.recvHeartBeat = 1
	rf.voteFor = -1

	if rf.me != args.Leader {
		rf.state = StateFollower
	}

	if args.Entries[0].Command == nil {
		atomic.StoreInt32(&reply.Ok, 1)
		return
	}

	// command entry
	if args.LastLogIdx == 0 {
		// 说明本条log是第一条，必须加上
		rf.logs = append(rf.logs, args.Entries...)
		atomic.StoreInt32(&reply.Ok, 1)
	} else {
		// args.LastLogIdx > 0
		// 例如：
		if len(rf.logs) == args.LastLogIdx {
			if rf.logs[args.LastLogIdx-1].Term == int64(args.LastTerm) {
				rf.logs = append(rf.logs, args.Entries...)
				atomic.StoreInt32(&reply.Ok, 1)
			}
		} else if len(rf.logs) > args.LastLogIdx {
			if rf.logs[args.LastLogIdx-1].Term == int64(args.LastTerm) {
				rf.logs[args.LastLogIdx].Term = args.Entries[0].Term
				rf.logs[args.LastLogIdx].Command = args.Entries[0].Command
				atomic.StoreInt32(&reply.Ok, 1)
			}
		}
	}
	fmt.Printf("日志：%d: %+v\n", rf.me, rf.logs)
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

	rf.mu.Lock()
	index = len(rf.logs) + 1
	term = int(rf.currentTerm)
	isLeader = (!rf.killed()) && (rf.state == StateLeader)
	rf.mu.Unlock()
	if isLeader {
		go rf.sendEntries(command, term, rf.me)
	}
	// Your code here (2B).

	return index, term, isLeader
}

// 小写应该就行了，只是服务器调用，rpc调用AppendEntries
// 可能要server log对齐，需要term信息, 如果command为nil表示是leader当选的信号
func (rf *Raft) sendEntries(command interface{}, term int, server int32) {
	// 不能通过此处添加nil指令，nil用作leader选举通知
	if command == nil {
		return
	}

	// 先给本地加上log
	rf.mu.Lock()
	rf.logs = append(rf.logs, LogEntry{Command: command, Term: int64(term)})
	rf.mu.Unlock()

	// term和logidx都设成从1开始好了

	rf.mu.Lock()
	defer fmt.Printf("%d最后: %+v\n", rf.me, rf.logs)
	commitApplyChMap := make(map[int]ApplyMsgIdList) // 存储log的id就行，然后发送给follower，让follower自己处理
	for i := 0; i < len(rf.peers); i++ {
		commitApplyChMap[i] = []int{}
	}
	commitApplyChMap[int(rf.me)] = append(commitApplyChMap[int(rf.me)], int(len(rf.logs)))
	commitApplyChMapMutex := sync.Mutex{}
	commitList := []int{int(rf.me)}
	for idx, _ := range rf.peers {
		i := idx
		if i == int(rf.me) {
			continue
		}

		go func() {
			rf.mu.Lock()
			lastLogIdx := len(rf.logs) - 1 // 例如之前已经有一条消息，这是第二条，last就是1
			if lastLogIdx < 0 {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()

			// 等于0说明不对应，加不上，尝试加前一个
			for {
				args := AppendEntriesArgs{}
				reply := AppendEntriesReply{0}
				rf.mu.Lock()
				if lastLogIdx >= len(rf.logs) || lastLogIdx < 0 {
					rf.mu.Unlock()
					break
				}
				args.Term = int64(term)
				args.LastLogIdx = lastLogIdx
				if lastLogIdx >= 1 {
					args.LastTerm = int(rf.logs[lastLogIdx-1].Term)
				}
				args.Leader = server
				args.Entries = []LogEntry{}
				// args.Entries = append(args.Entries, rf.logs[lastLogIdx])
				// 这里之前复制的rflogs，但是那个是已提交状态，就会干扰现在的chan发送
				args.Entries = append(args.Entries, LogEntry{
					Command:   rf.logs[lastLogIdx].Command,
					Term:      rf.logs[lastLogIdx].Term,
					Committed: 0,
				})
				rf.mu.Unlock()

				rf.peers[i].Call("Raft.AppendEntries", &args, &reply)
				if atomic.LoadInt32(&reply.Ok) == 1 {
					lastLogIdx++
					commitApplyChMapMutex.Lock()
					commitApplyChMap[i] = append(commitApplyChMap[i], lastLogIdx)
					commitApplyChMapMutex.Unlock()
				} else if atomic.LoadInt32(&reply.Ok) == 0 {
					lastLogIdx--
				} else {
					return
				}
			}
			fmt.Println(i, " 同步成功")
			commitApplyChMapMutex.Lock()
			commitList = append(commitList, i)
			fmt.Printf("%+v\n", commitList)
			commitApplyChMapMutex.Unlock()
		}()
	}
	rf.mu.Unlock()
	time.Sleep(time.Millisecond * 500)
	commitApplyChMapMutex.Lock()
	lencmls := len(commitList)
	commitApplyChMapMutex.Unlock()

	if lencmls > len(rf.peers)/2 {

		for _, i := range commitList {
			k := 0
			fmt.Printf("command %v提交给：%v: %v\n", command, i, commitApplyChMap[i])
			go rf.peers[i].Call("Raft.SendApplyCh", commitApplyChMap[i], &k)
		}
	} else {
		fmt.Printf("command %v follower不够\n", command)
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
		if curState == StateLeader {

			time.Sleep(time.Millisecond * 100)
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
			args.Leader = rf.me
			args.Entries = []LogEntry{}
			args.Entries = append(args.Entries, LogEntry{nil, rf.currentTerm, 0})
			rf.mu.Unlock()
			for idx, _ := range rf.peers {
				i := idx
				go func() {
					replies[i] = AppendEntriesReply{-1}
					rf.peers[i].Call("Raft.AppendEntries", args, &replies[i])
					time.Sleep(time.Millisecond * 100)
					if atomic.LoadInt32(&replies[i].Ok) == 0 {
						rf.mu.Lock()
						rf.state = StateFollower
						rf.mu.Unlock()
					}
				}()
			}

		} else if curState == StateFollower {
			// 目前应该看不到candidate状态
			randTime := 100 + rand.Intn(100)
			time.Sleep(time.Millisecond * time.Duration(randTime))

			rf.mu.Lock()
			heartBeat := rf.recvHeartBeat
			if heartBeat == 1 {
				// 收到心跳了
				// fmt.Printf("%d recv heartBeat\n", rf.me)
				rf.recvHeartBeat = 0
			} else {
				// 有段时间没收到心跳了，成为candidate
				// fmt.Printf("%d 超时称为candidate\n", rf.me)
				rf.state = StateCandidate
				rf.voteFor = -1
			}
			rf.mu.Unlock()
		} else {
			// candidate, 请求成为leader
			randTime := rand.Intn(600)
			time.Sleep(time.Millisecond * time.Duration(randTime))
			rf.mu.Lock()
			if rf.recvHeartBeat == 1 {
				// 收到心跳了
				// fmt.Println(rf.me, "候选人竞选之前收到心跳,变回follower")
				rf.recvHeartBeat = 0
				rf.state = StateFollower
				rf.mu.Unlock()
				continue
			}
			args := new(RequestVoteArgs)
			replies := make([]RequestVoteReply, len(rf.peers))
			// // 可以投票了
			// rf.voteFor = -1
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
				rf.mu.Unlock()
				for idx, _ := range rf.peers {
					i := idx
					replies[idx] = RequestVoteReply{Ok: 0}
					go func() {
						rf.peers[i].Call("Raft.RequestVote", args, &replies[i])
						atomic.AddInt32(&voteCount, replies[i].Ok)
					}()
				}
				time.Sleep(time.Millisecond * 100)
			} else {
				rf.mu.Unlock()
			}

			// fmt.Printf("%d voteCount: %v\n", rf.me, voteCount)
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
				args.Entries = []LogEntry{}
				args.Entries = append(args.Entries, LogEntry{nil, rf.currentTerm, 0})
				rf.mu.Unlock()
				for idx, _ := range rf.peers {
					i := idx
					go func() {
						replies[i] = AppendEntriesReply{-1}
						rf.peers[i].Call("Raft.AppendEntries", args, &replies[i])
						time.Sleep(time.Millisecond * 100)
						if atomic.LoadInt32(&replies[i].Ok) == 0 {
							rf.mu.Lock()
							rf.state = StateFollower
							rf.mu.Unlock()
						}
					}()
				}

			} else {
				// fmt.Println(rf.me, " 本轮选举失败")
				// 自己失败了，别的仍可能成功，所以要等待超时
				// 没有选举成功，成为follower，继续等待超时
				rf.mu.Lock()
				rf.state = StateFollower
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

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
