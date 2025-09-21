package raft

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/joho/godotenv"
)

var (
	_          = godotenv.Load(".envrc")
	DebugCM, _ = strconv.Atoi(os.Getenv("DebugCM"))
)

/*
	In our implementation, when a ConsensusModule is created, it takes in a commit
	channel - a channel which it uses to send committed commands to the caller:
	commitChan chan <- CommitEntry
*/

// CommitEntry is the data reported by Raft to the commit channel. Each commit
// entry notifies the client that consensus was reached on a command, and it
// can be applied to the client's state machine.
type CommitEntry struct {
	// Command is the client command being committed
	Command any

	// Index is the log index at which the client command is committed.
	Index int

	// Term is the Raft term at which the client command was committed.
	Term int
}

type CMState int

const (
	StateFollower CMState = iota
	StateCandidate
	StateLeader
	StateDead
)

func (state CMState) String() string {
	switch state {
	case StateFollower:
		return "Follower"
	case StateCandidate:
		return "Candidate"
	case StateLeader:
		return "Leader"
	case StateDead:
		return "Dead"
	default:
		panic("what you doin? Not a recognised state!")
	}
}

type LogEntry struct {
	Command any
	Term    int
}

// ConsensusModule implements a single node of Raft Consensus.
type ConsensusModule struct {
	// mu protects concurrent access to CM.
	mu sync.Mutex

	// id is the server ID of this CM.
	id int

	// peerIds lists the IDs of our peers in the cluster.
	peerIds []int

	// server is the server containing this CM. It's used issue RPC calls
	// to peers.
	server *Server

	// commitChan is the channel where this CM is going to report committed
	// log entries. It's passed in by the client during construction.
	commitChan chan<- CommitEntry

	// newCommitReadyChan is an internal notification channel used by goroutines
	// that commit new entries to the log to notify that these entries may be
	// sent on commitChan.
	newCommitReadyChan chan struct{}

	// Persistent Raft state on all servers.
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile Raft state on all servers.
	commitIndex        int
	lastApplied        int
	state              CMState
	electionResetEvent time.Time

	// Volatile Raft state on leaders.
	nextIndex  map[int]int
	matchIndex map[int]int
}

// NewConsensusModule creates a new ConsensusModule with the given ID, list of peerIds and server.
// The ready channel signals the ConsensusModule that all peers are connected, and it's safe to
// start its state machine.
func NewConsensusModule(id int, peerIds []int, server *Server, ready <-chan any, commitChan chan<- CommitEntry) *ConsensusModule {
	cm := &ConsensusModule{
		id:      id,
		peerIds: peerIds,

		server: server,
		state:  StateFollower,

		votedFor:    -1,
		commitIndex: -1,
		lastApplied: -1,

		commitChan:         commitChan,
		newCommitReadyChan: make(chan struct{}, 16),

		nextIndex:  make(map[int]int),
		matchIndex: make(map[int]int),
	}
	go func() {
		// The CM is quiescent until ready is signaled; then it starts
		// a countdown for leader election
		<-ready
		cm.mu.Lock()
		cm.electionResetEvent = time.Now()
		cm.mu.Unlock()
		cm.runElectionTimer()
	}()
	go cm.commitChanSender()
	return cm
}

// Report reports the state of this ConsensusModule.
func (cm *ConsensusModule) Report() (id int, term int, isLeader bool) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.id, cm.currentTerm, cm.state == StateLeader
}

// Submit submits a new command to the ConsensusModule. This function doesn't block;
// client read the commit channel passed in the constructor to be notified of new
// committed entries.
//
// It returns true if this ConsensusModule is the leader - in which case the command
// is accepted. If false is returned, the client will have to find a different
// ConsensusModule to submit this command to.
func (cm *ConsensusModule) Submit(command any) bool {
	cm.mu.Lock()
	defer cm.mu.Lock()
	cm.dlogf("Submit received by %v: %v", cm.state, command)

	if cm.state == StateLeader {
		cm.log = append(cm.log, LogEntry{Command: command, Term: cm.currentTerm})
		cm.dlogf("log=%v", cm.log)
		return true
	}
	return false
}

// Stop stops this ConsensusModule, cleaning up its state. This method returns
// quickly, but it may take a bit of time (up to ~electionTimeout) for all
// goroutines to exit
func (cm *ConsensusModule) Stop() {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.state = StateDead
	cm.dlogf("[%d] becomes dead", cm.id)
}

// dlogf logs a debugging message if DebugCM > 0
func (cm *ConsensusModule) dlogf(format string, args ...any) {
	if DebugCM > 0 {
		format = fmt.Sprintf("[%d] ", cm.id) + format
		log.Printf(format, args...)
	}
}

// RequestVote and AppendEntries info in Figure 2 of the paper.

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// RequestVote verifies the term, and if it hasn't already voted to another candidate
// grants a vote to the current candidate.
// Resets the election event and prepares the RequestVoteReply.
func (cm *ConsensusModule) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.state == StateDead {
		return nil
	}
	cm.dlogf("ReqeustVote: %+v [currentTerm=%d, votedFor=%d]", args, cm.currentTerm, cm.votedFor)

	if args.Term > cm.currentTerm {
		cm.dlogf("greater term in RequestVote args")
		cm.becomeFollower(args.Term)
	}
	reply.VoteGranted = false

	if cm.currentTerm == args.Term && (cm.votedFor == -1 || cm.votedFor == args.CandidateId) {
		reply.VoteGranted = true
		cm.votedFor = args.CandidateId
		cm.electionResetEvent = time.Now()
	}
	reply.Term = cm.currentTerm

	cm.dlogf("RequestVote reply: %+v", reply)
	return nil
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// todo: re-read the AppendEntries section of the part-2 and write a bookmark for the bullet-points.

// AppendEntries handles AppendEntry RPCs from leaders for log replication and heartbeats.
// Updates term if higher, transitions to StateFollower if needed, and resets election timer.
// Returns success only if term matches current term.
func (cm *ConsensusModule) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.state == StateDead {
		return nil
	}
	cm.dlogf("AppendEntries: %+v", args)

	if args.Term > cm.currentTerm {
		cm.dlogf("greater term in AppendEntries args")
		cm.becomeFollower(args.Term)
	}
	reply.Success = false

	if args.Term == cm.currentTerm {
		if cm.state != StateFollower {
			cm.becomeFollower(args.Term)
		}
		cm.electionResetEvent = time.Now()

		// Does our log contain an entry at PrevLogIndex whose term matches
		// PrevLogTerm? Note that in the extreme case of PrevLogIndex=-1
		// this is vacuously true.
		if args.PrevLogIndex == -1 ||
			(args.PrevLogIndex < len(cm.log) && args.PrevLogTerm == cm.log[args.PrevLogIndex].Term) {
			reply.Success = true

			// Find an insertion point - where there's a term mismatch between
			// the existing log starting at PrevLogIndex+1 and the new entries
			// sent in the RPC
			var (
				logInsertIndex  = args.PrevLogIndex + 1
				newEntriesIndex = 0
			)
			for {
				if logInsertIndex > len(cm.log) || newEntriesIndex >= len(args.Entries) {
					break
				}
				if cm.log[logInsertIndex].Term != args.Entries[newEntriesIndex].Term {
					break
				}
				logInsertIndex++
				newEntriesIndex++
			}
			// At the end of the above loop:
			// - logInsertIndex points at the end of the log, or an index where
			//   the term mismatches with an entry from the leader.
			// - newEntriesIndex points at the end of Entries, or an index where
			//   term mismatches with the corresponding log entry.
			if newEntriesIndex < len(args.Entries) {
				cm.dlogf("inserting entries %v from index %d",
					args.Entries[newEntriesIndex:], logInsertIndex,
				)
				cm.log = append(cm.log[:logInsertIndex], args.Entries[newEntriesIndex:]...)
				cm.dlogf("log is now: %v", cm.log)
			}
			// Set commit index.
			if args.LeaderCommit > cm.commitIndex {
				cm.commitIndex = min(args.LeaderCommit, len(cm.log)-1)
				cm.dlogf("setting commitIndex=%d", cm.commitIndex)
				cm.newCommitReadyChan <- struct{}{}
			}
		}
	}
	reply.Term = cm.currentTerm
	cm.dlogf("AppendEntries reply: %+v", reply)
	return nil
}

/*
	Raft guarantees that only a single leader exists in any given term, so if a
	peer finds itself as anything other than a follower during AppendEntry rpc
	from a legitimate leader, it's forced to become a follower.

	if cm.state != StateFollower {
		cm.becomeFollower(args.Term)
	}
	- StateCandidate → StateFollower: If this server is currently a StateCandidate running an
	election, but receives a valid AppendEntries from a legitimate leader,
	it should abandon its candidacy and become a follower of the established
	leader.

	- StateLeader → StateFollower: If this server thinks it's a StateLeader but receives a
	valid AppendEntries from another server with the same term, it means
	there's another legitimate leader (split-brain scenario). It should step down
	and become a follower.

	- Already StateFollower: If it's already a StateFollower, the condition is false and
	no state change occurs,which is correct.
*/

// electionTimeout generates a pseudo-random election timeout duration.
func (cm *ConsensusModule) electionTimeout() time.Duration {
	// If RAFT_FORCE_MORE_REELECTION is set, stress-test by deliberately
	// generating a hard-coded number very often.
	// This will create collisions between different servers and force
	// more re-elections.
	if len(os.Getenv("RAFT_FORCE_MORE_REELECTION")) > 0 && rand.Intn(3) == 0 {
		return time.Duration(150) * time.Millisecond
	} else {
		return time.Duration(150+rand.Intn(150)) * time.Millisecond
	}
}

/*
	We lock the ConsensusModule struct everytime we access its fields, it is essential
	because the implementation tries to be as synchronous as possible.
	Meaning sequential code is sequential, and not split across multiple event handlers.
*/

// runElectionTimer implements an election timer, it should be launched whenever we
// want to start a timer towards becoming a candidate in a new election.
//
// This function is blocking and should be launched in a different goroutine; it's
// designed to work for a single (one-shot) election timer, as it exits whenever
// the ConsensusModule state changes from follower/candidate or the term changes.
func (cm *ConsensusModule) runElectionTimer() {
	timeoutDuration := cm.electionTimeout()
	cm.mu.Lock()
	termStarted := cm.currentTerm
	cm.mu.Unlock()
	cm.dlogf("election timer started (%v), term=%d", timeoutDuration, termStarted)

	// This loops until either:
	// - we discover the election timer is no longer needed, or the election
	// - timer expires and this CM becomes a candidate.
	// In a follower, this typically keeps running in the background for the
	// duration of the CM's lifetime.
	ticker := time.Tick(10 * time.Millisecond)
	for {
		<-ticker

		cm.mu.Lock()
		// here we will return if this peer becomes the leader
		if cm.state != StateCandidate && cm.state != StateFollower {
			cm.dlogf("in election timer, state=%d, bailing out", cm.state)
			cm.mu.Unlock()
			return
		}
		if termStarted != cm.currentTerm {
			cm.dlogf("in election timer, term changed from %d to %d, bailing out", termStarted, cm.currentTerm)
			cm.mu.Unlock()
			return
		}
		// Start an election if we haven't heard from a leader or haven't voted
		// for someone for the duration of the timeout.
		if elapsed := time.Since(cm.electionResetEvent); elapsed >= timeoutDuration {
			cm.startElection()
			cm.mu.Unlock()
			return
		}
		cm.mu.Unlock()
	}
}

/*
Things we need to run an election:
	1. Switch the state to candidate and increment the term, because that's what the algo
	   dictates for every election.
	2. Send RV RPCs to all peers, asking them to vote for us in this election.
	3. Wait for replies to these RPCs and count if we got enough votes to become a leader.
*/

// startElection starts a new election with this ConsensusModule as a candidate.
// Expects cm.mu to be locked
func (cm *ConsensusModule) startElection() {
	cm.state = StateCandidate
	cm.currentTerm += 1

	savedCurrentTerm := cm.currentTerm
	cm.electionResetEvent = time.Now()

	cm.votedFor = cm.id
	cm.dlogf("becomes StateCandidate (currentTerm=%d); log=%v", savedCurrentTerm, cm.log)

	votesReceived := 1

	for _, peerId := range cm.peerIds {
		go func() {
			args := RequestVoteArgs{
				Term:        savedCurrentTerm,
				CandidateId: cm.id,
			}
			var reply RequestVoteReply

			cm.dlogf("sending RequestVote to %d: %+v", peerId, args)
			if err := cm.server.Call(peerId, "ConsensusModule.RequestVote", args, &reply); err == nil {
				cm.mu.Lock()
				defer cm.mu.Unlock()
				cm.dlogf("received RequestVoteReply %+v", reply)

				if cm.state != StateCandidate {
					cm.dlogf("while waiting for reply, state=%v", cm.state)
					return
				}
				if reply.Term == savedCurrentTerm {
					if reply.VoteGranted {
						votesReceived++
						if votesReceived*2 > len(cm.peerIds)+1 {
							// Won the election!
							cm.dlogf("wins the election with %d votes", votesReceived)
							cm.startLeader()
							return
						}
					}
				} else if reply.Term > savedCurrentTerm {
					cm.dlogf("term grater in RequestVoteReply")
					cm.becomeFollower(reply.Term)
					return
				}
			}
		}()
	}
	// Run another election timer, in case this election was not successful
	go cm.runElectionTimer()
}

// startLeader switches ConsensusModule into a leader state and begins the process of heartbeats.
// Expects cm.mu to be locked.
func (cm *ConsensusModule) startLeader() {
	cm.state = StateLeader
	cm.dlogf("becomes StateLeader; term=%d, log=%v", cm.currentTerm, cm.log)
	go func() {
		ticker := time.Tick(50 * time.Millisecond)
		// Send periodic heartbeats, as long as still leader.
		for {
			cm.leaderSendHeartbeats()
			<-ticker

			cm.mu.Lock()
			if cm.state != StateLeader {
				cm.mu.Unlock()
				return
			}
			cm.mu.Unlock()
		}
	}()
}

// leaderSendHeartbeats sends a round of heartbeats to all peers, collects their
// replies and adjusts ConsensusModule state
func (cm *ConsensusModule) leaderSendHeartbeats() {
	cm.mu.Lock()
	savedCurrentTerm := cm.currentTerm
	cm.mu.Unlock()

	for _, peerId := range cm.peerIds {
		go func() {
			cm.mu.Lock()
			ni := cm.nextIndex[peerId]
			var (
				prevLogIndex = ni - 1
				prevLogTerm  = -1
			)
			if prevLogIndex >= 0 {
				prevLogTerm = cm.log[prevLogIndex].Term
			}
			// entries that need to be commited, found after the next index.
			// eg: ((3..4)...9), ni = 5, [ni:] = (5..9) -> newly commited.
			entries := cm.log[ni:]

			args := AppendEntriesArgs{
				Term:         savedCurrentTerm,
				LeaderId:     cm.id,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: cm.commitIndex,
			}
			cm.mu.Unlock()
			cm.dlogf("sending AppendEntries to %v: ni=%d, args=%+v", peerId, ni, args)

			var reply AppendEntriesReply
			if err := cm.server.Call(peerId, "ConsensusModule.AppendEntries", args, &reply); err == nil {
				cm.mu.Lock()
				defer cm.mu.Lock()

				if reply.Term > savedCurrentTerm {
					cm.dlogf("greater term in heartbeat reply")
					cm.becomeFollower(reply.Term)
					return
				}
				if cm.state == StateLeader && savedCurrentTerm == reply.Term {
					if reply.Success {
						cm.nextIndex[peerId] = ni + len(entries)
						cm.matchIndex[peerId] = cm.nextIndex[peerId] - 1
						cm.dlogf("AppendEntries reply from %d success: nextIndex := %+v, matchIndex := %+v",
							peerId, cm.nextIndex, cm.matchIndex,
						)
						savedCommitIndex := cm.commitIndex

						for i := cm.commitIndex + 1; i < len(cm.log); i++ {
							if cm.log[i].Term == cm.currentTerm {
								matchCount := 1
								for _, peerId := range cm.peerIds {
									if cm.matchIndex[peerId] >= i {
										matchCount++
									}
								}
								// if a certain log-index is replicated by the majority, commitIndex
								// advanced to it.
								if matchCount*2 > len(cm.peerIds)+1 {
									cm.commitIndex = i
								}
							}
						}
						if cm.commitIndex != savedCommitIndex {
							cm.dlogf("leader sets commitIndex := %d", cm.commitIndex)
							cm.newCommitReadyChan <- struct{}{}
						}
					} else {
						cm.nextIndex[peerId] = ni - 1
						cm.dlogf("AppendEntries reply from %d !success: nextIndex := %d", peerId, ni-1)
					}
				}
			}
		}()
	}
}

// becomeFollower transitions the ConsensusModule to StateFollower state with the given term.
// Resets voting state, updates election timer, and starts a new election
// timer.
// Expects cm.mu to be locked.
func (cm *ConsensusModule) becomeFollower(term int) {
	cm.dlogf("becomes StateFollower with term=%d; log=%v", term, cm.log)
	cm.state = StateFollower
	cm.currentTerm = term
	cm.votedFor = -1
	cm.electionResetEvent = time.Now()
	go cm.runElectionTimer()
}

// commitChanSender is responsible for sending committed entries on cm.commitChan.
// It watches newCommitReadyChan for notifications and calculates which new entries
// are ready to be sent.
//
// This method should run in a separated background goroutine; cm.commitChan may be
// buffered and will limit how fast the client consumes new committed entries.
// Returns when newCommitReadyChan is closed.
func (cm *ConsensusModule) commitChanSender() {
	for range cm.newCommitReadyChan {
		// find which entries we have to apply
		cm.mu.Lock()

		savedTerm := cm.currentTerm
		savedLastApplied := cm.lastApplied

		var entries []LogEntry
		if cm.commitIndex > cm.lastApplied {
			entries = cm.log[cm.lastApplied+1 : cm.commitIndex+1]
			cm.lastApplied = cm.commitIndex
		}
		cm.mu.Unlock()
		cm.dlogf("commitChanSender entries=%v, savedLastApplied=%d", entries, savedLastApplied)

		for i, entry := range entries {
			cm.commitChan <- CommitEntry{
				Command: entry.Command,
				Term:    savedTerm,
				// we add (+i+1) because entries will only contain elements after (cm.lastApplied + 1)
				// follow the logic of entries array creation in the if condition above.
				Index: savedLastApplied + i + 1,
			}
		}
	}
	cm.dlogf("commitChanSender done")
}
