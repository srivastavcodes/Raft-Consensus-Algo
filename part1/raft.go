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

type LogEntry struct {
	Command any
	Term    int
}

type CMState int

const (
	Follower CMState = iota
	Candidate
	Leader
	Dead
)

func (state CMState) String() string {
	switch state {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	case Dead:
		return "Dead"
	default:
		panic("what you doin? Not a recognised state!")
	}
}

// ConsensusModule implements a single node of Raft Consensus.
type ConsensusModule struct {
	// mu protects concurrent access to CM.
	mu sync.Mutex

	// id is the server ID of this CM.
	id int

	// peerIds lists the IDs of our peers in the cluster.
	peerIds []int

	// server is the server containing this CM. It's used
	// issue RPC calls to peers.
	server *Server

	// Persistent Raft state on all servers.
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile Raft state on all servers.
	state              CMState
	electionResetEvent time.Time
}

// NewConsensusModule creates a new ConsensusModule with the given ID, list of peerIds and server.
// The ready channel signals the ConsensusModule that all peers are connected, and it's safe to
// start its state machine.
func NewConsensusModule(id int, peerIds []int, server *Server, ready <-chan any) *ConsensusModule {
	cm := &ConsensusModule{
		id:       id,
		peerIds:  peerIds,
		server:   server,
		votedFor: -1,
		state:    Follower,
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
	return cm
}

// Report reports the state of this ConsensusModule.
func (cm *ConsensusModule) Report() (id int, term int, isLeader bool) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.id, cm.currentTerm, cm.state == Leader
}

// Stop stops this ConsensusModule, cleaning up its state. This method returns
// quickly, but it may take a bit of time (up to ~electionTimeout) for all
// goroutines to exit
func (cm *ConsensusModule) Stop() {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.state = Dead
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

	if cm.state == Dead {
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

// AppendEntries handles AppendEntry RPCs from leaders for log replication and heartbeats.
// Updates term if higher, transitions to Follower if needed, and resets election timer.
// Returns success only if term matches current term.
func (cm *ConsensusModule) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.state == Dead {
		return nil
	}
	cm.dlogf("AppendEntries: %+v", args)

	if args.Term > cm.currentTerm {
		cm.dlogf("greater term in AppendEntries args")
		cm.becomeFollower(args.Term)
	}
	reply.Success = false

	if args.Term == cm.currentTerm {
		if cm.state != Follower {
			cm.becomeFollower(args.Term)
		}
		cm.electionResetEvent = time.Now()
		reply.Success = true
	}
	reply.Term = cm.currentTerm
	cm.dlogf("AppendEntries reply: %+v", reply)
	return nil
}

/*
	Raft guarantees that only a single leader exists in any given term, so if a
	peer finds itself as anything other than a follower during AppendEntry rpc
	from a legitimate leader, it's forced to become a follower.

	if cm.state != Follower {
		cm.becomeFollower(args.Term)
	}
	- Candidate → Follower: If this server is currently a Candidate running an
	election, but receives a valid AppendEntries from a legitimate leader,
	it should abandon its candidacy and become a follower of the established
	leader.

	- Leader → Follower: If this server thinks it's a Leader but receives a
	valid AppendEntries from another server with the same term, it means
	there's another legitimate leader (split-brain scenario). It should step down
	and become a follower.

	- Already Follower: If it's already a Follower, the condition is false and
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
		if cm.state != Candidate && cm.state != Follower {
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
	cm.state = Candidate
	cm.currentTerm += 1

	savedCurrentTerm := cm.currentTerm
	cm.electionResetEvent = time.Now()

	cm.votedFor = cm.id
	cm.dlogf("becomes Candidate (currentTerm=%d); log=%v", savedCurrentTerm, cm.log)

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

				if cm.state != Candidate {
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
	cm.state = Leader
	cm.dlogf("becomes Leader; term=%d, log=%v", cm.currentTerm, cm.log)
	go func() {
		ticker := time.Tick(50 * time.Millisecond)
		// Send periodic heartbeats, as long as still leader.
		for {
			cm.leaderSendHeartbeats()
			<-ticker

			cm.mu.Lock()
			if cm.state != Leader {
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
		args := AppendEntriesArgs{
			Term:     savedCurrentTerm,
			LeaderId: cm.id,
		}
		go func() {
			cm.dlogf("sending AppendEntries to %v: ni=%d, args=%+v", peerId, 0, args)

			var reply AppendEntriesReply
			if err := cm.server.Call(peerId, "ConsensusModule.AppendEntries", args, &reply); err == nil {
				cm.mu.Lock()
				defer cm.mu.Unlock()

				if reply.Term > savedCurrentTerm {
					cm.dlogf("term greater in heartbeat reply")
					cm.becomeFollower(reply.Term)
					return
				}
			}
		}()
	}
}

// becomeFollower transitions the ConsensusModule to Follower state with the given term.
// Resets voting state, updates election timer, and starts a new election
// timer.
// Expects cm.mu to be locked.
func (cm *ConsensusModule) becomeFollower(term int) {
	cm.dlogf("becomes Follower with term=%d; log=%v", term, cm.log)
	cm.state = Follower
	cm.currentTerm = term
	cm.votedFor = -1
	cm.electionResetEvent = time.Now()
	go cm.runElectionTimer()
}
