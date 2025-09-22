// Server container for a Raft Consensus Module. Exposes Raft to
// the network and enables RPCs between Raft peers.

package raft

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// Server wraps a raft.ConsensusModule along with rpc.Server that exposes
// its methods as RPC endpoints. It also manages the peers of the Raft
// server.
//
// The main goal of this type is to simply the code of raft.Server
// for presentation purposes. raft.ConsensusModule has a *Server to do its
// Peer communication and doesn't have to worry about the specifics of
// running an RPC server.
type Server struct {
	mu       sync.Mutex
	serverId int
	peerIds  []int
	cm       *ConsensusModule
	storage  Storage

	rpcServer *rpc.Server
	rpcProxy  *RPCProxy
	listener  net.Listener

	commitChan  chan<- CommitEntry
	peerClients map[int]*rpc.Client

	ready  <-chan any
	quitch chan struct{}
	wg     sync.WaitGroup
}

func NewServer(serverId int, peerIds []int, storage Storage, ready <-chan any, commitChan chan<- CommitEntry) *Server {
	return &Server{
		serverId: serverId,

		peerIds:     peerIds,
		peerClients: make(map[int]*rpc.Client),
		storage:     storage,

		ready:      ready,
		commitChan: commitChan,
		quitch:     make(chan struct{}),
	}
}

/*
	We lock the ConsensusModule struct everytime we access its fields, it is essential
	because the implementation tries to be as synchronous as possible.
	Meaning sequential code is sequential, and not split across multiple event handlers.
*/

func (srv *Server) Serve() {
	srv.mu.Lock()
	srv.cm = NewConsensusModule(srv.serverId, srv.peerIds, srv, srv.storage, srv.ready, srv.commitChan)

	// Create a new RPC server and register a RPCProxy that forwards
	// all method to srv.cm
	srv.rpcServer = rpc.NewServer()
	srv.rpcProxy = &RPCProxy{cm: srv.cm}
	srv.rpcServer.RegisterName("ConsensusModule", srv.rpcProxy)

	var err error
	srv.listener, err = net.Listen("tcp", ":0")
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("[%v] listening at %s", srv.serverId, srv.listener.Addr())
	srv.mu.Unlock()

	srv.wg.Add(1)
	go func() {
		defer srv.wg.Done()
		for {
			conn, err := srv.listener.Accept()
			if err != nil {
				select {
				case <-srv.quitch:
					return
				default:
					log.Fatal("accept error:", err)
				}
			}
			srv.wg.Add(1)
			go func() {
				srv.rpcServer.ServeConn(conn)
				srv.wg.Done()
			}()
		}
	}()
}

// Submit wraps the underlying ConsensusModule's Submit; see that method for doc.
func (srv *Server) Submit(cmd any) int {
	return srv.cm.Submit(cmd)
}

// DisconnectAll closes all the client connections to peers for this server.
func (srv *Server) DisconnectAll() {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	for id := range srv.peerClients {
		if srv.peerClients[id] != nil {
			srv.peerClients[id].Close()
			srv.peerClients[id] = nil
		}
	}
}

// Shutdown closes the server and waits for it to shut down properly.
func (srv *Server) Shutdown() {
	srv.cm.Stop()
	close(srv.quitch)
	srv.listener.Close()
	srv.wg.Wait()
}

func (srv *Server) GetListenAddr() net.Addr {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	return srv.listener.Addr()
}

// ConnectToPeer connects this server to the peer identified by the (addr) param.
// The connected client (peer) is saved to the Server for continued operation.
func (srv *Server) ConnectToPeer(peerId int, addr net.Addr) error {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	if srv.peerClients[peerId] == nil {
		client, err := rpc.Dial(addr.Network(), addr.String())
		if err != nil {
			return fmt.Errorf("failed to dial addr=%s", addr.String())
		}
		srv.peerClients[peerId] = client
	}
	return nil
}

// DisconnectPeer disconnects this server from the peer identified by the peerId.
// The connected (now disconnected) client is removed from the Server.
func (srv *Server) DisconnectPeer(peerId int) error {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	if srv.peerClients[peerId] != nil {
		err := srv.peerClients[peerId].Close()
		srv.peerClients[peerId] = nil
		return err
	}
	return nil
}

// Call works by calling the (serviceMethod) of peer with ID=(id) with the (args)
// as params and populates the (reply) struct.
func (srv *Server) Call(id int, serviceMethod string, args any, reply any) error {
	srv.mu.Lock()
	peer := srv.peerClients[id]
	srv.mu.Unlock()

	// If this is called after shutdown (where client.Close is called), it will
	// return an error
	if peer == nil {
		return fmt.Errorf("call client %d after it's closed", id)
	} else {
		return peer.Call(serviceMethod, args, reply)
	}
}

// IsLeader checks if Server thinks it's the leader in the Raft cluster.
func (srv *Server) IsLeader() bool {
	_, _, isLeader := srv.cm.Report()
	return isLeader
}

// Proxy provides access to the RPC proxy this server is using; this is only
// for testing purposes to simulate faults.
func (srv *Server) Proxy() *RPCProxy {
	return srv.rpcProxy
}

// RPCProxy is a trivial pass-thru proxy type for ConsensusModule's RPC methods.
// It's useful for:
//   - Simulating a small delay in RPC transmission.
//   - Simulating possible unreliable connections by delaying some messages
//     significantly and dropping others when RAFT_UNRELIABLE_RPC is set.
type RPCProxy struct {
	mu sync.Mutex
	cm *ConsensusModule

	// numCallsBeforeDrop is used to control dropping RPC calls:
	//   -1: means we're not dropping any calls
	//    0: means we're dropping all calls now
	//   >0: means we'll start dropping calls after this number is made
	numCallsBeforeDrop int
}

func NewProxy(cm *ConsensusModule) *RPCProxy {
	return &RPCProxy{
		cm:                 cm,
		numCallsBeforeDrop: -1,
	}
}

func (rpp *RPCProxy) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	if len(os.Getenv("RAFT_UNRELIABLE_RPC")) > 0 {
		dice := rand.Intn(10)
		if dice == 9 {
			rpp.cm.dlogf("drop RequestVote")
			return fmt.Errorf("RPC failed")
		} else if dice == 8 {
			rpp.cm.dlogf("delay RequestVote")
			time.Sleep(75 * time.Millisecond)
		}
	} else {
		time.Sleep(time.Duration(1+rand.Intn(5)) * time.Millisecond)
	}
	return rpp.cm.RequestVote(args, reply)
}

func (rpp *RPCProxy) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	if len(os.Getenv("RAFT_UNRELIABLE_RPC")) > 0 {
		dice := rand.Intn(10)
		if dice == 9 {
			rpp.cm.dlogf("drop AppendEntries")
			return fmt.Errorf("RPC failed")
		} else if dice == 8 {
			rpp.cm.dlogf("delay AppendEntries")
			time.Sleep(75 * time.Millisecond)
		}
	} else {
		time.Sleep(time.Duration(1+rand.Intn(5)) * time.Millisecond)
	}
	return rpp.cm.AppendEntries(args, reply)
}

func (rpp *RPCProxy) Call(peer *rpc.Client, method string, args any, reply any) error {
	rpp.mu.Lock()
	if rpp.numCallsBeforeDrop == 0 {
		rpp.mu.Unlock()
		rpp.cm.dlogf("drop Call %s: %v", method, args)
		return fmt.Errorf("RPC failed")
	} else {
		if rpp.numCallsBeforeDrop > 0 {
			rpp.numCallsBeforeDrop--
		}
		rpp.mu.Unlock()
		return peer.Call(method, args, reply)
	}
}

// DropCallsAfterN instruct the proxy to drop calls after n are made from this
// point.
func (rpp *RPCProxy) DropCallsAfterN(n int) {
	rpp.mu.Lock()
	defer rpp.mu.Unlock()

	rpp.numCallsBeforeDrop = n
}

func (rpp *RPCProxy) DontDropCalls() {
	rpp.mu.Lock()
	defer rpp.mu.Unlock()

	rpp.numCallsBeforeDrop = -1
}
