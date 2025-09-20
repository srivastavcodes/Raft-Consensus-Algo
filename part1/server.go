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
	mu          sync.Mutex
	serverId    int
	peerIds     []int
	cm          *ConsensusModule
	rpcProxy    *RPCProxy
	rpcServer   *rpc.Server
	listener    net.Listener
	peerClients map[int]*rpc.Client
	ready       <-chan any
	quit        chan any
	wg          sync.WaitGroup
}

func NewServer(serverId int, peerIds []int, ready <-chan any) *Server {
	return &Server{
		serverId:    serverId,
		peerIds:     peerIds,
		peerClients: make(map[int]*rpc.Client),

		ready: ready,
		quit:  make(chan any),
	}
}

/*
	We lock the ConsensusModule struct everytime we access its fields, it is essential
	because the implementation tries to be as synchronous as possible.
	Meaning sequential code is sequential, and not split across multiple event handlers.
*/

func (srv *Server) Serve() {
	srv.mu.Lock()
	srv.cm = NewConsensusModule(srv.serverId, srv.peerIds, srv, srv.ready)

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
				case <-srv.quit:
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
	close(srv.quit)
	srv.listener.Close()
	srv.wg.Wait()
}

func (srv *Server) GetListenAddr() net.Addr {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	return srv.listener.Addr()
}

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

// RPCProxy is a trivial pass-thru proxy type for ConsensusModule's RPC methods.
// It's useful for:
//   - Simulating a small delay in RPC transmission.
//   - Simulating possible unreliable connections by delaying some messages
//     significantly and dropping others when RAFT_UNRELIABLE_RPC is set.
type RPCProxy struct {
	cm *ConsensusModule
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
