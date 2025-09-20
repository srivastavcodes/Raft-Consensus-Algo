// Server container for a Raft Consensus Module. Exposes Raft to
// the network and enables RPCs between Raft peers.

package raft

import (
	"fmt"
	"net"
	"net/rpc"
	"sync"
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

func (srv *Server) Serve() {}

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

type RPCProxy struct {
	cm *ConsensusModule
}
