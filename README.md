# Raft Consensus Algorithm Implementation

A complete implementation of the Raft distributed consensus algorithm in Go, designed for educational purposes and practical understanding of how distributed consensus works in real systems.

## Overview

Raft is a consensus algorithm designed to be more understandable than Paxos while providing the same consistency guarantees. This implementation demonstrates how to build a replicated state machine that can tolerate server crashes and network partitions while maintaining strong consistency across all nodes.

## Features

- **Leader Election**: Automatic leader election with randomized timeouts
- **Log Replication**: Commands are replicated across cluster nodes before being committed
- **Persistence**: State persists across server crashes and restarts
- **Key/Value Database**: A practical application built on top of the Raft consensus module
- **Exactly-Once Delivery**: Ensures client requests are processed exactly once
- **Strong Consistency**: Prioritizes consistency over availability (CP in CAP theorem)

## Implementation Structure

### Core Components
- **Consensus Module**: The heart of the Raft algorithm handling elections, log replication, and safety
- **Persistent Log**: Commands are stored persistently before being applied to the state machine
- **State Machine**: The actual service (e.g., key/value store) that processes committed commands

### Server States
- **Follower**: Default state, responds to RPCs from candidates and leaders
- **Candidate**: Attempts to become leader during election periods  
- **Leader**: Handles client requests and replicates log entries to followers

### Core RPCs
- **RequestVote**: Used during leader election to gather votes
- **AppendEntries**: Used for log replication and leader heartbeats

## Architecture

The implementation follows the Raft paper specification:

- **Consensus Module**: Handles leader election, log replication, and ensures safety properties
- **Persistent State**: Current term, voted for candidate, and log entries (survives crashes)
- **Volatile State**: Commit index and last applied index on all servers
- **Leader State**: Next index and match index for each follower (leader only)

## What This Implementation Covers

This project implements the complete Raft algorithm across multiple progressive parts:

1. **Elections**: Leader election mechanism with randomized timeouts
2. **Commands and Log Replication**: Handling client commands and replicating them across the cluster
3. **Persistence and Optimizations**: Making state persist across crashes with performance improvements
4. **Key/Value Database**: A practical application demonstrating Raft in action
5. **Exactly-Once Delivery**: Ensuring client requests are processed exactly once

## Getting Started

### Prerequisites

- Go 1.16 or higher
- Understanding of the Raft paper (recommended reading)

### Installation

```bash
git clone <repository-url>
cd raft-implementation
go build ./...
```

### Running the Implementation

The implementation uses Go's `net/rpc` for inter-server communication. Each part can be tested independently using the provided test suite.

### Key/Value Database Example

The implementation includes a complete key/value database built on Raft:

- **GET key**: Retrieve a value for a key
- **SET key value**: Store a key-value pair  
- **CAS key old_value new_value**: Compare-and-swap operation

Clients connect to any server in the cluster, and the system automatically handles routing to the leader.

## Testing

The implementation includes comprehensive tests for each component:

```bash
# Run all tests
go test ./...

# Test specific components
go test ./raft     # Core Raft algorithm tests
go test ./kvdb     # Key/Value database tests
```

## Design Philosophy

This implementation prioritizes:

- **Educational Value**: Code is written to be clear and understandable
- **Correctness**: Rigorous testing ensures the implementation handles edge cases
- **Simplicity**: Uses Go's built-in concurrency primitives and standard library
- **Consistency over Availability**: Follows CP in CAP theorem - prioritizes consistency

## Fault Tolerance

The system handles various failure scenarios:

- Server crashes and restarts (with persistent state recovery)
- Network partitions (requires majority to make progress)
- Message loss and reordering
- Leader failures (automatic re-election)

A cluster with N servers can tolerate (N-1)/2 failures while maintaining availability.

## Performance Characteristics

This implementation is designed for:
- **Low-throughput, high-consistency scenarios** (configuration management, leader election, etc.)
- **Coarse-grained operations** rather than fine-grained database transactions
- **Strong consistency guarantees** at the expense of some availability during partitions

## Implementation Notes

- Uses Go's `net/rpc` for simple inter-server communication
- Leverages Go's goroutines and channels for concurrent operations
- Persistent state is handled through simple file-based storage
- Designed for educational purposes with clear separation of concerns

## References

- [Raft Paper](https://raft.github.io/raft.pdf) - "In Search of an Understandable Consensus Algorithm" by Diego Ongaro and John Ousterhout
- [Raft Website](https://raft.github.io/) - Interactive visualizations and additional resources
- [Diego Ongaro's PhD Dissertation](https://github.com/ongardie/dissertation) - Comprehensive treatment of Raft

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
