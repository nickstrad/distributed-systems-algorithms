# **Go Distributed Systems Repo Starter Guide**

# **Foundation for "Introduction to Reliable and Secure Distributed Programming" (Cachin, Guerraoui & Rodrigues)**

## **1\. Architecture Principles**

* **Event-Driven CSP/Actor Model:** Mirror the book's `upon event <module, Event | args> do` pseudocode by giving each abstraction its own goroutine and a single `for { select { ... } }` event loop. This eliminates mutexes inside algorithm logic and aligns cleanly with Go's Communicating Sequential Processes (CSP) primitives.  
* **Crash-Stop Simulation via Context:** Pass a `context.Context` into every module's `Run(ctx)` method. Canceling a node's context cleanly halts all its underlying goroutines, accurately simulating a crash-stop process failure without leaving orphaned background routines.  
* **Swappable Transports:** Abstract message delivery behind a single `Link` interface so Chapter 3–5 algorithms can run over an in-memory simulated network for deterministic unit testing, then swap to real UDP/TCP sockets without modifying any core algorithm logic.  
* **Why Not NATS (Yet):** Avoid higher-level message brokers like NATS while working through the foundational algorithms. Production brokers hide raw peer-to-peer failure modes—such as dropped packets, arbitrary delays, duplicates, and network partitions—that these algorithms are explicitly designed to handle and resolve.

## **2\. Quick Repo Setup**

Execute the following commands to set up the Go module and core directory structural layout:

```
mkdir distributed-go && cd distributed-go
go mod init github.com/yourusername/distributed-go
mkdir -p model transport link detector broadcast register consensus
```

## **3\. Project Layout**

The directory layout mirrors the book's progressive chapter structure to ensure package dependencies flow strictly downward without cyclic imports:t  
distributed-go/  
├── go.mod  
├── model/          \# NodeID, Message envelope, shared Event types  
├── transport/      \# Raw delivery: SimTransport (in-memory) & UDPTransport  
├── link/           \# Ch 2: FairLoss, Stubborn, Perfect links  
├── detector/       \# Ch 2: Perfect (P), Eventually Perfect (◇P), Leader (Ω)  
├── broadcast/      \# Ch 3: Best-Effort, Reliable, Uniform Reliable  
├── register/       \# Ch 4: Regular, Atomic shared memory  
└── consensus/      \# Ch 5: Flooding, Hierarchical, Paxos

````

### 4. Core Types & Interfaces

#### Model Package (`model/message.go`)

```go
package model

type NodeID string

type Message struct {
	ID      string // UUID or (SenderID + SeqNum) for deduplication
	From    NodeID
	To      NodeID
	Type    string // e.g., "HEARTBEAT", "BEB_DATA", "PAXOS_PREPARE"
	Payload []byte
}
````

### **Link Interface (`link/link.go`)**

```
package link

import (
	"context"
	"github.com/yourusername/distributed-go/model"
)

type Link interface {
	Send(to model.NodeID, msg model.Message) error
	Deliver() <-chan model.Message
	Run(ctx context.Context) error
}
```

## **5\. Chapter 2 Building Blocks (Starter Code)**

### **1\. PerfectLink Implementation (`link/perfect.go`)**

`PerfectLink` wraps a `StubbornLink` to eliminate duplicate messages (Algorithm 2.2):

```
package link

import (
	"context"
	"github.com/yourusername/distributed-go/model"
)

type PerfectLink struct {
	stubborn  Link
	delivered map[string]struct{} // Tracks seen Message.IDs
	out       chan model.Message
}

func NewPerfectLink(stubborn Link) *PerfectLink {
	return &PerfectLink{
		stubborn:  stubborn,
		delivered: make(map[string]struct{}),
		out:       make(chan model.Message, 128),
	}
}

func (pl *PerfectLink) Send(to model.NodeID, msg model.Message) error {
	return pl.stubborn.Send(to, msg)
}

func (pl *PerfectLink) Deliver() <-chan model.Message {
	return pl.out
}

func (pl *PerfectLink) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err() // Simulates crash-stop
		case msg := <-pl.stubborn.Deliver():
			if _, seen := pl.delivered[msg.ID]; seen {
				continue // Drop duplicates
			}
			pl.delivered[msg.ID] = struct{}{}
			select {
			case pl.out <- msg:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}
```

### **2\. Wrapping Pattern for Remaining Chapter 2 Modules**

* **`SimTransport` (`transport/sim.go`):** Acts as a central in-memory router holding a `map[model.NodeID]chan model.Message`. Includes configurable middleware hooks to inject drop rates, artificial latency/jitter, and network partitions via explicit control calls like `Block(nodeA, nodeB)`.  
* **`FairLossLink` (`link/fairloss.go`):** Direct thin layer over `SimTransport` (or `net.UDPConn`). It guarantees that if a message is sent infinitely often, it will be delivered infinitely often, though individual packets may drop.  
* **`StubbornLink` (`link/stubborn.go`):** Wraps a `FairLossLink`. Retains sent messages in an internal buffer and runs a `time.Ticker` inside its `Run(ctx)` event loop to periodically retransmit all buffered messages until acknowledged or canceled.  
* **`FailureDetector` (`detector/failure.go`):** Implements Perfect (\$P\$) and Eventually Perfect (\$\\diamond P\$) failure detectors. Uses a `PerfectLink` to transmit periodic `HEARTBEAT` messages, maintains an active node state map, and emits `Suspect(NodeID)` or `Restore(NodeID)` events over output channels upon timeout threshold violations.  
* **`LeaderDetector` (`detector/leader.go`):** Implements an Eventual Leader Detector (\$\\Omega\$). Subscribes directly to \$\\diamond P\$ failure detector events and emits a `Trust(NodeID)` event whenever the deterministic lowest-ID non-suspected node changes.

## **6\. Abstraction Mapping Reference (Learning vs. Production)**

| Book Abstraction | Learning / Implementing the Book (Go Stdlib) | Production Go Equivalent |
| :---- | :---- | :---- |
| **Fair-Loss Link** | `net.UDPConn` (raw datagrams) or `SimTransport` | Rarely used directly unless building low-level custom protocols |
| **Stubborn Link** | `net.UDPConn` \+ `time.Ticker` retransmission goroutine | gRPC with retries, or durable message queue (NATS JetStream) |
| **Perfect Link** | Stubborn Link \+ `map[MsgID]struct{}` deduplication (or `net.TCPConn` \+ framing) | gRPC / TCP with application-level idempotency keys |
| **Failure Detectors (\$P\$, \$\\diamond P\$)** | Heartbeat goroutine (`time.Ticker`) emitting Suspect/Restore channel events | HashiCorp `memberlist` (SWIM gossip) or `etcd` lease mechanisms |
| **Leader Detector (\$\\Omega\$)** | Subscribes to \$\\diamond P\$ events; picks lowest-ID non-suspected process | `etcd` concurrency/election API, or `hashicorp/raft` consensus |

## **7\. Recommended Implementation Roadmap**

1. **Repository Setup:** Initialize the Go module and create the defined directory structural layout.  
2. **Core Model Definition:** Implement basic data contracts in `model/message.go` and abstraction boundaries in `link/link.go`.  
3. **Simulated Transport Layer:** Build `transport.SimTransport` featuring dynamic packet drop rates, artificial delays, and network partition toggles.  
4. **Link Hierarchy Verification:** Implement `FairLossLink` \$\\rightarrow\$ `StubbornLink` \$\\rightarrow\$ `PerfectLink`. Construct deterministic unit tests validating reliable delivery under simulated lossy network conditions.  
5. **Failure Detection Abstractions:** Implement Perfect (\$P\$) and Eventually Perfect (\$\\diamond P\$) failure detectors. Validate crash-stop semantics by canceling individual node contexts.  
6. **Leader Election Layer:** Implement Eventual Leader Detector (\$\\Omega\$) driven by output events from the \$\\diamond P\$ detector.  
7. **Broadcast Protocols:** Advance to Chapter 3 algorithms (Best-Effort and Reliable Broadcast) by composing your existing `PerfectLink` and `FailureDetector` implementations.  
8. **Real-World Networking:** Optionally implement a concrete `UDPTransport` (`net.UDPConn` with JSON/gob serialization) to verify algorithm correctness across isolated operating system processes.

