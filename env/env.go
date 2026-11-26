// Package env defines the minimal runtime boundary that distributed algorithms
// depend on for identity, cluster membership, virtual time, FIFO local event
// dispatch, unreliable transport, durable storage, and cryptography.
//
// Algorithm packages should import only env (and lower-layer algorithm
// interfaces), never net, os, sync, time.Now, or global math/rand. That keeps
// the exact same protocol code runnable inside the deterministic simulator and
// on real cloud VMs.
package env

import (
	"math"
	"math/rand/v2"
	"slices"
	"strconv"
	"time"
)

// NodeID uniquely identifies a process (replica or client) in the system.
type NodeID string

// InstanceID scopes a module instance when multiple instances of the same
// abstraction run concurrently on a process (e.g. multiple consensus instances).
type InstanceID string

// Time is an instant in nanoseconds since virtual epoch 0 (or Unix epoch in
// production). Arithmetic methods saturate at MinTime and MaxTime.
type Time int64

const (
	MinTime Time = math.MinInt64
	MaxTime Time = math.MaxInt64
)

// Add returns t advanced by d, saturating at MinTime or MaxTime on overflow.
func (t Time) Add(d time.Duration) Time {
	delta := int64(d)
	if delta > 0 && int64(t) > math.MaxInt64-delta {
		return MaxTime
	}
	if delta < 0 && int64(t) < math.MinInt64-delta {
		return MinTime
	}
	return Time(int64(t) + delta)
}

// Sub returns the elapsed duration t - u, saturating on overflow.
func (t Time) Sub(u Time) time.Duration {
	if u < 0 && int64(t) > math.MaxInt64+int64(u) {
		return time.Duration(math.MaxInt64)
	}
	if u > 0 && int64(t) < math.MinInt64+int64(u) {
		return time.Duration(math.MinInt64)
	}
	return time.Duration(int64(t) - int64(u))
}

// String formats t as a decimal nanosecond value.
func (t Time) String() string {
	return strconv.FormatInt(int64(t), 10) + "ns"
}

// Cluster represents a sorted, immutable view of process membership (Π).
type Cluster struct {
	Self  NodeID
	Peers []NodeID
}

// NewCluster constructs a Cluster with deduplicated, lexicographically sorted
// peer IDs so Rank and iteration order are deterministic.
func NewCluster(self NodeID, peers []NodeID) Cluster {
	sorted := slices.Clone(peers)
	slices.Sort(sorted)
	sorted = slices.Compact(sorted)
	return Cluster{
		Self:  self,
		Peers: sorted,
	}
}

// N returns the number of peers in the cluster.
func (c Cluster) N() int { return len(c.Peers) }

// Rank returns the 1-based deterministic rank of id in Peers, or 0 if absent.
func (c Cluster) Rank(id NodeID) int {
	idx, ok := slices.BinarySearch(c.Peers, id)
	if !ok {
		return 0
	}
	return idx + 1
}

// Contains reports whether id is a member of Peers.
func (c Cluster) Contains(id NodeID) bool { return c.Rank(id) > 0 }

// Majority returns floor(N/2)+1, or 0 if the cluster is empty.
func (c Cluster) Majority() int {
	if len(c.Peers) == 0 {
		return 0
	}
	return len(c.Peers)/2 + 1
}

// ByzantineQuorum returns ceil((N+f+1)/2) = (N+f)/2 + 1 for a Byzantine fault
// tolerance bound f, or 0 if N == 0 or f < 0.
func (c Cluster) ByzantineQuorum(f int) int {
	if len(c.Peers) == 0 || f < 0 {
		return 0
	}
	return (len(c.Peers)+f)/2 + 1
}

// PeersCopy returns an independent copy of Peers.
func (c Cluster) PeersCopy() []NodeID { return slices.Clone(c.Peers) }

// Dispatcher schedules local module events on a process's FIFO queue.
// Callbacks run mutually exclusively and never re-entrantly inside Enqueue.
type Dispatcher interface {
	Enqueue(fn func())
}

// Clock provides local time and single-shot timers whose callbacks run on the
// process's event loop, never synchronously inside AfterFunc.
type Clock interface {
	Now() Time
	AfterFunc(d time.Duration, fn func()) Timer
}

// Timer controls a pending callback created by Clock.AfterFunc.
type Timer interface {
	Stop() bool
	Reset(d time.Duration) bool
}

// RawTransport transmits unreliable point-to-point byte messages.
// Network drops, duplicates, reordering, and partitions happen silently.
type RawTransport interface {
	Send(to NodeID, data []byte) error
}

// Store is durable per-node key-value storage that survives Crash and Recover.
type Store interface {
	Store(key string, val []byte) error
	Retrieve(key string) ([]byte, bool, error)
	Delete(key string) error
	Keys(prefix string) ([]string, error)
}

// Process is a single-threaded distributed state machine hosted by one node
// incarnation. Calls into a Process are serialized by its host.
type Process interface {
	// Init runs once on a node's initial boot.
	Init()
	// Recover runs instead of Init when a crashed node restarts with its
	// durable Store intact.
	Recover()
	// OnMessage handles a raw byte message delivered from peer from.
	OnMessage(from NodeID, data []byte)
}

// BaseProcess provides no-op defaults for Process so simple components or
// tests can embed it and override only the hooks they need.
type BaseProcess struct{}

func (BaseProcess) Init()                    {}
func (BaseProcess) Recover()                 {}
func (BaseProcess) OnMessage(NodeID, []byte) {}

// Env bundles the identity and runtime dependencies supplied to a Process.
type Env struct {
	Self     NodeID
	Cluster  Cluster
	Dispatch Dispatcher
	Clock    Clock
	Rand     *rand.Rand
	Net      RawTransport
	Store    Store
	Keys     *KeyRing
	Log      Logger
}
