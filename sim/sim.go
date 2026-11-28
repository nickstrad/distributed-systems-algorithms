// Package sim implements a minimal, single-threaded deterministic simulator
// for distributed algorithms built against package env.
package sim

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"math/rand/v2"
	"runtime/debug"
	"slices"
	"time"

	"github.com/nickstrad/distributed-systems-algorithms/env"
)

const (
	defaultMaxSteps       int64 = 5_000_000
	defaultMaxSameInstant int64 = 100_000
	defaultTraceCap       int   = 512
)

// Sentinel simulator errors.
var (
	ErrBudgetExhausted = errors.New("sim: step budget exhausted")
	ErrTimeFrozen      = errors.New("sim: same-instant step limit exceeded")
)

// InvariantError wraps a violated safety invariant with failure coordinates.
type InvariantError struct {
	Name string
	Err  error
	Seed int64
	Step int64
	At   env.Time
}

func (e *InvariantError) Error() string {
	return fmt.Sprintf("sim: invariant %q failed at step=%d t=%s seed=%d: %v",
		e.Name, e.Step, e.At, e.Seed, e.Err)
}

func (e *InvariantError) Unwrap() error { return e.Err }

// PanicError captures a panic raised inside a simulated callback or invariant.
type PanicError struct {
	Value any
	Stack []byte
	Seed  int64
	Step  int64
	At    env.Time
}

func (e *PanicError) Error() string {
	return fmt.Sprintf("sim: panic at step=%d t=%s seed=%d: %v",
		e.Step, e.At, e.Seed, e.Value)
}

// TraceEntry records one executed simulator event for debugging and replay.
type TraceEntry struct {
	At     env.Time
	Seq    uint64
	Kind   string
	Detail string
}

func (e TraceEntry) String() string {
	if e.Detail == "" {
		return fmt.Sprintf("t=%s #%d %s", e.At, e.Seq, e.Kind)
	}
	return fmt.Sprintf("t=%s #%d %s %s", e.At, e.Seq, e.Kind, e.Detail)
}

// Config configures a Sim instance.
type Config struct {
	Seed           int64
	MaxSteps       int64
	MaxSameInstant int64
	TraceCap       int
	Net            NetConfig
	// LogLevel sets the minimum level recorded into the trace by node loggers.
	// Defaults to env.LevelInfo when LogEnabled is true.
	LogEnabled bool
	LogLevel   env.Level
	// NewFIFO constructs each node's local FIFO queue. Defaults to SliceFIFO.
	NewFIFO func() env.FIFO[func()]
}

type invariant struct {
	name string
	fn   func() error
}

type simNode struct {
	id             env.NodeID
	cluster        env.Cluster
	factory        func(env.Env) env.Process
	proc           env.Process
	alive          bool
	incarnation    uint64
	fifo           env.FIFO[func()]
	drainScheduled bool
	drainFn        func()
	timers         map[*simTimer]struct{}
	store          *memStore
	rng            *rand.Rand
	keys           *env.KeyRing
}

// Sim is a single-threaded, seed-deterministic discrete-event simulator.
type Sim struct {
	cfg         Config
	now         env.Time
	seq         uint64
	steps       int64
	sameInstant int64
	lastInstant env.Time
	q           eventQueue
	nodes       map[env.NodeID]*simNode
	net         *simNet
	invariants  []invariant
	err         error
	digest      hash.Hash
	digestBuf   [256]byte
	traceRing   []TraceEntry
	traceHead   int
	traceCount  int
	totalTraces int64
	stats       simStats
}

// New constructs a new deterministic simulator from cfg.
func New(cfg Config) *Sim {
	if cfg.MaxSteps <= 0 {
		cfg.MaxSteps = defaultMaxSteps
	}
	if cfg.MaxSameInstant <= 0 {
		cfg.MaxSameInstant = defaultMaxSameInstant
	}
	if cfg.TraceCap <= 0 {
		cfg.TraceCap = defaultTraceCap
	}
	if cfg.NewFIFO == nil {
		cfg.NewFIFO = func() env.FIFO[func()] { return env.NewSliceFIFO[func()]() }
	}
	s := &Sim{
		cfg:       cfg,
		nodes:     make(map[env.NodeID]*simNode),
		digest:    sha256.New(),
		traceRing: make([]TraceEntry, cfg.TraceCap),
	}
	s.net = newSimNet(s, cfg.Net)
	return s
}

// Seed returns the simulator's root PRNG seed.
func (s *Sim) Seed() int64 { return s.cfg.Seed }

// Now returns the current virtual time.
func (s *Sim) Now() env.Time { return s.now }

// Steps returns the number of executed events.
func (s *Sim) Steps() int64 { return s.steps }

// Err returns the first sticky error encountered by the simulator, if any.
func (s *Sim) Err() error { return s.err }

// Stream derives an isolated deterministic PCG stream for label.
func (s *Sim) Stream(label string) *rand.Rand {
	return NewStream(s.cfg.Seed, label)
}

// Digest returns the hex-encoded SHA-256 hash of all recorded trace entries.
func (s *Sim) Digest() string {
	return hex.EncodeToString(s.digest.Sum(nil))
}

// TraceTail returns up to the last n trace entries in chronological order.
func (s *Sim) TraceTail(n int) []TraceEntry {
	if n <= 0 || s.traceCount == 0 {
		return nil
	}
	if n > s.traceCount {
		n = s.traceCount
	}
	out := make([]TraceEntry, n)
	capLen := len(s.traceRing)
	start := (s.traceHead - n + capLen) % capLen
	for i := 0; i < n; i++ {
		out[i] = s.traceRing[(start+i)%capLen]
	}
	return out
}

// TraceLen returns the total number of trace entries recorded so far.
func (s *Sim) TraceLen() int64 { return s.totalTraces }

// Trace records a custom domain event in the trace buffer and running digest.
func (s *Sim) Trace(kind, detail string) {
	s.record(kind, detail)
}

func (s *Sim) record(kind, detail string) {
	entry := TraceEntry{
		At:     s.now,
		Seq:    s.seq,
		Kind:   kind,
		Detail: detail,
	}
	s.traceRing[s.traceHead] = entry
	s.traceHead = (s.traceHead + 1) % len(s.traceRing)
	if s.traceCount < len(s.traceRing) {
		s.traceCount++
	}
	s.totalTraces++

	binary.BigEndian.PutUint64(s.digestBuf[0:8], uint64(entry.At))
	binary.BigEndian.PutUint64(s.digestBuf[8:16], uint64(len(kind)))
	_, _ = s.digest.Write(s.digestBuf[:16])
	s.writeDigestString(kind)
	binary.BigEndian.PutUint64(s.digestBuf[0:8], uint64(len(detail)))
	_, _ = s.digest.Write(s.digestBuf[:8])
	s.writeDigestString(detail)
}

func (s *Sim) writeDigestString(str string) {
	for len(str) > 0 {
		n := copy(s.digestBuf[:], str)
		_, _ = s.digest.Write(s.digestBuf[:n])
		str = str[n:]
	}
}

// AddNode dynamically registers a new node and schedules its Init callback.
func (s *Sim) AddNode(id env.NodeID, peers []env.NodeID, factory func(env.Env) env.Process) error {
	return s.AddNodeWithKeys(id, peers, nil, factory)
}

// AddNodeWithKeys registers a new node with an explicit KeyRing.
func (s *Sim) AddNodeWithKeys(id env.NodeID, peers []env.NodeID, keys *env.KeyRing, factory func(env.Env) env.Process) error {
	if factory == nil {
		return fmt.Errorf("add node %q: nil factory: %w", id, env.ErrInvalidArgument)
	}
	if err := s.validateNewNodeID(id); err != nil {
		return fmt.Errorf("add node: %w", err)
	}
	node := &simNode{
		id:          id,
		cluster:     env.NewCluster(id, peers),
		factory:     factory,
		alive:       true,
		incarnation: 1,
		fifo:        s.cfg.NewFIFO(),
		timers:      make(map[*simTimer]struct{}),
		store:       newMemStore(),
		rng:         s.Stream("node/" + string(id)),
		keys:        keys,
	}
	s.nodes[id] = node
	s.stats.liveNodes.Inc()
	node.drainFn = s.makeDrainFn(node, node.incarnation)
	node.proc = factory(s.makeEnv(node))
	s.record("node.add", string(id))
	s.enqueueLocal(node, node.incarnation, node.proc.Init)
	return nil
}

func (s *Sim) validateNewNodeID(id env.NodeID) error {
	if id == "" {
		return fmt.Errorf("empty node ID: %w", env.ErrInvalidArgument)
	}
	if _, exists := s.nodes[id]; exists {
		return fmt.Errorf("duplicate node %q: %w", id, env.ErrInvalidArgument)
	}
	return nil
}

// AddCluster registers all distinct peers with shared deterministic
// cryptographic keys. An empty peer list is a no-op. Factories must not
// change node registrations while the cluster is being added.
//
//	post: validation errors leave node registrations and scheduled callbacks unchanged
func (s *Sim) AddCluster(peers []env.NodeID, factory func(env.Env) env.Process) error {
	if len(peers) == 0 {
		return nil
	}
	if factory == nil {
		return fmt.Errorf("add cluster: nil factory: %w", env.ErrInvalidArgument)
	}
	sorted := slices.Clone(peers)
	slices.Sort(sorted)
	sorted = slices.Compact(sorted)
	for _, id := range sorted {
		if err := s.validateNewNodeID(id); err != nil {
			return fmt.Errorf("add cluster: %w", err)
		}
	}
	rings, err := env.GenerateKeyRings(sorted, s.Stream("keys"))
	if err != nil {
		return fmt.Errorf("add cluster: generate key rings: %w", err)
	}
	for _, id := range sorted {
		if err := s.AddNodeWithKeys(id, sorted, rings[id], factory); err != nil {
			return fmt.Errorf("add cluster: %w", err)
		}
	}
	return nil
}

// Node returns the live Process for id, or nil if absent or crashed.
func (s *Sim) Node(id env.NodeID) env.Process {
	n, ok := s.nodes[id]
	if !ok || !n.alive {
		return nil
	}
	return n.proc
}

// IsAlive reports whether id exists and is currently running.
func (s *Sim) IsAlive(id env.NodeID) bool {
	n, ok := s.nodes[id]
	return ok && n.alive
}

// Crash halts node id immediately, clearing its local FIFO queue and timers
// while preserving its durable Store and PRNG stream.
func (s *Sim) Crash(id env.NodeID) error {
	n, ok := s.nodes[id]
	if !ok {
		return fmt.Errorf("crash node %q: %w", id, env.ErrUnknownPeer)
	}
	if !n.alive {
		return nil
	}
	n.alive = false
	n.incarnation++
	n.fifo.Clear()
	n.drainScheduled = false
	n.drainFn = nil
	for tm := range n.timers {
		if tm.ev != nil {
			s.q.remove(tm.ev)
			tm.ev = nil
		}
	}
	clear(n.timers)
	n.proc = nil
	s.stats.crashes.Inc()
	s.stats.liveNodes.Dec()
	s.record("node.crash", string(id))
	return nil
}

// Recover restarts a crashed node with fresh volatile state, its preserved
// durable Store, and a scheduled Recover callback.
func (s *Sim) Recover(id env.NodeID) error {
	n, ok := s.nodes[id]
	if !ok {
		return fmt.Errorf("recover node %q: %w", id, env.ErrUnknownPeer)
	}
	if n.alive {
		return nil
	}
	n.alive = true
	n.incarnation++
	n.drainFn = s.makeDrainFn(n, n.incarnation)
	n.proc = n.factory(s.makeEnv(n))
	s.stats.recoveries.Inc()
	s.stats.liveNodes.Inc()
	s.record("node.recover", string(id))
	s.enqueueLocal(n, n.incarnation, n.proc.Recover)
	return nil
}

// SetNetConfig updates simulated network latency and fault rates.
func (s *Sim) SetNetConfig(cfg NetConfig) {
	s.net.cfg = normalizeNetConfig(cfg)
	s.record("net.config", fmt.Sprintf("drop=%d dup=%d", s.net.cfg.DropPPM, s.net.cfg.DupPPM))
}

// Partition blocks messages sent from -> to.
func (s *Sim) Partition(from, to env.NodeID) {
	s.net.blocked[linkKey{from: from, to: to}] = true
	s.record("net.partition", fmt.Sprintf("%s->%s", from, to))
}

// PartitionBi blocks messages in both directions between a and b.
func (s *Sim) PartitionBi(a, b env.NodeID) {
	s.Partition(a, b)
	s.Partition(b, a)
}

// Isolate partitions id in both directions from every other registered node.
func (s *Sim) Isolate(id env.NodeID) {
	peers := make([]env.NodeID, 0, len(s.nodes))
	for other := range s.nodes {
		if other != id {
			peers = append(peers, other)
		}
	}
	slices.Sort(peers)
	for _, other := range peers {
		s.PartitionBi(id, other)
	}
}

// Heal removes a directed partition from -> to.
func (s *Sim) Heal(from, to env.NodeID) {
	delete(s.net.blocked, linkKey{from: from, to: to})
	s.record("net.heal", fmt.Sprintf("%s->%s", from, to))
}

// HealAll clears all network partitions.
func (s *Sim) HealAll() {
	clear(s.net.blocked)
	s.record("net.heal_all", "")
}

// Stabilize models Global Stabilization Time (GST) for partial synchrony:
// clears all partitions and switches the network to cfg.
func (s *Sim) Stabilize(cfg NetConfig) {
	clear(s.net.blocked)
	s.net.cfg = normalizeNetConfig(cfg)
	s.record("net.gst", fmt.Sprintf("min=%s max=%s", s.net.cfg.MinLatency, s.net.cfg.MaxLatency))
}

// AddInvariant registers a safety predicate checked after every simulator step.
func (s *Sim) AddInvariant(name string, fn func() error) {
	s.invariants = append(s.invariants, invariant{name: name, fn: fn})
}

// ScheduleAt schedules a test or workload callback at virtual time at.
func (s *Sim) ScheduleAt(at env.Time, fn func()) {
	if at < s.now {
		at = s.now
	}
	s.enqueueEvent(at, "sched", "", fn)
}

// ScheduleAfter schedules a test or workload callback after duration d.
func (s *Sim) ScheduleAfter(d time.Duration, fn func()) {
	if d < 0 {
		d = 0
	}
	s.enqueueEvent(s.now.Add(d), "sched", "", fn)
}

// Step executes the next scheduled event and verifies all registered invariants.
// Returns (false, nil) when the event queue is empty.
func (s *Sim) Step() (bool, error) {
	if s.err != nil {
		return false, s.err
	}
	if len(s.q) == 0 {
		return false, nil
	}
	if s.steps >= s.cfg.MaxSteps {
		s.err = ErrBudgetExhausted
		return false, s.err
	}

	ev := s.q.pop()
	if ev.at > s.now {
		s.now = ev.at
	}
	if s.steps == 0 || s.now != s.lastInstant {
		s.lastInstant = s.now
		s.sameInstant = 1
	} else {
		s.sameInstant++
		if s.sameInstant > s.cfg.MaxSameInstant {
			s.err = ErrTimeFrozen
			return false, s.err
		}
	}
	s.steps++
	s.record(ev.kind, ev.detail)

	if err := s.runProtected(ev.fn); err != nil {
		s.err = err
		return false, err
	}
	if err := s.checkInvariants(); err != nil {
		s.err = err
		return false, err
	}
	return true, nil
}

// RunUntil executes events with timestamp <= limit and advances virtual time to limit.
func (s *Sim) RunUntil(limit env.Time) error {
	if s.err != nil {
		return s.err
	}
	for len(s.q) > 0 && s.q[0].at <= limit {
		if _, err := s.Step(); err != nil {
			return err
		}
	}
	if limit > s.now {
		s.now = limit
	}
	return nil
}

// RunFor advances virtual time by d, executing all events scheduled in [now, now+d].
func (s *Sim) RunFor(d time.Duration) error {
	if d < 0 {
		d = 0
	}
	return s.RunUntil(s.now.Add(d))
}

// RunUntilQuiet drains events until the queue is empty or an error occurs.
func (s *Sim) RunUntilQuiet() error {
	if s.err != nil {
		return s.err
	}
	for len(s.q) > 0 {
		if _, err := s.Step(); err != nil {
			return err
		}
	}
	return nil
}

func (s *Sim) runProtected(fn func()) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = &PanicError{
				Value: r,
				Stack: debug.Stack(),
				Seed:  s.cfg.Seed,
				Step:  s.steps,
				At:    s.now,
			}
		}
	}()
	fn()
	return nil
}

func (s *Sim) checkInvariants() error {
	for _, inv := range s.invariants {
		var invErr error
		if pErr := s.runProtected(func() { invErr = inv.fn() }); pErr != nil {
			return pErr
		}
		if invErr != nil {
			return &InvariantError{
				Name: inv.name,
				Err:  invErr,
				Seed: s.cfg.Seed,
				Step: s.steps,
				At:   s.now,
			}
		}
	}
	return nil
}

func (s *Sim) enqueueEvent(at env.Time, kind, detail string, fn func()) *simEvent {
	s.seq++
	ev := &simEvent{
		at:     at,
		seq:    s.seq,
		hi:     -1,
		kind:   kind,
		detail: detail,
		fn:     fn,
	}
	s.q.push(ev)
	return ev
}

func (s *Sim) makeEnv(node *simNode) env.Env {
	inc := node.incarnation
	var logger env.Logger = env.NopLogger{}
	if s.cfg.LogEnabled {
		logger = simLogger{s: s, node: node.id, minLevel: s.cfg.LogLevel}
	}
	return env.Env{
		Self:     node.id,
		Cluster:  node.cluster,
		Dispatch: nodeDispatcher{s: s, node: node, inc: inc},
		Clock:    nodeClock{s: s, node: node, inc: inc},
		Rand:     node.rng,
		Net:      nodeTransport{net: s.net, node: node, inc: inc},
		Store:    node.store,
		Keys:     node.keys,
		Log:      logger,
	}
}

type simLogger struct {
	s        *Sim
	node     env.NodeID
	minLevel env.Level
}

func (l simLogger) Log(level env.Level, msg string, attrs ...env.Attr) {
	if level < l.minLevel {
		return
	}
	all := make([]env.Attr, 0, len(attrs)+1)
	all = append(all, env.Str("node", string(l.node)))
	all = append(all, attrs...)
	l.s.record("log."+level.String(), env.FormatLogAttrs(msg, all...))
}

func (l simLogger) Debug(msg string, attrs ...env.Attr) { l.Log(env.LevelDebug, msg, attrs...) }
func (l simLogger) Info(msg string, attrs ...env.Attr)  { l.Log(env.LevelInfo, msg, attrs...) }
func (l simLogger) Warn(msg string, attrs ...env.Attr)  { l.Log(env.LevelWarn, msg, attrs...) }
func (l simLogger) Error(msg string, attrs ...env.Attr) { l.Log(env.LevelError, msg, attrs...) }

func (s *Sim) enqueueLocal(node *simNode, inc uint64, fn func()) {
	if fn == nil || !node.alive || node.incarnation != inc {
		return
	}
	node.fifo.Push(fn)
	if !node.drainScheduled {
		node.drainScheduled = true
		s.scheduleDrain(node)
	}
}

func (s *Sim) scheduleDrain(node *simNode) {
	s.enqueueEvent(s.now, "local", string(node.id), node.drainFn)
}

func (s *Sim) makeDrainFn(node *simNode, inc uint64) func() {
	return func() {
		if !node.alive || node.incarnation != inc {
			return
		}
		fn, ok := node.fifo.Pop()
		if !ok {
			node.drainScheduled = false
			return
		}
		fn()
		if !node.alive || node.incarnation != inc {
			return
		}
		if node.fifo.Len() > 0 {
			s.scheduleDrain(node)
		} else {
			node.drainScheduled = false
		}
	}
}

type nodeDispatcher struct {
	s    *Sim
	node *simNode
	inc  uint64
}

func (d nodeDispatcher) Enqueue(fn func()) {
	d.s.enqueueLocal(d.node, d.inc, fn)
}

type nodeClock struct {
	s    *Sim
	node *simNode
	inc  uint64
}

func (c nodeClock) Now() env.Time { return c.s.now }

func (c nodeClock) AfterFunc(d time.Duration, fn func()) env.Timer {
	tm := &simTimer{
		s:    c.s,
		node: c.node,
		inc:  c.inc,
		fn:   fn,
	}
	if !c.node.alive || c.node.incarnation != c.inc {
		return tm
	}
	tm.arm(d)
	return tm
}

type simTimer struct {
	s    *Sim
	node *simNode
	inc  uint64
	fn   func()
	ev   *simEvent
}

func (t *simTimer) arm(d time.Duration) {
	if d < 0 {
		d = 0
	}
	t.node.timers[t] = struct{}{}
	t.ev = t.s.enqueueEvent(t.s.now.Add(d), "timer", string(t.node.id), func() {
		active := t.ev != nil
		t.ev = nil
		if !active || !t.node.alive || t.node.incarnation != t.inc {
			return
		}
		delete(t.node.timers, t)
		if t.fn != nil {
			t.fn()
		}
	})
}

func (t *simTimer) Stop() bool {
	if t.ev == nil {
		return false
	}
	t.s.q.remove(t.ev)
	t.ev = nil
	delete(t.node.timers, t)
	return true
}

func (t *simTimer) Reset(d time.Duration) bool {
	was := t.Stop()
	if !t.node.alive || t.node.incarnation != t.inc {
		return was
	}
	t.arm(d)
	return was
}
