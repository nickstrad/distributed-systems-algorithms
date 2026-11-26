// Story: The discrete-event simulator executes deterministic schedules, network faults, crashes, and safety guards.
//
// What these tests prove: (Time, Seq) event ordering, independent PRNG streams, latency/drop/dup/partition/heal/GST faults, crash/recover reset vs durable store, sticky invariant/panic halts, livelock guards, and SliceFIFO/ChanFIFO digest parity.
// How they probe: instrumented hookProcess nodes driving targeted schedules, fault transitions, invariant/panic triggers, and MaxSteps/MaxSameInstant exhaustion checks.
// What they do not prove: randomized property invariants across arbitrary topologies or obs.Source/env.Logger integration (covered in sim/prop_test.go).
package sim_test

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nickstrad/distributed-systems-algorithms/env"
	"github.com/nickstrad/distributed-systems-algorithms/sim"
)

type hookProcess struct {
	env.BaseProcess
	env       env.Env
	onInit    func(env.Env)
	onRecover func(env.Env)
	onMsg     func(env.Env, env.NodeID, []byte)
}

func (p *hookProcess) Init() {
	if p.onInit != nil {
		p.onInit(p.env)
	}
}

func (p *hookProcess) Recover() {
	if p.onRecover != nil {
		p.onRecover(p.env)
	}
}

func (p *hookProcess) OnMessage(from env.NodeID, data []byte) {
	if p.onMsg != nil {
		p.onMsg(p.env, from, data)
	}
}

func TestOrderingAndTimers(t *testing.T) {
	s := sim.New(sim.Config{Seed: 1})

	var order []string
	var nodeEnv env.Env
	err := s.AddNode("n1", []env.NodeID{"n1"}, func(e env.Env) env.Process {
		nodeEnv = e
		return &hookProcess{
			env: e,
			onInit: func(e env.Env) {
				order = append(order, "init")
			},
		}
	})
	if err != nil {
		t.Fatalf("AddNode error: %v", err)
	}

	// Same timestamp events must break ties by scheduling sequence (seq).
	s.ScheduleAt(200, func() { order = append(order, "t200") })
	s.ScheduleAt(100, func() { order = append(order, "t100-a") })
	s.ScheduleAt(100, func() { order = append(order, "t100-b") })
	s.ScheduleAt(100, func() { order = append(order, "t100-c") })
	s.ScheduleAt(50, func() { order = append(order, "t50") })

	// Run through t=0 (init) so nodeEnv is ready, then verify timers.
	if err := s.RunUntil(0); err != nil {
		t.Fatalf("RunUntil(0) error: %v", err)
	}

	var stoppedFired bool
	var resetFires []env.Time

	tStop := nodeEnv.Clock.AfterFunc(30, func() {
		stoppedFired = true
	})
	tReset := nodeEnv.Clock.AfterFunc(40, func() {
		resetFires = append(resetFires, nodeEnv.Clock.Now())
	})

	s.ScheduleAt(10, func() {
		// Past ScheduleAt clamps to Now().
		s.ScheduleAt(1, func() {
			order = append(order, fmt.Sprintf("clamped@%d", s.Now()))
		})
		// Negative ScheduleAfter clamps to 0.
		s.ScheduleAfter(-5*time.Millisecond, func() {
			order = append(order, fmt.Sprintf("neg@%d", s.Now()))
		})
		if !tStop.Stop() {
			t.Error("first Stop() = false, want true")
		}
		if tStop.Stop() {
			t.Error("second Stop() = true, want false")
		}
		if !tReset.Reset(60) {
			t.Error("Reset on pending timer = false, want true")
		}
	})

	if err := s.RunUntil(150); err != nil {
		t.Fatalf("RunUntil(150) error: %v", err)
	}
	if s.Now() != 150 {
		t.Fatalf("Now() = %d, want 150", s.Now())
	}

	// Fired timer: Stop returns false, Reset returns false and re-arms.
	if tReset.Stop() {
		t.Error("Stop after firing = true, want false")
	}
	if tReset.Reset(10) {
		t.Error("Reset after firing = true, want false")
	}
	if err := s.RunFor(20); err != nil {
		t.Fatalf("RunFor(20) error: %v", err)
	}
	if err := s.RunUntilQuiet(); err != nil {
		t.Fatalf("RunUntilQuiet error: %v", err)
	}

	wantOrder := []string{"init", "clamped@10", "neg@10", "t50", "t100-a", "t100-b", "t100-c", "t200"}
	if !slices.Equal(order, wantOrder) {
		t.Fatalf("execution order = %v, want %v", order, wantOrder)
	}
	if stoppedFired {
		t.Fatal("stopped timer fired")
	}
	wantResetFires := []env.Time{70, 160}
	if !slices.Equal(resetFires, wantResetFires) {
		t.Fatalf("resetFires = %v, want %v", resetFires, wantResetFires)
	}
}

func TestNewStreamIsolation(t *testing.T) {
	a1 := sim.NewStream(42, "alpha")
	b1 := sim.NewStream(42, "beta")

	// Drawing heavily from alpha must not perturb beta.
	for i := 0; i < 100; i++ {
		_ = a1.Uint64()
	}
	gotB := make([]uint64, 8)
	for i := range gotB {
		gotB[i] = b1.Uint64()
	}

	b2 := sim.NewStream(42, "beta")
	s := sim.New(sim.Config{Seed: 42})
	b3 := s.Stream("beta")
	for i, want := range gotB {
		if g2 := b2.Uint64(); g2 != want {
			t.Fatalf("b2[%d] = %d, want %d", i, g2, want)
		}
		if g3 := b3.Uint64(); g3 != want {
			t.Fatalf("s.Stream(beta)[%d] = %d, want %d", i, g3, want)
		}
	}

	aFresh := sim.NewStream(42, "alpha")
	if aFresh.Uint64() == gotB[0] {
		t.Fatal("distinct stream labels produced identical first draw")
	}
}

func TestSimNetLatencyDropDupAndErrors(t *testing.T) {
	const minLat = 5 * time.Millisecond
	const maxLat = 15 * time.Millisecond

	s := sim.New(sim.Config{
		Seed: 99,
		Net: sim.NetConfig{
			MinLatency: minLat,
			MaxLatency: maxLat,
		},
	})

	type delivery struct {
		from env.NodeID
		data []byte
		at   env.Time
	}
	var delivered []delivery
	var envs = make(map[env.NodeID]env.Env)

	makeNode := func(e env.Env) env.Process {
		envs[e.Self] = e
		return &hookProcess{
			env: e,
			onMsg: func(e env.Env, from env.NodeID, data []byte) {
				delivered = append(delivered, delivery{
					from: from,
					data: slices.Clone(data),
					at:   e.Clock.Now(),
				})
				// Mutate received slice in-place to verify DupPPM clones independently.
				if len(data) > 0 {
					data[0] ^= 0xff
				}
			},
		}
	}
	for _, id := range []env.NodeID{"a", "b"} {
		if err := s.AddNode(id, []env.NodeID{"a", "b"}, makeNode); err != nil {
			t.Fatalf("AddNode(%s): %v", id, err)
		}
	}
	if err := s.RunUntilQuiet(); err != nil {
		t.Fatalf("boot RunUntilQuiet: %v", err)
	}

	// Unknown peer error.
	if err := envs["a"].Net.Send("missing", []byte("hi")); !errors.Is(err, env.ErrUnknownPeer) {
		t.Fatalf("Send(missing) = %v, want ErrUnknownPeer", err)
	}

	// Latency range check across 40 messages.
	distinctLat := make(map[time.Duration]struct{})
	for i := 0; i < 40; i++ {
		sendAt := envs["a"].Clock.Now()
		if err := envs["a"].Net.Send("b", []byte{byte(i)}); err != nil {
			t.Fatalf("Send error: %v", err)
		}
		if err := s.RunUntilQuiet(); err != nil {
			t.Fatalf("RunUntilQuiet error: %v", err)
		}
		if len(delivered) != i+1 {
			t.Fatalf("delivered count = %d, want %d", len(delivered), i+1)
		}
		lat := delivered[i].at.Sub(sendAt)
		if lat < minLat || lat > maxLat {
			t.Fatalf("latency %v out of [%v, %v]", lat, minLat, maxLat)
		}
		distinctLat[lat] = struct{}{}
	}
	if len(distinctLat) < 2 {
		t.Fatalf("expected multiple distinct latencies in range, got %d", len(distinctLat))
	}

	// DropPPM = PPM drops all messages.
	delivered = nil
	s.SetNetConfig(sim.NetConfig{MinLatency: 1 * time.Millisecond, MaxLatency: 1 * time.Millisecond, DropPPM: sim.PPM})
	for i := 0; i < 10; i++ {
		if err := envs["a"].Net.Send("b", []byte("drop")); err != nil {
			t.Fatalf("Send error: %v", err)
		}
	}
	if err := s.RunUntilQuiet(); err != nil {
		t.Fatalf("RunUntilQuiet error: %v", err)
	}
	if len(delivered) != 0 {
		t.Fatalf("DropPPM=PPM delivered %d messages, want 0", len(delivered))
	}

	// Partial DropPPM (50%) drops some and delivers some.
	s.SetNetConfig(sim.NetConfig{MinLatency: 1 * time.Millisecond, MaxLatency: 1 * time.Millisecond, DropPPM: sim.PPM / 2})
	for i := 0; i < 40; i++ {
		_ = envs["a"].Net.Send("b", []byte("half"))
	}
	if err := s.RunUntilQuiet(); err != nil {
		t.Fatalf("RunUntilQuiet error: %v", err)
	}
	if len(delivered) == 0 || len(delivered) == 40 {
		t.Fatalf("DropPPM=50%% delivered %d/40 messages, expected partial delivery", len(delivered))
	}

	// DupPPM = PPM delivers independent cloned slices unaffected by caller or receiver mutations.
	delivered = nil
	s.SetNetConfig(sim.NetConfig{
		MinLatency:    2 * time.Millisecond,
		MaxLatency:    2 * time.Millisecond,
		DropPPM:       0,
		DupPPM:        sim.PPM,
		MaxDuplicates: 2,
	})
	payload := []byte("immutable-payload")
	if err := envs["a"].Net.Send("b", payload); err != nil {
		t.Fatalf("Send error: %v", err)
	}
	payload[0] = 'X' // Caller mutates original slice immediately after Send.
	if err := s.RunUntilQuiet(); err != nil {
		t.Fatalf("RunUntilQuiet error: %v", err)
	}
	if len(delivered) < 2 || len(delivered) > 3 {
		t.Fatalf("DupPPM=PPM MaxDuplicates=2 delivered %d copies, want 2 or 3", len(delivered))
	}
	for i, d := range delivered {
		if string(d.data) != "immutable-payload" {
			t.Fatalf("copy %d got %q, want %q", i, d.data, "immutable-payload")
		}
	}

	// Stale transport after Crash returns ErrClosed.
	if err := s.Crash("a"); err != nil {
		t.Fatalf("Crash(a) error: %v", err)
	}
	if err := envs["a"].Net.Send("b", []byte("after-crash")); !errors.Is(err, env.ErrClosed) {
		t.Fatalf("Send after crash = %v, want ErrClosed", err)
	}
}

func TestSimNetPartitionsHealAndStabilize(t *testing.T) {
	s := sim.New(sim.Config{
		Seed: 7,
		Net:  sim.NetConfig{MinLatency: 5 * time.Millisecond, MaxLatency: 5 * time.Millisecond},
	})

	peers := []env.NodeID{"n1", "n2", "n3"}
	envs := make(map[env.NodeID]env.Env)
	var received []string

	for _, id := range peers {
		if err := s.AddNode(id, peers, func(e env.Env) env.Process {
			envs[e.Self] = e
			return &hookProcess{
				env: e,
				onMsg: func(e env.Env, from env.NodeID, data []byte) {
					received = append(received, fmt.Sprintf("%s->%s:%s", from, e.Self, data))
				},
			}
		}); err != nil {
			t.Fatalf("AddNode(%s): %v", id, err)
		}
	}
	if err := s.RunUntilQuiet(); err != nil {
		t.Fatalf("boot: %v", err)
	}

	sendPair := func(from, to env.NodeID, msg string) {
		if err := envs[from].Net.Send(to, []byte(msg)); err != nil {
			t.Fatalf("Send(%s->%s): %v", from, to, err)
		}
	}

	// Asymmetric partition: block n1->n2 only.
	s.Partition("n1", "n2")
	sendPair("n1", "n2", "p1")
	sendPair("n2", "n1", "p2")
	if err := s.RunUntilQuiet(); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(received, []string{"n2->n1:p2"}) {
		t.Fatalf("asymmetric partition got %v, want [n2->n1:p2]", received)
	}

	// Heal restores n1->n2.
	received = nil
	s.Heal("n1", "n2")
	sendPair("n1", "n2", "h1")
	if err := s.RunUntilQuiet(); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(received, []string{"n1->n2:h1"}) {
		t.Fatalf("after Heal got %v, want [n1->n2:h1]", received)
	}

	// PartitionBi blocks both; Heal one side leaves reverse blocked; HealAll clears all.
	received = nil
	s.PartitionBi("n1", "n2")
	sendPair("n1", "n2", "bi1")
	sendPair("n2", "n1", "bi2")
	if err := s.RunUntilQuiet(); err != nil {
		t.Fatal(err)
	}
	if len(received) != 0 {
		t.Fatalf("PartitionBi got %v, want empty", received)
	}
	s.Heal("n1", "n2")
	sendPair("n1", "n2", "one")
	sendPair("n2", "n1", "two")
	if err := s.RunUntilQuiet(); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(received, []string{"n1->n2:one"}) {
		t.Fatalf("half-healed got %v, want [n1->n2:one]", received)
	}
	received = nil
	s.HealAll()
	sendPair("n2", "n1", "all")
	if err := s.RunUntilQuiet(); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(received, []string{"n2->n1:all"}) {
		t.Fatalf("after HealAll got %v, want [n2->n1:all]", received)
	}

	// Isolate n1 from everyone else while n2<->n3 remains connected.
	received = nil
	s.Isolate("n1")
	sendPair("n1", "n2", "iso1")
	sendPair("n3", "n1", "iso2")
	sendPair("n2", "n3", "ok")
	if err := s.RunUntilQuiet(); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(received, []string{"n2->n3:ok"}) {
		t.Fatalf("during Isolate got %v, want [n2->n3:ok]", received)
	}

	// Stabilize (GST) clears partitions and switches NetConfig.
	received = nil
	s.Stabilize(sim.NetConfig{MinLatency: 2 * time.Millisecond, MaxLatency: 2 * time.Millisecond, DropPPM: 0})
	t0 := s.Now()
	sendPair("n1", "n3", "gst")
	if err := s.RunUntilQuiet(); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(received, []string{"n1->n3:gst"}) {
		t.Fatalf("after Stabilize got %v, want [n1->n3:gst]", received)
	}
	if gotLat := s.Now().Sub(t0); gotLat != 2*time.Millisecond {
		t.Fatalf("latency after Stabilize = %v, want 2ms", gotLat)
	}
}

func TestCrashAndRecover(t *testing.T) {
	const seed int64 = 31415
	s := sim.New(sim.Config{
		Seed: seed,
		Net:  sim.NetConfig{MinLatency: 5 * time.Millisecond, MaxLatency: 5 * time.Millisecond},
	})

	type nodeState struct {
		env         env.Env
		initCalls   int
		recoverCall int
		volatileVal int
		timerFired  bool
		localFired  bool
		msgAfter    bool
	}

	var cur *nodeState
	var preDraws []uint64
	var postDraw uint64

	factory := func(e env.Env) env.Process {
		st := &nodeState{env: e}
		cur = st
		return &hookProcess{
			env: e,
			onInit: func(e env.Env) {
				st.initCalls++
				st.volatileVal = 99
			},
			onRecover: func(e env.Env) {
				st.recoverCall++
				postDraw = e.Rand.Uint64()
			},
			onMsg: func(e env.Env, from env.NodeID, data []byte) {
				st.msgAfter = true
			},
		}
	}

	if err := s.AddNode("n1", []env.NodeID{"n1", "n2"}, factory); err != nil {
		t.Fatalf("AddNode(n1): %v", err)
	}
	var n2Env env.Env
	if err := s.AddNode("n2", []env.NodeID{"n1", "n2"}, func(e env.Env) env.Process {
		n2Env = e
		return &hookProcess{env: e}
	}); err != nil {
		t.Fatalf("AddNode(n2): %v", err)
	}

	if err := s.RunUntilQuiet(); err != nil {
		t.Fatalf("boot: %v", err)
	}
	if cur.initCalls != 1 || cur.volatileVal != 99 || !s.IsAlive("n1") || s.Node("n1") == nil {
		t.Fatalf("unexpected initial state: %+v alive=%v", cur, s.IsAlive("n1"))
	}

	// Exercise durable Store cloning + Keys + Delete.
	val := []byte("durable-one")
	if err := cur.env.Store.Store("k/b", val); err != nil {
		t.Fatalf("Store error: %v", err)
	}
	val[0] = 'X' // Mutate caller slice after Store.
	if err := cur.env.Store.Store("k/a", []byte("durable-zero")); err != nil {
		t.Fatalf("Store error: %v", err)
	}
	if err := cur.env.Store.Store("other", []byte("keep")); err != nil {
		t.Fatalf("Store error: %v", err)
	}
	gotB, ok, err := cur.env.Store.Retrieve("k/b")
	if err != nil || !ok || string(gotB) != "durable-one" {
		t.Fatalf("Retrieve(k/b) = (%q, %v, %v)", gotB, ok, err)
	}
	gotB[0] = 'Y' // Mutate returned slice after Retrieve.
	if err := cur.env.Store.Store("k/c", []byte("to-delete")); err != nil {
		t.Fatalf("Store error: %v", err)
	}
	if err := cur.env.Store.Delete("k/c"); err != nil {
		t.Fatalf("Delete error: %v", err)
	}
	keys, err := cur.env.Store.Keys("k/")
	if err != nil || !slices.Equal(keys, []string{"k/a", "k/b"}) {
		t.Fatalf("Keys(k/) = (%v, %v), want [k/a k/b]", keys, err)
	}

	// Draw 3 values from n1's PRNG stream before crash.
	for i := 0; i < 3; i++ {
		preDraws = append(preDraws, cur.env.Rand.Uint64())
	}

	// Arm a timer, queue local FIFO callbacks, send an in-flight message, then Crash.
	preCrashState := cur
	preCrashState.env.Clock.AfterFunc(20*time.Millisecond, func() {
		preCrashState.timerFired = true
	})
	preCrashState.env.Dispatch.Enqueue(func() {
		preCrashState.localFired = true
	})
	if err := n2Env.Net.Send("n1", []byte("in-flight")); err != nil {
		t.Fatalf("Send error: %v", err)
	}

	if err := s.Crash("n1"); err != nil {
		t.Fatalf("Crash(n1) error: %v", err)
	}
	if err := s.Crash("n1"); err != nil {
		t.Fatalf("idempotent Crash(n1) error: %v", err)
	}
	if s.IsAlive("n1") || s.Node("n1") != nil {
		t.Fatal("expected n1 to be dead and Node(n1)==nil after Crash")
	}
	if err := s.Crash("unknown"); !errors.Is(err, env.ErrUnknownPeer) {
		t.Fatalf("Crash(unknown) = %v, want ErrUnknownPeer", err)
	}
	if err := s.Recover("unknown"); !errors.Is(err, env.ErrUnknownPeer) {
		t.Fatalf("Recover(unknown) = %v, want ErrUnknownPeer", err)
	}

	// Advance time past in-flight delivery and timer expiry while crashed.
	if err := s.RunUntilQuiet(); err != nil {
		t.Fatalf("RunUntilQuiet while crashed: %v", err)
	}
	if preCrashState.timerFired || preCrashState.localFired || preCrashState.msgAfter {
		t.Fatalf("crashed callbacks ran: timer=%v local=%v msg=%v",
			preCrashState.timerFired, preCrashState.localFired, preCrashState.msgAfter)
	}

	// Recover n1: Recover() runs instead of Init(), volatile state is wiped,
	// Store survives, and PRNG continues forward without replaying preDraws.
	if err := s.Recover("n1"); err != nil {
		t.Fatalf("Recover(n1) error: %v", err)
	}
	if err := s.Recover("n1"); err != nil {
		t.Fatalf("idempotent Recover(n1) error: %v", err)
	}
	if err := s.RunUntilQuiet(); err != nil {
		t.Fatalf("RunUntilQuiet after Recover: %v", err)
	}
	if !s.IsAlive("n1") || s.Node("n1") == nil {
		t.Fatal("expected n1 alive after Recover")
	}
	if cur == preCrashState {
		t.Fatal("Recover did not construct a fresh Process instance")
	}
	if cur.initCalls != 0 || cur.recoverCall != 1 || cur.volatileVal != 0 {
		t.Fatalf("unexpected recovered state: init=%d recover=%d volatile=%d",
			cur.initCalls, cur.recoverCall, cur.volatileVal)
	}
	if preCrashState.timerFired || preCrashState.localFired {
		t.Fatal("stale timer or FIFO callback ran after Recover")
	}

	// Verify Store durability and isolation after Recover.
	gotAfter, ok, err := cur.env.Store.Retrieve("k/b")
	if err != nil || !ok || string(gotAfter) != "durable-one" {
		t.Fatalf("post-recover Retrieve(k/b) = (%q, %v, %v)", gotAfter, ok, err)
	}
	if _, ok, _ := cur.env.Store.Retrieve("k/c"); ok {
		t.Fatal("deleted key k/c reappeared after Recover")
	}
	allKeys, err := cur.env.Store.Keys("")
	if err != nil || !slices.Equal(allKeys, []string{"k/a", "k/b", "other"}) {
		t.Fatalf("post-recover Keys(\"\") = (%v, %v)", allKeys, err)
	}

	// Verify PRNG stream continued cleanly from draw #4 instead of replaying draws #1..3.
	ref := sim.NewStream(seed, "node/n1")
	for i := 0; i < 3; i++ {
		if g := ref.Uint64(); g != preDraws[i] {
			t.Fatalf("ref preDraw[%d] = %d, want %d", i, g, preDraws[i])
		}
	}
	wantPostDraw := ref.Uint64()
	if postDraw != wantPostDraw {
		t.Fatalf("post-recover PRNG draw = %d, want %d (stream did not advance cleanly)", postDraw, wantPostDraw)
	}
}

func TestDynamicAddNodeAndAddCluster(t *testing.T) {
	s := sim.New(sim.Config{
		Seed: 555,
		Net:  sim.NetConfig{MinLatency: 1 * time.Millisecond, MaxLatency: 1 * time.Millisecond},
	})

	// Validation checks on AddNode.
	if err := s.AddNode("", []env.NodeID{"a"}, func(e env.Env) env.Process { return env.BaseProcess{} }); !errors.Is(err, env.ErrInvalidArgument) {
		t.Fatalf("AddNode empty id err = %v, want ErrInvalidArgument", err)
	}
	if err := s.AddNode("a", []env.NodeID{"a"}, nil); !errors.Is(err, env.ErrInvalidArgument) {
		t.Fatalf("AddNode nil factory err = %v, want ErrInvalidArgument", err)
	}

	var verifiedSigs int
	var verifiedMACs int
	var clientReplies int

	clusterFactory := func(e env.Env) env.Process {
		return &hookProcess{
			env: e,
			onInit: func(e env.Env) {
				if e.Self == "a" {
					msg := env.Envelope{
						Instance: "auth",
						MsgID:    "m1",
						From:     "a",
						Type:     "signed",
						Payload:  []byte("hello-b"),
					}
					msg.Auth = e.Keys.Sign(msg.CanonicalBytes())
					raw, err := env.MarshalEnvelope(msg)
					if err != nil {
						t.Fatalf("MarshalEnvelope: %v", err)
					}
					if err := e.Net.Send("b", raw); err != nil {
						t.Fatalf("Send(b): %v", err)
					}

					macMsg := env.Envelope{
						Instance: "auth",
						MsgID:    "m2",
						From:     "a",
						Type:     "mac",
						Payload:  []byte("hello-c"),
					}
					macMsg.Auth = e.Keys.MAC("c", macMsg.CanonicalBytes())
					rawMAC, err := env.MarshalEnvelope(macMsg)
					if err != nil {
						t.Fatalf("MarshalEnvelope: %v", err)
					}
					if err := e.Net.Send("c", rawMAC); err != nil {
						t.Fatalf("Send(c): %v", err)
					}
				}
			},
			onMsg: func(e env.Env, from env.NodeID, data []byte) {
				if string(data) == "ping" {
					_ = e.Net.Send(from, []byte("pong"))
					return
				}
				msg, err := env.UnmarshalEnvelope(data)
				if err != nil {
					t.Fatalf("UnmarshalEnvelope: %v", err)
				}
				switch msg.Type {
				case "signed":
					if !e.Keys.Verify(from, msg.CanonicalBytes(), msg.Auth) {
						t.Fatalf("%s failed to verify signature from %s", e.Self, from)
					}
					// Tampered payload must fail verification.
					tampered := msg
					tampered.Payload = []byte("tampered")
					if e.Keys.Verify(from, tampered.CanonicalBytes(), tampered.Auth) {
						t.Fatal("verified tampered envelope")
					}
					verifiedSigs++
				case "mac":
					if !e.Keys.VerifyMAC(from, msg.CanonicalBytes(), msg.Auth) {
						t.Fatalf("%s failed to verify MAC from %s", e.Self, from)
					}
					verifiedMACs++
				}
			},
		}
	}

	if err := s.AddCluster([]env.NodeID{"c", "a", "b", "a"}, clusterFactory); err != nil {
		t.Fatalf("AddCluster error: %v", err)
	}
	if err := s.AddNode("a", []env.NodeID{"a"}, func(e env.Env) env.Process { return env.BaseProcess{} }); !errors.Is(err, env.ErrInvalidArgument) {
		t.Fatalf("duplicate AddNode err = %v, want ErrInvalidArgument", err)
	}

	if err := s.RunUntil(env.Time(20 * time.Millisecond)); err != nil {
		t.Fatalf("RunUntil(20ms) error: %v", err)
	}
	if verifiedSigs != 1 || verifiedMACs != 1 {
		t.Fatalf("verifiedSigs=%d verifiedMACs=%d, want 1 and 1", verifiedSigs, verifiedMACs)
	}

	// Dynamically add a client node mid-run at t=20ms.
	var clientInitAt env.Time = -1
	err := s.AddNode("client-1", []env.NodeID{"a", "b", "c"}, func(e env.Env) env.Process {
		return &hookProcess{
			env: e,
			onInit: func(e env.Env) {
				clientInitAt = e.Clock.Now()
				if err := e.Net.Send("a", []byte("ping")); err != nil {
					t.Fatalf("client Send(a): %v", err)
				}
			},
			onMsg: func(e env.Env, from env.NodeID, data []byte) {
				if from == "a" && string(data) == "pong" {
					clientReplies++
				}
			},
		}
	})
	if err != nil {
		t.Fatalf("dynamic AddNode(client-1): %v", err)
	}

	if err := s.RunUntilQuiet(); err != nil {
		t.Fatalf("RunUntilQuiet after dynamic AddNode: %v", err)
	}
	if clientInitAt != env.Time(20*time.Millisecond) {
		t.Fatalf("clientInitAt = %s, want 20ms", clientInitAt)
	}
	if clientReplies != 1 {
		t.Fatalf("clientReplies = %d, want 1", clientReplies)
	}
}

func TestInvariantErrorAndSticky(t *testing.T) {
	sentinel := errors.New("counter exceeded 2")
	s := sim.New(sim.Config{Seed: 12})

	count := 0
	s.AddInvariant("bound", func() error {
		if count > 2 {
			return sentinel
		}
		return nil
	})

	afterViolationRan := false
	s.ScheduleAt(10, func() { count = 1 })
	s.ScheduleAt(20, func() { count = 2 })
	s.ScheduleAt(30, func() { count = 3 })
	s.ScheduleAt(40, func() { afterViolationRan = true })

	err := s.RunUntilQuiet()
	var invErr *sim.InvariantError
	if !errors.As(err, &invErr) {
		t.Fatalf("err = %T (%v), want *sim.InvariantError", err, err)
	}
	if invErr.Name != "bound" || !errors.Is(err, sentinel) || invErr.Seed != 12 || invErr.Step != 3 || invErr.At != 30 {
		t.Fatalf("unexpected InvariantError fields: %+v", invErr)
	}
	if !strings.Contains(invErr.Error(), "bound") {
		t.Fatalf("InvariantError.Error() = %q, expected invariant name", invErr.Error())
	}
	if afterViolationRan {
		t.Fatal("callback after invariant violation executed")
	}

	// Sticky error checks.
	if s.Err() != err {
		t.Fatalf("s.Err() = %v, want %v", s.Err(), err)
	}
	stepsAtErr := s.Steps()
	if ok, stepErr := s.Step(); ok || stepErr != err {
		t.Fatalf("Step after error = (%v, %v), want (false, %v)", ok, stepErr, err)
	}
	if uErr := s.RunUntil(100); uErr != err {
		t.Fatalf("RunUntil after error = %v, want %v", uErr, err)
	}
	if fErr := s.RunFor(100); fErr != err {
		t.Fatalf("RunFor after error = %v, want %v", fErr, err)
	}
	if qErr := s.RunUntilQuiet(); qErr != err {
		t.Fatalf("RunUntilQuiet after error = %v, want %v", qErr, err)
	}
	if s.Steps() != stepsAtErr || afterViolationRan {
		t.Fatal("simulator executed steps after sticky error")
	}
}

func TestPanicInCallbackAndInvariant(t *testing.T) {
	s := sim.New(sim.Config{Seed: 77})
	afterPanicRan := false
	s.ScheduleAt(10, func() { panic("boom-callback") })
	s.ScheduleAt(20, func() { afterPanicRan = true })

	err := s.RunUntilQuiet()
	var pErr *sim.PanicError
	if !errors.As(err, &pErr) {
		t.Fatalf("err = %T (%v), want *sim.PanicError", err, err)
	}
	if pErr.Value != "boom-callback" || len(pErr.Stack) == 0 || pErr.Seed != 77 || pErr.Step != 1 || pErr.At != 10 {
		t.Fatalf("unexpected PanicError: value=%v stackLen=%d seed=%d step=%d at=%d",
			pErr.Value, len(pErr.Stack), pErr.Seed, pErr.Step, pErr.At)
	}
	if !strings.Contains(pErr.Error(), "boom-callback") {
		t.Fatalf("PanicError.Error() = %q", pErr.Error())
	}
	if afterPanicRan || s.Err() != err {
		t.Fatal("simulation did not halt cleanly with sticky PanicError")
	}

	// Panic inside an invariant also produces *sim.PanicError.
	s2 := sim.New(sim.Config{Seed: 88})
	s2.AddInvariant("panicking", func() error { panic("boom-inv") })
	s2.ScheduleAt(5, func() {})
	err2 := s2.RunUntilQuiet()
	var pErr2 *sim.PanicError
	if !errors.As(err2, &pErr2) || pErr2.Value != "boom-inv" {
		t.Fatalf("invariant panic = %v, want PanicError(boom-inv)", err2)
	}
}

func TestLivelockGuards(t *testing.T) {
	// MaxSteps -> ErrBudgetExhausted.
	s1 := sim.New(sim.Config{Seed: 1, MaxSteps: 5})
	var reschedule func()
	reschedule = func() {
		s1.ScheduleAfter(1, reschedule)
	}
	s1.ScheduleAt(1, reschedule)

	if err := s1.RunUntilQuiet(); !errors.Is(err, sim.ErrBudgetExhausted) {
		t.Fatalf("MaxSteps RunUntilQuiet err = %v, want ErrBudgetExhausted", err)
	}
	if s1.Steps() != 5 || !errors.Is(s1.Err(), sim.ErrBudgetExhausted) {
		t.Fatalf("Steps=%d Err=%v, want Steps=5 Err=ErrBudgetExhausted", s1.Steps(), s1.Err())
	}

	// MaxSameInstant -> ErrTimeFrozen.
	s2 := sim.New(sim.Config{Seed: 1, MaxSameInstant: 6})
	err := s2.AddNode("n1", []env.NodeID{"n1"}, func(e env.Env) env.Process {
		var spin func()
		spin = func() {
			e.Dispatch.Enqueue(spin)
		}
		return &hookProcess{
			env: e,
			onInit: func(e env.Env) {
				e.Dispatch.Enqueue(spin)
			},
		}
	})
	if err != nil {
		t.Fatalf("AddNode error: %v", err)
	}
	if err := s2.RunUntilQuiet(); !errors.Is(err, sim.ErrTimeFrozen) {
		t.Fatalf("MaxSameInstant RunUntilQuiet err = %v, want ErrTimeFrozen", err)
	}
	if !errors.Is(s2.Err(), sim.ErrTimeFrozen) {
		t.Fatalf("s2.Err() = %v, want ErrTimeFrozen", s2.Err())
	}
}

func TestSwappableFIFODeterminism(t *testing.T) {
	runScenario := func(newFIFO func() env.FIFO[func()]) (string, int64, []sim.TraceEntry, int64) {
		s := sim.New(sim.Config{
			Seed:     2026,
			TraceCap: 16,
			Net: sim.NetConfig{
				MinLatency:    2 * time.Millisecond,
				MaxLatency:    8 * time.Millisecond,
				DropPPM:       100_000,
				DupPPM:        100_000,
				MaxDuplicates: 2,
			},
			NewFIFO: newFIFO,
		})

		peers := []env.NodeID{"n1", "n2", "n3"}
		factory := func(e env.Env) env.Process {
			rounds := 0
			return &hookProcess{
				env: e,
				onInit: func(e env.Env) {
					e.Dispatch.Enqueue(func() {
						for _, p := range e.Cluster.Peers {
							if p != e.Self {
								_ = e.Net.Send(p, []byte("hello"))
							}
						}
					})
					e.Clock.AfterFunc(10*time.Millisecond, func() {
						s.Trace("tick", string(e.Self))
					})
				},
				onRecover: func(e env.Env) {
					s.Trace("rec", string(e.Self))
					_ = e.Net.Send("n2", []byte("back"))
				},
				onMsg: func(e env.Env, from env.NodeID, data []byte) {
					rounds++
					if rounds <= 3 {
						r := e.Rand.IntN(10)
						e.Dispatch.Enqueue(func() {
							s.Trace("msg", fmt.Sprintf("%s<-%s:%d", e.Self, from, r))
							_ = e.Net.Send(from, []byte("ack"))
						})
					}
				},
			}
		}

		if err := s.AddCluster(peers, factory); err != nil {
			t.Fatalf("AddCluster: %v", err)
		}
		s.ScheduleAt(env.Time(6*time.Millisecond), func() {
			if err := s.Crash("n1"); err != nil {
				t.Fatalf("Crash: %v", err)
			}
		})
		s.ScheduleAt(env.Time(12*time.Millisecond), func() {
			if err := s.Recover("n1"); err != nil {
				t.Fatalf("Recover: %v", err)
			}
		})

		if err := s.RunUntilQuiet(); err != nil {
			t.Fatalf("RunUntilQuiet: %v", err)
		}
		return s.Digest(), s.Steps(), s.TraceTail(5), s.TraceLen()
	}

	d1, steps1, tail1, len1 := runScenario(func() env.FIFO[func()] { return env.NewSliceFIFO[func()]() })
	d2, steps2, tail2, len2 := runScenario(func() env.FIFO[func()] { return env.NewChanFIFO[func()](64) })

	if d1 == "" || steps1 == 0 || len1 == 0 || len(tail1) == 0 {
		t.Fatalf("scenario produced trivial run: digest=%q steps=%d traceLen=%d", d1, steps1, len1)
	}
	if d1 != d2 {
		t.Fatalf("Digest mismatch between SliceFIFO (%s) and ChanFIFO (%s)", d1, d2)
	}
	if steps1 != steps2 {
		t.Fatalf("Steps mismatch between SliceFIFO (%d) and ChanFIFO (%d)", steps1, steps2)
	}
	if len1 != len2 || len(tail1) != len(tail2) {
		t.Fatalf("TraceLen/Tail mismatch: (%d,%d) vs (%d,%d)", len1, len(tail1), len2, len(tail2))
	}
	for i := range tail1 {
		if tail1[i].String() != tail2[i].String() {
			t.Fatalf("TraceTail[%d] mismatch: %q vs %q", i, tail1[i].String(), tail2[i].String())
		}
	}
}

func TestEdgeHelpers(t *testing.T) {
	// Verify MinLatency<0, MaxLatency<MinLatency, out-of-range PPM normalization, TraceTail bounds.
	s := sim.New(sim.Config{
		Seed: 3,
		Net: sim.NetConfig{
			MinLatency:    -10,
			MaxLatency:    -20,
			DropPPM:       -5,
			DupPPM:        sim.PPM + 50,
			MaxDuplicates: 0,
		},
	})
	if s.Seed() != 3 {
		t.Fatalf("Seed() = %d, want 3", s.Seed())
	}
	if s.TraceTail(5) != nil || s.TraceTail(0) != nil {
		t.Fatal("expected nil TraceTail on empty sim")
	}

	var envs = make(map[env.NodeID]env.Env)
	var hits int
	for _, id := range []env.NodeID{"a", "b"} {
		if err := s.AddNode(id, []env.NodeID{"a", "b"}, func(e env.Env) env.Process {
			envs[e.Self] = e
			return &hookProcess{
				env: e,
				onMsg: func(e env.Env, from env.NodeID, data []byte) {
					hits++
				},
			}
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.RunUntilQuiet(); err != nil {
		t.Fatal(err)
	}
	if err := envs["a"].Net.Send("b", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := s.RunUntilQuiet(); err != nil {
		t.Fatal(err)
	}
	// DropPPM clamped to 0, DupPPM clamped to PPM, MaxDuplicates clamped to 1 => 2 deliveries at 0ns.
	if hits != 2 || s.Now() != 0 {
		t.Fatalf("hits=%d now=%s, want hits=2 now=0ns", hits, s.Now())
	}
	// TraceTail(n > traceCount) returns all traces without panicking.
	if tail := s.TraceTail(1000); int64(len(tail)) != s.TraceLen() {
		t.Fatalf("len(TraceTail(1000)) = %d, want %d", len(tail), s.TraceLen())
	}
	s.Trace("empty.detail", "")
	last := s.TraceTail(1)[0]
	if strings.HasSuffix(last.String(), " ") {
		t.Fatalf("TraceEntry.String() with empty detail has trailing space: %q", last.String())
	}
}
