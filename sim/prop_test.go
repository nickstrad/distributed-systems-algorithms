// Story: Simulator determinism, post-GST delivery, durable recovery, and observability hold under randomized schedules.
//
// What these tests prove: PRNG stream isolation, monotonic virtual time across timer/network schedules, guaranteed delivery within [MinLatency, MaxLatency] after Stabilize, crash-durable Store semantics, and accurate sim metrics/logs.
// How they probe: rapid property generators over seeds, fault sequences, and crash-recover cycles alongside exact obs.Collector snapshot and log-level filter assertions.
// What they do not prove: bounded-queue backpressure or application job pipelines (job/), nor external file/HTTP exporters (obs/).
package sim_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/nickstrad/distributed-systems-algorithms/env"
	"github.com/nickstrad/distributed-systems-algorithms/obs"
	"github.com/nickstrad/distributed-systems-algorithms/sim"
)

func TestPropStreamsIndependent(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		seed := rapid.Int64().Draw(t, "seed")
		labelA := rapid.String().Draw(t, "labelA")
		labelB := rapid.String().Filter(func(s string) bool { return s != labelA }).Draw(t, "labelB")
		extraA := rapid.IntRange(0, 256).Draw(t, "extraA")
		checkCount := rapid.IntRange(1, 32).Draw(t, "checkCount")

		s1 := sim.New(sim.Config{Seed: seed})
		s2 := sim.New(sim.Config{Seed: seed})

		a1 := s1.Stream(labelA)
		bBefore := s1.Stream(labelB)
		for range extraA {
			_ = a1.Uint64()
		}
		bAfter := s1.Stream(labelB)
		bRef := s2.Stream(labelB)
		bDirect := sim.NewStream(seed, labelB)

		for i := range checkCount {
			want := bRef.Uint64()
			if got := bDirect.Uint64(); got != want {
				t.Fatalf("NewStream mismatch at draw %d: got %d, want %d", i, got, want)
			}
			if got := bBefore.Uint64(); got != want {
				t.Fatalf("pre-created stream B perturbed at draw %d: got %d, want %d", i, got, want)
			}
			if got := bAfter.Uint64(); got != want {
				t.Fatalf("post-created stream B perturbed at draw %d: got %d, want %d", i, got, want)
			}
		}
	})
}

func TestPropTimeMonotonic(t *testing.T) {
	type schedOp struct {
		atMs    int
		srcIdx  int
		dstIdx  int
		isTimer bool
		delayMs int
		ttl     int
	}

	rapid.Check(t, func(t *rapid.T) {
		seed := rapid.Int64().Draw(t, "seed")
		numNodes := rapid.IntRange(2, 5).Draw(t, "numNodes")
		minLatMs := rapid.IntRange(0, 40).Draw(t, "minLatMs")
		spanLatMs := rapid.IntRange(0, 60).Draw(t, "spanLatMs")
		dropPPM := rapid.IntRange(0, sim.PPM).Draw(t, "dropPPM")
		dupPPM := rapid.IntRange(0, sim.PPM).Draw(t, "dupPPM")
		maxDup := rapid.IntRange(1, 4).Draw(t, "maxDup")
		numOps := rapid.IntRange(1, 30).Draw(t, "numOps")

		ops := make([]schedOp, numOps)
		for i := range ops {
			ops[i] = schedOp{
				atMs:    rapid.IntRange(0, 300).Draw(t, fmt.Sprintf("op%d.atMs", i)),
				srcIdx:  rapid.IntRange(0, numNodes-1).Draw(t, fmt.Sprintf("op%d.src", i)),
				dstIdx:  rapid.IntRange(0, numNodes-1).Draw(t, fmt.Sprintf("op%d.dst", i)),
				isTimer: rapid.Bool().Draw(t, fmt.Sprintf("op%d.isTimer", i)),
				delayMs: rapid.IntRange(0, 50).Draw(t, fmt.Sprintf("op%d.delayMs", i)),
				ttl:     rapid.IntRange(0, 2).Draw(t, fmt.Sprintf("op%d.ttl", i)),
			}
		}

		netCfg := sim.NetConfig{
			MinLatency:    time.Duration(minLatMs) * time.Millisecond,
			MaxLatency:    time.Duration(minLatMs+spanLatMs) * time.Millisecond,
			DropPPM:       dropPPM,
			DupPPM:        dupPPM,
			MaxDuplicates: maxDup,
		}

		runOnce := func() (digest string, steps int64, finalNow env.Time) {
			s := sim.New(sim.Config{
				Seed: seed,
				Net:  netCfg,
			})
			peers := make([]env.NodeID, numNodes)
			for i := range numNodes {
				peers[i] = env.NodeID(fmt.Sprintf("n%d", i))
			}
			envs := make([]env.Env, numNodes)
			prevNow := s.Now()

			s.AddInvariant("time_monotonic", func() error {
				cur := s.Now()
				if cur < prevNow {
					return fmt.Errorf("virtual time moved backwards: %s < %s", cur, prevNow)
				}
				prevNow = cur
				return nil
			})

			for i, id := range peers {
				idx := i
				if err := s.AddNode(id, peers, func(e env.Env) env.Process {
					envs[idx] = e
					return &hookProcess{
						env: e,
						onInit: func(e env.Env) {
							if e.Clock.Now() != s.Now() {
								t.Fatalf("clock mismatch in Init: %s != %s", e.Clock.Now(), s.Now())
							}
							d := time.Duration(e.Rand.IntN(25)) * time.Millisecond
							dst := peers[e.Rand.IntN(len(peers))]
							e.Clock.AfterFunc(d, func() {
								_ = e.Net.Send(dst, []byte{1, byte(idx)})
							})
						},
						onMsg: func(e env.Env, from env.NodeID, data []byte) {
							if e.Clock.Now() != s.Now() {
								t.Fatalf("clock mismatch in OnMessage: %s != %s", e.Clock.Now(), s.Now())
							}
							if len(data) >= 2 && data[0] > 0 {
								nextTTL := data[0] - 1
								d := time.Duration(e.Rand.IntN(15)) * time.Millisecond
								e.Clock.AfterFunc(d, func() {
									_ = e.Net.Send(from, []byte{nextTTL, byte(idx)})
								})
							}
						},
					}
				}); err != nil {
					t.Fatalf("AddNode(%s): %v", id, err)
				}
			}

			for _, op := range ops {
				op := op
				at := env.Time(0).Add(time.Duration(op.atMs) * time.Millisecond)
				s.ScheduleAt(at, func() {
					srcEnv := envs[op.srcIdx]
					dst := peers[op.dstIdx]
					payload := []byte{byte(op.ttl), byte(op.srcIdx)}
					if op.isTimer {
						srcEnv.Clock.AfterFunc(time.Duration(op.delayMs)*time.Millisecond, func() {
							_ = srcEnv.Net.Send(dst, payload)
						})
					} else {
						_ = srcEnv.Net.Send(dst, payload)
					}
				})
			}

			for {
				ok, err := s.Step()
				if err != nil {
					t.Fatalf("Step failed: %v", err)
				}
				if s.Now() < prevNow {
					t.Fatalf("s.Now() moved backwards: %s < %s", s.Now(), prevNow)
				}
				prevNow = s.Now()
				if !ok {
					break
				}
			}
			return s.Digest(), s.Steps(), s.Now()
		}

		d1, st1, now1 := runOnce()
		d2, st2, now2 := runOnce()
		if d1 != d2 || st1 != st2 || now1 != now2 {
			t.Fatalf("nondeterministic replay: (%s, %d, %s) != (%s, %d, %s)",
				d1, st1, now1, d2, st2, now2)
		}
	})
}

func TestPropHealedNetworkDelivers(t *testing.T) {
	type faultOp struct {
		kind   int // 0=partition, 1=partitionBi, 2=isolate, 3=heal, 4=send, 5=crash, 6=recover
		srcIdx int
		dstIdx int
	}

	rapid.Check(t, func(t *rapid.T) {
		seed := rapid.Int64().Draw(t, "seed")
		numNodes := rapid.IntRange(2, 5).Draw(t, "numNodes")
		preDrop := rapid.IntRange(0, sim.PPM).Draw(t, "preDropPPM")
		preDup := rapid.IntRange(0, sim.PPM).Draw(t, "preDupPPM")
		minLatMs := rapid.IntRange(0, 20).Draw(t, "minLatMs")
		spanLatMs := rapid.IntRange(0, 40).Draw(t, "spanLatMs")
		numPreOps := rapid.IntRange(1, 25).Draw(t, "numPreOps")
		numPostSends := rapid.IntRange(1, 16).Draw(t, "numPostSends")

		preOps := make([]faultOp, numPreOps)
		for i := range preOps {
			preOps[i] = faultOp{
				kind:   rapid.IntRange(0, 6).Draw(t, fmt.Sprintf("pre%d.kind", i)),
				srcIdx: rapid.IntRange(0, numNodes-1).Draw(t, fmt.Sprintf("pre%d.src", i)),
				dstIdx: rapid.IntRange(0, numNodes-1).Draw(t, fmt.Sprintf("pre%d.dst", i)),
			}
		}
		type postSend struct {
			srcIdx int
			dstIdx int
		}
		postSends := make([]postSend, numPostSends)
		for i := range postSends {
			postSends[i] = postSend{
				srcIdx: rapid.IntRange(0, numNodes-1).Draw(t, fmt.Sprintf("post%d.src", i)),
				dstIdx: rapid.IntRange(0, numNodes-1).Draw(t, fmt.Sprintf("post%d.dst", i)),
			}
		}

		s := sim.New(sim.Config{
			Seed: seed,
			Net: sim.NetConfig{
				MinLatency:    time.Millisecond,
				MaxLatency:    10 * time.Millisecond,
				DropPPM:       preDrop,
				DupPPM:        preDup,
				MaxDuplicates: 2,
			},
		})

		peers := make([]env.NodeID, numNodes)
		for i := range numNodes {
			peers[i] = env.NodeID(fmt.Sprintf("n%d", i))
		}
		envs := make([]env.Env, numNodes)

		type deliveryRecord struct {
			at   env.Time
			from env.NodeID
			to   env.NodeID
		}
		delivered := make(map[string]deliveryRecord)

		for i, id := range peers {
			idx := i
			nodeID := id
			if err := s.AddNode(nodeID, peers, func(e env.Env) env.Process {
				envs[idx] = e
				return &hookProcess{
					env: e,
					onRecover: func(e env.Env) {
						envs[idx] = e
					},
					onMsg: func(e env.Env, from env.NodeID, data []byte) {
						msg := string(data)
						if strings.HasPrefix(msg, "gst:") {
							delivered[msg] = deliveryRecord{
								at:   e.Clock.Now(),
								from: from,
								to:   nodeID,
							}
						}
					},
				}
			}); err != nil {
				t.Fatalf("AddNode(%s): %v", nodeID, err)
			}
		}

		if err := s.RunUntilQuiet(); err != nil {
			t.Fatalf("initial RunUntilQuiet: %v", err)
		}

		for i, op := range preOps {
			src := peers[op.srcIdx]
			dst := peers[op.dstIdx]
			switch op.kind {
			case 0:
				s.Partition(src, dst)
			case 1:
				s.PartitionBi(src, dst)
			case 2:
				s.Isolate(src)
			case 3:
				s.Heal(src, dst)
			case 4:
				_ = envs[op.srcIdx].Net.Send(dst, fmt.Appendf(nil, "pre:%d", i))
			case 5:
				if err := s.Crash(src); err != nil {
					t.Fatalf("Crash(%s): %v", src, err)
				}
			case 6:
				if err := s.Recover(src); err != nil {
					t.Fatalf("Recover(%s): %v", src, err)
				}
			}
			if err := s.RunFor(2 * time.Millisecond); err != nil {
				t.Fatalf("RunFor during preOps: %v", err)
			}
		}

		if err := s.RunUntilQuiet(); err != nil {
			t.Fatalf("pre-GST RunUntilQuiet: %v", err)
		}

		minLat := time.Duration(minLatMs) * time.Millisecond
		maxLat := time.Duration(minLatMs+spanLatMs) * time.Millisecond
		s.Stabilize(sim.NetConfig{
			MinLatency: minLat,
			MaxLatency: maxLat,
		})

		// Collect currently live nodes in deterministic sorted order.
		var liveIdxs []int
		for i, id := range peers {
			if s.IsAlive(id) {
				liveIdxs = append(liveIdxs, i)
			}
		}
		if len(liveIdxs) < 2 {
			// Ensure at least two live nodes exist so post-GST traffic can be checked.
			for i, id := range peers {
				if !s.IsAlive(id) {
					if err := s.Recover(id); err != nil {
						t.Fatalf("Recover(%s): %v", id, err)
					}
					liveIdxs = append(liveIdxs, i)
					if len(liveIdxs) >= 2 {
						break
					}
				}
			}
			slices.Sort(liveIdxs)
			if err := s.RunUntilQuiet(); err != nil {
				t.Fatalf("post-recover RunUntilQuiet: %v", err)
			}
		}

		type sentMsg struct {
			id     string
			sentAt env.Time
			from   env.NodeID
			to     env.NodeID
		}
		var sent []sentMsg
		sendAt := s.Now()
		for i, ps := range postSends {
			srcIdx := liveIdxs[ps.srcIdx%len(liveIdxs)]
			dstIdx := liveIdxs[ps.dstIdx%len(liveIdxs)]
			msgID := fmt.Sprintf("gst:%d", i)
			if err := envs[srcIdx].Net.Send(peers[dstIdx], []byte(msgID)); err != nil {
				t.Fatalf("post-GST Send(%s->%s): %v", peers[srcIdx], peers[dstIdx], err)
			}
			sent = append(sent, sentMsg{
				id:     msgID,
				sentAt: sendAt,
				from:   peers[srcIdx],
				to:     peers[dstIdx],
			})
		}

		if err := s.RunUntilQuiet(); err != nil {
			t.Fatalf("post-GST RunUntilQuiet: %v", err)
		}

		for _, m := range sent {
			rec, ok := delivered[m.id]
			if !ok {
				t.Fatalf("post-GST message %s (%s->%s) was not delivered", m.id, m.from, m.to)
			}
			if rec.from != m.from || rec.to != m.to {
				t.Fatalf("message %s delivered with wrong endpoints: got %s->%s, want %s->%s",
					m.id, rec.from, rec.to, m.from, m.to)
			}
			latency := rec.at.Sub(m.sentAt)
			if latency < minLat || latency > maxLat {
				t.Fatalf("message %s latency %s outside [%s, %s]", m.id, latency, minLat, maxLat)
			}
		}
	})
}

func TestPropStoreSurvivesRecovery(t *testing.T) {
	keyUniverse := []string{"", "a", "ab", "abc", "b", "k1", "k2", "z"}
	prefixes := []string{"", "a", "ab", "k", "z", "missing"}

	rapid.Check(t, func(t *rapid.T) {
		seed := rapid.Int64().Draw(t, "seed")
		cycles := rapid.IntRange(1, 4).Draw(t, "cycles")

		s := sim.New(sim.Config{Seed: seed})
		var curEnv env.Env
		recoveries := 0
		if err := s.AddNode("n1", []env.NodeID{"n1"}, func(e env.Env) env.Process {
			curEnv = e
			return &hookProcess{
				env: e,
				onRecover: func(e env.Env) {
					curEnv = e
					recoveries++
				},
			}
		}); err != nil {
			t.Fatalf("AddNode: %v", err)
		}
		if err := s.RunUntilQuiet(); err != nil {
			t.Fatalf("initial RunUntilQuiet: %v", err)
		}

		ref := make(map[string][]byte)
		for c := range cycles {
			nOps := rapid.IntRange(1, 20).Draw(t, fmt.Sprintf("cycle%d.nOps", c))
			for i := range nOps {
				isDelete := rapid.Bool().Draw(t, fmt.Sprintf("c%d.op%d.del", c, i))
				key := rapid.SampledFrom(keyUniverse).Draw(t, fmt.Sprintf("c%d.op%d.key", c, i))
				if isDelete {
					if err := curEnv.Store.Delete(key); err != nil {
						t.Fatalf("Delete(%q): %v", key, err)
					}
					delete(ref, key)
				} else {
					val := rapid.SliceOfN(rapid.Byte(), 0, 16).Draw(t, fmt.Sprintf("c%d.op%d.val", c, i))
					stored := slices.Clone(val)
					if err := curEnv.Store.Store(key, stored); err != nil {
						t.Fatalf("Store(%q): %v", key, err)
					}
					// Mutate caller's slice to verify memStore defensively clones writes.
					if len(stored) > 0 {
						stored[0] ^= 0xff
					}
					ref[key] = slices.Clone(val)
				}
			}

			if err := s.Crash("n1"); err != nil {
				t.Fatalf("Crash: %v", err)
			}
			if s.IsAlive("n1") {
				t.Fatal("expected n1 to be crashed")
			}
			if err := s.Recover("n1"); err != nil {
				t.Fatalf("Recover: %v", err)
			}
			if err := s.RunUntilQuiet(); err != nil {
				t.Fatalf("RunUntilQuiet after recover: %v", err)
			}
			if !s.IsAlive("n1") {
				t.Fatal("expected n1 to be alive after Recover")
			}
			if recoveries != c+1 {
				t.Fatalf("recoveries = %d, want %d", recoveries, c+1)
			}

			for _, k := range keyUniverse {
				got, ok, err := curEnv.Store.Retrieve(k)
				if err != nil {
					t.Fatalf("Retrieve(%q): %v", k, err)
				}
				want, wantOK := ref[k]
				if ok != wantOK {
					t.Fatalf("Retrieve(%q) ok = %v, want %v", k, ok, wantOK)
				}
				if ok && !bytes.Equal(got, want) {
					t.Fatalf("Retrieve(%q) = %x, want %x", k, got, want)
				}
				// Mutate returned slice to verify Retrieve defensively clones reads.
				if len(got) > 0 {
					got[0] ^= 0xff
					got2, ok2, err2 := curEnv.Store.Retrieve(k)
					if err2 != nil || !ok2 || !bytes.Equal(got2, want) {
						t.Fatalf("mutating Retrieve(%q) result mutated store", k)
					}
				}
			}

			for _, prefix := range prefixes {
				gotKeys, err := curEnv.Store.Keys(prefix)
				if err != nil {
					t.Fatalf("Keys(%q): %v", prefix, err)
				}
				var wantKeys []string
				for k := range ref {
					if strings.HasPrefix(k, prefix) {
						wantKeys = append(wantKeys, k)
					}
				}
				slices.Sort(wantKeys)
				if !slices.Equal(gotKeys, wantKeys) {
					t.Fatalf("Keys(%q) = %v, want %v", prefix, gotKeys, wantKeys)
				}
			}
		}
	})
}

func TestSimMetricsSource(t *testing.T) {
	const fixedLatency = 5 * time.Millisecond

	s := sim.New(sim.Config{
		Seed: 7,
		Net: sim.NetConfig{
			MinLatency: fixedLatency,
			MaxLatency: fixedLatency,
		},
	})

	envs := make(map[env.NodeID]env.Env)
	addNode := func(id env.NodeID) {
		t.Helper()
		if err := s.AddNode(id, []env.NodeID{"n1", "n2", "n3"}, func(e env.Env) env.Process {
			envs[id] = e
			return &hookProcess{
				env: e,
				onRecover: func(e env.Env) {
					envs[id] = e
				},
			}
		}); err != nil {
			t.Fatalf("AddNode(%s): %v", id, err)
		}
	}

	addNode("n1")
	addNode("n2")
	// 2 Init local drains
	if err := s.RunUntilQuiet(); err != nil {
		t.Fatalf("RunUntilQuiet init: %v", err)
	}

	// 1) Normal delivered message (sent=1, delivered=1, latency count=1)
	if err := envs["n1"].Net.Send("n2", []byte("ok")); err != nil {
		t.Fatalf("Send 1: %v", err)
	}
	if err := s.RunUntilQuiet(); err != nil {
		t.Fatalf("RunUntilQuiet 1: %v", err)
	}

	// 2) Partition drop (sent=2, dropped=1)
	s.Partition("n1", "n2")
	if err := envs["n1"].Net.Send("n2", []byte("partitioned")); err != nil {
		t.Fatalf("Send 2: %v", err)
	}
	s.Heal("n1", "n2")

	// 3) Probabilistic drop with DropPPM=PPM (sent=3, dropped=2)
	s.SetNetConfig(sim.NetConfig{
		MinLatency: fixedLatency,
		MaxLatency: fixedLatency,
		DropPPM:    sim.PPM,
	})
	if err := envs["n1"].Net.Send("n2", []byte("loss")); err != nil {
		t.Fatalf("Send 3: %v", err)
	}

	// 4) Guaranteed duplication with DupPPM=PPM, MaxDuplicates=1 (sent=4, duplicated=1, delivered+=2 -> 3)
	s.SetNetConfig(sim.NetConfig{
		MinLatency:    fixedLatency,
		MaxLatency:    fixedLatency,
		DupPPM:        sim.PPM,
		MaxDuplicates: 1,
	})
	if err := envs["n1"].Net.Send("n2", []byte("dup")); err != nil {
		t.Fatalf("Send 4: %v", err)
	}
	if err := s.RunUntilQuiet(); err != nil {
		t.Fatalf("RunUntilQuiet 2: %v", err)
	}

	// 5) Crash n2, add n3, then recover n2 (crashes=1, recoveries=1, live=3, peak=3)
	if err := s.Crash("n2"); err != nil {
		t.Fatalf("Crash(n2): %v", err)
	}
	addNode("n3")
	if err := s.Recover("n2"); err != nil {
		t.Fatalf("Recover(n2): %v", err)
	}
	if err := s.RunUntilQuiet(); err != nil {
		t.Fatalf("RunUntilQuiet 3: %v", err)
	}

	// Total steps:
	// - 2 Init local drains
	// - 1 net.deliver ("ok")
	// - 2 net.deliver ("dup" x2)
	// - 1 Init local drain (n3)
	// - 1 Recover local drain (n2)
	// Total = 7 steps.
	const wantSteps int64 = 7
	if s.Steps() != wantSteps {
		t.Fatalf("s.Steps() = %d, want %d", s.Steps(), wantSteps)
	}

	col := obs.NewCollector("sim-test")
	col.Register(s.MetricsSource())
	snap := col.Collect(int64(s.Now()))

	wantPoints := map[string]struct {
		kind  obs.Kind
		unit  string
		value int64
	}{
		"steps":               {kind: obs.KindCounter, unit: "count", value: wantSteps},
		"messages_sent":       {kind: obs.KindCounter, unit: "count", value: 4},
		"messages_dropped":    {kind: obs.KindCounter, unit: "count", value: 2},
		"messages_duplicated": {kind: obs.KindCounter, unit: "count", value: 1},
		"messages_delivered":  {kind: obs.KindCounter, unit: "count", value: 3},
		"node_crashes":        {kind: obs.KindCounter, unit: "count", value: 1},
		"node_recoveries":     {kind: obs.KindCounter, unit: "count", value: 1},
		"nodes_live":          {kind: obs.KindGauge, unit: "count", value: 3},
		"nodes_live_peak":     {kind: obs.KindGauge, unit: "count", value: 3},
	}

	if len(snap.Points) != len(wantPoints) {
		t.Fatalf("len(snap.Points) = %d, want %d: %+v", len(snap.Points), len(wantPoints), snap.Points)
	}
	seen := make(map[string]bool)
	for _, p := range snap.Points {
		if p.Component != "sim" {
			t.Fatalf("point %s component = %q, want %q", p.Metric, p.Component, "sim")
		}
		want, ok := wantPoints[p.Metric]
		if !ok {
			t.Fatalf("unexpected metric %q", p.Metric)
		}
		seen[p.Metric] = true
		if p.Kind != want.kind || p.Unit != want.unit || p.Value != want.value {
			t.Fatalf("metric %s = (kind=%v unit=%q val=%d), want (kind=%v unit=%q val=%d)",
				p.Metric, p.Kind, p.Unit, p.Value, want.kind, want.unit, want.value)
		}
	}
	for k := range wantPoints {
		if !seen[k] {
			t.Fatalf("missing metric %q in snapshot", k)
		}
	}

	if len(snap.Hists) != 1 {
		t.Fatalf("len(snap.Hists) = %d, want 1", len(snap.Hists))
	}
	h := snap.Hists[0]
	if h.Component != "sim" || h.Metric != "net_delivery_latency" || h.Unit != "ns" {
		t.Fatalf("unexpected hist metadata: %+v", h)
	}
	if h.Count != 3 || h.ClampedLow != 0 || h.ClampedHigh != 0 {
		t.Fatalf("hist counts = (count=%d low=%d high=%d), want (3, 0, 0)",
			h.Count, h.ClampedLow, h.ClampedHigh)
	}
	if len(h.Buckets) != 1 {
		t.Fatalf("expected 1 non-empty latency bucket, got %+v", h.Buckets)
	}
	b := h.Buckets[0]
	wantNs := int64(fixedLatency)
	if b.Count != 3 || b.Lo > wantNs || b.Hi < wantNs {
		t.Fatalf("bucket = %+v, want Count=3 covering %d ns", b, wantNs)
	}
}

func TestSimLogger(t *testing.T) {
	t.Run("disabled_uses_nop_logger", func(t *testing.T) {
		s := sim.New(sim.Config{
			Seed:       11,
			LogEnabled: false,
		})
		var gotLog env.Logger
		if err := s.AddNode("n1", []env.NodeID{"n1"}, func(e env.Env) env.Process {
			gotLog = e.Log
			return &hookProcess{
				env: e,
				onInit: func(e env.Env) {
					e.Log.Debug("dbg")
					e.Log.Info("inf")
					e.Log.Warn("wrn")
					e.Log.Error("err")
				},
			}
		}); err != nil {
			t.Fatalf("AddNode: %v", err)
		}
		if _, ok := gotLog.(env.NopLogger); !ok {
			t.Fatalf("expected env.NopLogger when LogEnabled=false, got %T", gotLog)
		}
		if err := s.RunUntilQuiet(); err != nil {
			t.Fatalf("RunUntilQuiet: %v", err)
		}
		for _, entry := range s.TraceTail(int(s.TraceLen())) {
			if strings.HasPrefix(entry.Kind, "log.") {
				t.Fatalf("unexpected log trace entry when LogEnabled=false: %s", entry)
			}
		}
	})

	t.Run("warn_level_filters_and_is_deterministic", func(t *testing.T) {
		run := func() *sim.Sim {
			s := sim.New(sim.Config{
				Seed:       123,
				LogEnabled: true,
				LogLevel:   env.LevelWarn,
			})
			if err := s.AddNode("alpha", []env.NodeID{"alpha"}, func(e env.Env) env.Process {
				return &hookProcess{
					env: e,
					onInit: func(e env.Env) {
						e.Log.Debug("ignored debug", env.Int("step", 1))
						e.Log.Info("ignored info", env.Int("step", 2))
						e.Log.Log(env.LevelInfo, "ignored log info")
						e.Log.Warn("low disk", env.Int("pct", 92))
						e.Log.Error("write failed", env.Err(errors.New("eio")))
						e.Log.Log(env.LevelWarn, "explicit warn", env.Str("k", "v"))
					},
				}
			}); err != nil {
				t.Fatalf("AddNode: %v", err)
			}
			if err := s.RunUntilQuiet(); err != nil {
				t.Fatalf("RunUntilQuiet: %v", err)
			}
			return s
		}

		s1 := run()
		s2 := run()
		if s1.Digest() != s2.Digest() {
			t.Fatalf("nondeterministic digest with logging enabled: %s != %s", s1.Digest(), s2.Digest())
		}

		t1 := s1.TraceTail(int(s1.TraceLen()))
		t2 := s2.TraceTail(int(s2.TraceLen()))
		if !slices.Equal(t1, t2) {
			t.Fatalf("trace mismatch across runs: %v != %v", t1, t2)
		}

		var logEntries []sim.TraceEntry
		for _, entry := range t1 {
			if entry.Kind == "log.debug" || entry.Kind == "log.info" {
				t.Fatalf("unexpected filtered log entry in trace: %s", entry)
			}
			if strings.HasPrefix(entry.Kind, "log.") {
				logEntries = append(logEntries, entry)
			}
		}

		wantEntries := []struct {
			kind   string
			detail string
		}{
			{kind: "log.warn", detail: `msg="low disk" node=alpha pct=92`},
			{kind: "log.error", detail: `msg="write failed" node=alpha err=eio`},
			{kind: "log.warn", detail: `msg="explicit warn" node=alpha k=v`},
		}
		if len(logEntries) != len(wantEntries) {
			t.Fatalf("got %d log entries %v, want %d", len(logEntries), logEntries, len(wantEntries))
		}
		for i, want := range wantEntries {
			got := logEntries[i]
			if got.Kind != want.kind || got.Detail != want.detail {
				t.Fatalf("log entry %d = (%q, %q), want (%q, %q)",
					i, got.Kind, got.Detail, want.kind, want.detail)
			}
		}
	})
}

func FuzzSimSchedule(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{1, 0, 0, 0, 0, 0, 0, 0, 1, 5, 10, 10, 0, 0, 1, 3, 1, 1, 2, 4, 2, 0, 1, 2, 4, 1, 0, 0, 5, 1, 0, 0, 3, 0, 1, 0})
	f.Add([]byte{42, 1, 2, 3, 4, 5, 6, 7, 2, 20, 50, 80, 0, 0, 2, 1, 1, 2, 3, 5, 4, 2, 0, 1, 5, 2, 0, 2, 6, 3, 1, 4})

	f.Fuzz(func(t *testing.T, data []byte) {
		var hdr [12]byte
		copy(hdr[:], data)

		seed := int64(binary.LittleEndian.Uint64(hdr[0:8]))
		minLat := time.Duration(hdr[8]%25) * time.Millisecond
		maxLat := minLat + time.Duration(hdr[9]%40)*time.Millisecond
		dropPPM := int(hdr[10]) * (sim.PPM / 255)
		dupPPM := int(hdr[11]) * (sim.PPM / 255)

		type fuzzOp struct {
			kind  uint8
			src   int
			dst   int
			param uint8
		}
		var ops []fuzzOp
		rest := data
		if len(rest) > 12 {
			rest = rest[12:]
		} else {
			rest = nil
		}
		const maxOps = 48
		for i := 0; i+3 < len(rest) && len(ops) < maxOps; i += 4 {
			ops = append(ops, fuzzOp{
				kind:  rest[i] % 7,
				src:   int(rest[i+1] % 4),
				dst:   int(rest[i+2] % 4),
				param: rest[i+3],
			})
		}

		peers := []env.NodeID{"n0", "n1", "n2", "n3"}
		netCfg := sim.NetConfig{
			MinLatency:    minLat,
			MaxLatency:    maxLat,
			DropPPM:       dropPPM,
			DupPPM:        dupPPM,
			MaxDuplicates: 2,
		}

		runSchedule := func() (*sim.Sim, error) {
			s := sim.New(sim.Config{
				Seed:       seed,
				Net:        netCfg,
				LogEnabled: hdr[8]&1 == 1,
				LogLevel:   env.LevelWarn,
			})
			var envs [4]env.Env
			for i, id := range peers {
				idx := i
				if err := s.AddNode(id, peers, func(e env.Env) env.Process {
					envs[idx] = e
					return &hookProcess{
						env: e,
						onRecover: func(e env.Env) {
							envs[idx] = e
						},
						onMsg: func(e env.Env, from env.NodeID, payload []byte) {
							e.Log.Warn("recv", env.Str("from", string(from)), env.Int("len", len(payload)))
						},
					}
				}); err != nil {
					return s, err
				}
			}

			cursor := env.Time(0)
			for i, op := range ops {
				op := op
				opIdx := i
				cursor = cursor.Add(time.Duration(op.param&0x0f) * time.Millisecond)
				s.ScheduleAt(cursor, func() {
					srcID := peers[op.src]
					dstID := peers[op.dst]
					switch op.kind {
					case 0: // node send
						_ = envs[op.src].Net.Send(dstID, []byte{byte(opIdx), op.param})
					case 1: // timer arm
						d := time.Duration(op.param&0x1f) * time.Millisecond
						e := envs[op.src]
						e.Clock.AfterFunc(d, func() {
							_ = e.Net.Send(dstID, []byte{byte(opIdx), 0xaa})
						})
					case 2: // partition
						s.Partition(srcID, dstID)
					case 3: // heal
						s.Heal(srcID, dstID)
					case 4: // crash
						_ = s.Crash(srcID)
					case 5: // recover
						_ = s.Recover(srcID)
					case 6: // stabilize
						s.Stabilize(sim.NetConfig{
							MinLatency: minLat,
							MaxLatency: maxLat,
						})
					}
				})
			}

			err := s.RunUntilQuiet()
			return s, err
		}

		s1, err1 := runSchedule()
		s2, err2 := runSchedule()
		if err1 != nil || err2 != nil {
			var pe *sim.PanicError
			if errors.As(err1, &pe) || errors.As(err2, &pe) {
				t.Fatalf("panic during fuzz schedule: err1=%v err2=%v", err1, err2)
			}
			t.Fatalf("unexpected simulation error: err1=%v err2=%v", err1, err2)
		}
		if s1.Digest() != s2.Digest() {
			t.Fatalf("digest mismatch for identical schedule: %s != %s", s1.Digest(), s2.Digest())
		}
		if s1.Steps() != s2.Steps() || s1.Now() != s2.Now() {
			t.Fatalf("state mismatch: (%d, %s) != (%d, %s)",
				s1.Steps(), s1.Now(), s2.Steps(), s2.Now())
		}
	})
}
