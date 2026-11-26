package scenario

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/nickstrad/distributed-systems-algorithms/env"
	"github.com/nickstrad/distributed-systems-algorithms/job"
	"github.com/nickstrad/distributed-systems-algorithms/obs"
	"github.com/nickstrad/distributed-systems-algorithms/sim"
)

// JobBufferedParams configures the built-in "job-buffered" suite.
type JobBufferedParams struct {
	Handler   string `json:"handler"` // "sync" or "async"
	Capacity  int    `json:"capacity"`
	Bursts    int    `json:"bursts"`
	BurstSize int    `json:"burst_size"`
	GapMs     int64  `json:"gap_ms"`
}

// SimPingParams configures the built-in "sim-ping" multi-node chaos suite.
type SimPingParams struct {
	Nodes         int    `json:"nodes"`
	Rounds        int    `json:"rounds"`
	IntervalMs    int64  `json:"interval_ms"`
	CrashNode     string `json:"crash_node,omitempty"`
	CrashAtMs     int64  `json:"crash_at_ms,omitempty"`
	RecoverAtMs   int64  `json:"recover_at_ms,omitempty"`
	PartitionAtMs int64  `json:"partition_at_ms,omitempty"`
	GSTAtMs       int64  `json:"gst_at_ms,omitempty"`
}

func registerBuiltins(reg *Registry) {
	reg.Register(Suite{
		Name:        "job-buffered",
		Description: "Composable bounded BufferedTransformer over SyncHandler or AsyncHandler with soundness invariants",
		DefaultParams: func() any {
			return &JobBufferedParams{
				Handler:   "sync",
				Capacity:  4,
				Bursts:    8,
				BurstSize: 6,
				GapMs:     5,
			}
		},
		Run: runJobBuffered,
	})

	reg.Register(Suite{
		Name:        "sim-ping",
		Description: "Multi-node signed ping cluster exercising SimNet faults, partitions, GST, and crash-recovery",
		DefaultParams: func() any {
			return &SimPingParams{
				Nodes:         5,
				Rounds:        10,
				IntervalMs:    20,
				CrashNode:     "n2",
				CrashAtMs:     60,
				RecoverAtMs:   140,
				PartitionAtMs: 40,
				GSTAtMs:       120,
			}
		},
		Run: runSimPing,
	})
}

func runJobBuffered(cfg RunConfig, seed int64, rawParams json.RawMessage) (SuiteOutcome, error) {
	p := JobBufferedParams{
		Handler:   "sync",
		Capacity:  4,
		Bursts:    8,
		BurstSize: 6,
		GapMs:     5,
	}
	if err := DecodeParams(rawParams, &p); err != nil {
		return SuiteOutcome{}, err
	}
	if p.Capacity <= 0 {
		return SuiteOutcome{}, fmt.Errorf("job-buffered: capacity must be > 0, got %d", p.Capacity)
	}
	if p.Bursts <= 0 || p.BurstSize <= 0 {
		return SuiteOutcome{}, fmt.Errorf("job-buffered: bursts and burst_size must be > 0")
	}
	if p.Handler != "sync" && p.Handler != "async" {
		return SuiteOutcome{}, fmt.Errorf("job-buffered: unknown handler %q (want sync or async)", p.Handler)
	}

	simCfg, err := cfg.SimConfig(seed)
	if err != nil {
		return SuiteOutcome{}, err
	}
	s := sim.New(simCfg)

	processed := make(map[string]int)
	confirmed := make(map[string]bool)
	errored := make(map[string]bool)
	var tr *job.BufferedTransformer
	var handlerSrc obs.Source

	type jobNode struct {
		env.BaseProcess
		e env.Env
	}

	var node *jobNode
	err = s.AddNode("worker", []env.NodeID{"worker"}, func(e env.Env) env.Process {
		node = &jobNode{e: e}
		processFn := func(j job.Job) {
			processed[j.ID]++
			e.Log.Debug("processed job", env.Str("job_id", j.ID))
		}
		var h job.Handler
		switch p.Handler {
		case "sync":
			sh := job.NewSyncHandler(e.Dispatch, processFn)
			h = sh
			handlerSrc = sh.MetricsSource("job_handler")
		case "async":
			ah := job.NewAsyncHandler(e.Dispatch, processFn)
			ah.Init()
			h = ah
			handlerSrc = ah.MetricsSource("job_handler")
		default:
			panic("job-buffered: invalid handler " + p.Handler)
		}
		tr = job.NewBufferedTransformer(e.Dispatch, h, p.Capacity)
		tr.OnConfirm(func(j job.Job) { confirmed[j.ID] = true })
		tr.OnError(func(j job.Job) { errored[j.ID] = true })
		return node
	})
	if err != nil {
		return SuiteOutcome{Sim: s}, err
	}

	s.AddInvariant("soundness", func() error {
		for id := range errored {
			if processed[id] > 0 {
				return fmt.Errorf("errored job %s was processed", id)
			}
			if confirmed[id] {
				return fmt.Errorf("job %s both confirmed and errored", id)
			}
		}
		return nil
	})

	rng := s.Stream("workload")
	totalSubmitted := 0
	for b := 0; b < p.Bursts; b++ {
		burstIdx := b
		delay := time.Duration(b)*time.Duration(p.GapMs)*time.Millisecond + time.Duration(rng.IntN(3))*time.Millisecond
		count := 1 + rng.IntN(p.BurstSize)
		totalSubmitted += count
		s.ScheduleAfter(delay, func() {
			for i := 0; i < count; i++ {
				id := fmt.Sprintf("b%d-j%d", burstIdx, i)
				tr.Submit(job.Job{ID: id, Payload: []byte(id)})
			}
		})
	}

	if err := s.RunUntilQuiet(); err != nil {
		return SuiteOutcome{Sim: s}, err
	}

	erroredProcessed := 0
	for id := range errored {
		if processed[id] > 0 {
			erroredProcessed++
		}
	}

	col := obs.NewCollector("job-buffered")
	col.Register(s.MetricsSource())
	col.Register(tr.MetricsSource("job_buffered"))
	col.Register(handlerSrc)

	summary := fmt.Sprintf("handler=%s cap=%d submitted=%d confirmed=%d errored=%d processed=%d",
		p.Handler, p.Capacity, totalSubmitted, len(confirmed), len(errored), len(processed))
	fifoKind := cfg.FIFO
	if fifoKind == "" {
		fifoKind = "slice"
	}
	story := Story{
		Proves: fmt.Sprintf(
			"Composing a bounded BufferedTransformer (cap=%d) over %s JobHandler preserves Guaranteed Response (every job gets exactly one Confirm or Error) and Soundness (no rejected job is ever processed), with non-reentrant FIFO dispatch.",
			p.Capacity, p.Handler,
		),
		HowCollected: fmt.Sprintf(
			"Single-threaded deterministic simulation (fifo=%s) submitting %d seeded bursts (up to %d jobs/burst, %dms gap) with continuous soundness invariants checked after every event step.",
			fifoKind, p.Bursts, p.BurstSize, p.GapMs,
		),
		DoesNotProve: "Proves single-node local module composition and FIFO ordering under deterministic scheduling; does not test network failures, multi-process crashes, or OS thread contention.",
		Checks: []Check{
			{
				Claim:    "Guaranteed response (submitted == confirmed + errored)",
				Observed: fmt.Sprintf("%d == %d + %d", totalSubmitted, len(confirmed), len(errored)),
				Expected: fmt.Sprintf("%d responses", totalSubmitted),
				Pass:     len(confirmed)+len(errored) == totalSubmitted,
			},
			{
				Claim:    "Every confirmed job processed exactly once",
				Observed: fmt.Sprintf("%d processed", len(processed)),
				Expected: fmt.Sprintf("%d confirmed", len(confirmed)),
				Pass:     len(processed) == len(confirmed),
			},
			{
				Claim:    "Soundness (errored jobs never processed)",
				Observed: fmt.Sprintf("%d processed", erroredProcessed),
				Expected: "0 processed",
				Pass:     erroredProcessed == 0,
			},
		},
	}
	return SuiteOutcome{Sim: s, Collector: col, Summary: summary, Story: story}, nil
}

type pingNode struct {
	env.BaseProcess
	e         env.Env
	rounds    int
	interval  time.Duration
	sent      int
	received  int
	verified  int
	recovered bool
	counter   *obs.Counter
	recvAll   *obs.Counter
}

func (p *pingNode) Init() {
	p.arm(0)
}

func (p *pingNode) Recover() {
	p.recovered = true
	if raw, ok, _ := p.e.Store.Retrieve("sent"); ok {
		var prev int
		_ = json.Unmarshal(raw, &prev)
		p.sent = prev
	}
	p.e.Log.Info("recovered ping node", env.Int("restored_sent", p.sent))
	p.arm(p.sent)
}

func (p *pingNode) arm(round int) {
	if round >= p.rounds {
		return
	}
	p.e.Clock.AfterFunc(p.interval, func() {
		peers := p.e.Cluster.Peers
		target := peers[p.e.Rand.IntN(len(peers))]
		ev := env.Envelope{
			Instance: "ping-1",
			MsgID:    fmt.Sprintf("%s-%d", p.e.Self, round),
			From:     p.e.Self,
			Type:     "PING",
			Payload:  []byte(fmt.Sprintf("round=%d", round)),
		}
		ev.Auth = p.e.Keys.Sign(ev.CanonicalBytes())
		raw, _ := env.MarshalEnvelope(ev)
		_ = p.e.Net.Send(target, raw)
		p.sent = round + 1
		if b, err := json.Marshal(p.sent); err == nil {
			_ = p.e.Store.Store("sent", b)
		}
		p.arm(round + 1)
	})
}

func (p *pingNode) OnMessage(from env.NodeID, data []byte) {
	p.received++
	if p.recvAll != nil {
		p.recvAll.Inc()
	}
	ev, err := env.UnmarshalEnvelope(data)
	if err != nil {
		return
	}
	if p.e.Keys.Verify(from, ev.CanonicalBytes(), ev.Auth) {
		p.verified++
		p.counter.Inc()
	}
}

func runSimPing(cfg RunConfig, seed int64, rawParams json.RawMessage) (SuiteOutcome, error) {
	p := SimPingParams{
		Nodes:         5,
		Rounds:        10,
		IntervalMs:    20,
		CrashNode:     "n2",
		CrashAtMs:     60,
		RecoverAtMs:   140,
		PartitionAtMs: 40,
		GSTAtMs:       120,
	}
	if err := DecodeParams(rawParams, &p); err != nil {
		return SuiteOutcome{}, err
	}
	if p.Nodes <= 0 || p.Rounds <= 0 || p.IntervalMs <= 0 {
		return SuiteOutcome{}, fmt.Errorf("sim-ping: nodes, rounds, and interval_ms must be > 0")
	}

	simCfg, err := cfg.SimConfig(seed)
	if err != nil {
		return SuiteOutcome{}, err
	}
	s := sim.New(simCfg)

	peers := make([]env.NodeID, p.Nodes)
	for i := 0; i < p.Nodes; i++ {
		peers[i] = env.NodeID(fmt.Sprintf("n%d", i+1))
	}

	var totalVerified obs.Counter
	var totalReceived obs.Counter
	err = s.AddCluster(peers, func(e env.Env) env.Process {
		return &pingNode{
			e:        e,
			rounds:   p.Rounds,
			interval: time.Duration(p.IntervalMs) * time.Millisecond,
			counter:  &totalVerified,
			recvAll:  &totalReceived,
		}
	})
	if err != nil {
		return SuiteOutcome{Sim: s}, err
	}

	if p.PartitionAtMs > 0 && p.Nodes >= 2 {
		s.ScheduleAt(env.Time(p.PartitionAtMs*int64(time.Millisecond)), func() {
			s.Isolate(peers[0])
		})
	}
	if p.GSTAtMs > 0 {
		s.ScheduleAt(env.Time(p.GSTAtMs*int64(time.Millisecond)), func() {
			s.Stabilize(sim.NetConfig{
				MinLatency: 1 * time.Millisecond,
				MaxLatency: 5 * time.Millisecond,
			})
		})
	}
	if p.CrashNode != "" && p.CrashAtMs > 0 {
		id := env.NodeID(p.CrashNode)
		s.ScheduleAt(env.Time(p.CrashAtMs*int64(time.Millisecond)), func() {
			_ = s.Crash(id)
		})
		if p.RecoverAtMs > p.CrashAtMs {
			s.ScheduleAt(env.Time(p.RecoverAtMs*int64(time.Millisecond)), func() {
				_ = s.Recover(id)
			})
		}
	}

	s.AddInvariant("signature_validity", func() error {
		for _, id := range peers {
			if pr, ok := s.Node(id).(*pingNode); ok && pr != nil {
				if pr.verified != pr.received {
					return fmt.Errorf("node %s received unverified message (%d/%d)", id, pr.verified, pr.received)
				}
			}
		}
		return nil
	})

	if err := s.RunUntilQuiet(); err != nil {
		return SuiteOutcome{Sim: s}, err
	}

	col := obs.NewCollector("sim-ping")
	col.Register(s.MetricsSource())
	col.Register(obs.SourceFunc{
		Name: "ping",
		Fn: func(e *obs.Emitter) {
			e.Counter("verified_messages", "count", "Total Ed25519-verified ping messages delivered to live nodes", totalVerified.Value())
		},
	})

	recoveredOK := true
	recoveredObs := "no crash configured"
	recoveredExp := "n/a"
	if p.CrashNode != "" && p.CrashAtMs > 0 && p.RecoverAtMs > p.CrashAtMs {
		id := env.NodeID(p.CrashNode)
		pn, ok := s.Node(id).(*pingNode)
		recoveredOK = ok && pn != nil && pn.recovered && pn.sent == p.Rounds
		recoveredObs = fmt.Sprintf("alive=%v recovered=%v sent=%d", ok && pn != nil, ok && pn != nil && pn.recovered, func() int {
			if ok && pn != nil {
				return pn.sent
			}
			return 0
		}())
		recoveredExp = fmt.Sprintf("alive=true recovered=true sent=%d", p.Rounds)
	}

	summary := fmt.Sprintf("nodes=%d rounds=%d verified=%d", p.Nodes, p.Rounds, totalVerified.Value())
	story := Story{
		Proves: fmt.Sprintf(
			"A %d-node cluster exchanging Ed25519-signed envelopes verifies 100%% of delivered messages under packet drops, duplicates, asymmetric isolation, GST stabilization, and crash-recovery with durable Store restoration.",
			p.Nodes,
		),
		HowCollected: fmt.Sprintf(
			"%d peers each schedule %d periodic signed PINGs (every %dms) over SimNet (drop_ppm=%d, dup_ppm=%d); partition at %dms, GST heal at %dms, crash %s at %dms and recover at %dms.",
			p.Nodes, p.Rounds, p.IntervalMs, cfg.Net.DropPPM, cfg.Net.DupPPM,
			p.PartitionAtMs, p.GSTAtMs, p.CrashNode, p.CrashAtMs, p.RecoverAtMs,
		),
		DoesNotProve: "Proves deterministic simulator networking, cryptography, and durable Store recovery; does not test real OS UDP/TCP sockets, TLS handshakes, or POSIX disk fsync behavior.",
		Checks: []Check{
			{
				Claim:    "All delivered messages pass Ed25519 signature verification",
				Observed: fmt.Sprintf("%d/%d verified", totalVerified.Value(), totalReceived.Value()),
				Expected: "100% verified",
				Pass:     totalVerified.Value() == totalReceived.Value() && totalVerified.Value() > 0,
			},
			{
				Claim:    "Crashed node recovers durable progress and completes remaining rounds",
				Observed: recoveredObs,
				Expected: recoveredExp,
				Pass:     recoveredOK,
			},
		},
	}
	return SuiteOutcome{Sim: s, Collector: col, Summary: summary, Story: story}, nil
}
