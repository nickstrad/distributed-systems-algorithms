package sim

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"strconv"
	"time"

	"github.com/nickstrad/distributed-systems-algorithms/env"
	"github.com/nickstrad/distributed-systems-algorithms/obs"
)

// PPM is the denominator (parts per million) for integer fault rates.
const PPM = 1_000_000

// NetConfig controls latency, drops, and duplicates on the simulated network.
type NetConfig struct {
	MinLatency    time.Duration
	MaxLatency    time.Duration
	DropPPM       int // 0..1_000_000
	DupPPM        int // 0..1_000_000
	MaxDuplicates int // extra copies delivered when a message duplicates (default 1)
}

type linkKey struct {
	from env.NodeID
	to   env.NodeID
}

type simNet struct {
	s          *Sim
	cfg        NetConfig
	rng        *rand.Rand
	blocked    map[linkKey]bool
	sent       obs.Counter
	dropped    obs.Counter
	duplicated obs.Counter
	delivered  obs.Counter
	latency    *obs.Histogram
}

func newSimNet(s *Sim, cfg NetConfig) *simNet {
	return &simNet{
		s:       s,
		cfg:     normalizeNetConfig(cfg),
		rng:     s.Stream("net"),
		blocked: make(map[linkKey]bool),
		latency: obs.NewDurationHistogram(),
	}
}

func normalizeNetConfig(cfg NetConfig) NetConfig {
	if cfg.MinLatency < 0 {
		cfg.MinLatency = 0
	}
	if cfg.MaxLatency < cfg.MinLatency {
		cfg.MaxLatency = cfg.MinLatency
	}
	if cfg.DropPPM < 0 {
		cfg.DropPPM = 0
	}
	if cfg.DropPPM > PPM {
		cfg.DropPPM = PPM
	}
	if cfg.DupPPM < 0 {
		cfg.DupPPM = 0
	}
	if cfg.DupPPM > PPM {
		cfg.DupPPM = PPM
	}
	if cfg.MaxDuplicates <= 0 {
		cfg.MaxDuplicates = 1
	}
	return cfg
}

func (n *simNet) drawLatency() time.Duration {
	span := n.cfg.MaxLatency - n.cfg.MinLatency
	if span <= 0 {
		return n.cfg.MinLatency
	}
	return n.cfg.MinLatency + time.Duration(n.rng.Int64N(int64(span)+1))
}

func (n *simNet) send(fromNode *simNode, inc uint64, to env.NodeID, data []byte) error {
	if !fromNode.alive || fromNode.incarnation != inc {
		return env.ErrClosed
	}
	dst, ok := n.s.nodes[to]
	if !ok {
		return fmt.Errorf("%w: %s", env.ErrUnknownPeer, to)
	}
	from := fromNode.id
	n.sent.Inc()
	if n.blocked[linkKey{from: from, to: to}] {
		n.dropped.Inc()
		n.s.record("net.drop", string(from)+"->"+string(to)+" partition")
		return nil
	}
	if n.cfg.DropPPM > 0 && n.rng.IntN(PPM) < n.cfg.DropPPM {
		n.dropped.Inc()
		n.s.record("net.drop", string(from)+"->"+string(to)+" loss")
		return nil
	}

	copies := 1
	if n.cfg.DupPPM > 0 && n.rng.IntN(PPM) < n.cfg.DupPPM {
		extra := 1 + n.rng.IntN(n.cfg.MaxDuplicates)
		copies += extra
		n.duplicated.Add(int64(extra))
	}
	for i := 0; i < copies; i++ {
		payload := slices.Clone(data)
		delay := n.drawLatency()
		at := n.s.now.Add(delay)
		detail := string(from) + "->" + string(to) + " len=" + strconv.Itoa(len(payload))
		n.s.enqueueEvent(at, "net.deliver", detail, func() {
			if !dst.alive {
				return
			}
			n.delivered.Inc()
			n.latency.Record(int64(delay))
			dst.proc.OnMessage(from, payload)
		})
	}
	return nil
}

// nodeTransport binds RawTransport to a specific node incarnation.
type nodeTransport struct {
	net  *simNet
	node *simNode
	inc  uint64
}

func (t nodeTransport) Send(to env.NodeID, data []byte) error {
	return t.net.send(t.node, t.inc, to, data)
}
