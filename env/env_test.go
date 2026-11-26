// Story: env's deterministic building blocks behave cleanly at boundaries and under mutation.
//
// What these tests prove: saturating Time arithmetic, canonical Cluster peer sorting, FIFO semantics and capacity panics, collision-free Envelope canonical bytes, and deterministic KeyRing signatures/MACs.
// How they probe: table-driven boundary cases (MinTime/MaxTime), shared SliceFIFO/ChanFIFO suites with compaction and panic checks, JSON/byte-collision assertions, and tampered-input crypto checks.
// What they do not prove: multi-node scheduling, network faults, or randomized property equivalence (covered in env/errors_log_test.go and sim/).
package env_test

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"math"
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	"github.com/nickstrad/distributed-systems-algorithms/env"
)

func TestTimeArithmeticAndString(t *testing.T) {
	tests := []struct {
		name string
		t    env.Time
		d    time.Duration
		want env.Time
	}{
		{name: "zero plus positive", t: 0, d: 500, want: 500},
		{name: "positive plus negative", t: 1000, d: -250, want: 750},
		{name: "negative plus positive", t: -100, d: 250, want: 150},
		{name: "max plus zero", t: env.MaxTime, d: 0, want: env.MaxTime},
		{name: "max plus positive saturates", t: env.MaxTime, d: 1, want: env.MaxTime},
		{name: "near max overflow saturates", t: env.MaxTime - 10, d: 11, want: env.MaxTime},
		{name: "exact max boundary", t: env.MaxTime - 10, d: 10, want: env.MaxTime},
		{name: "min plus zero", t: env.MinTime, d: 0, want: env.MinTime},
		{name: "min plus negative saturates", t: env.MinTime, d: -1, want: env.MinTime},
		{name: "near min underflow saturates", t: env.MinTime + 10, d: -11, want: env.MinTime},
		{name: "exact min boundary", t: env.MinTime + 10, d: -10, want: env.MinTime},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.t.Add(tc.d); got != tc.want {
				t.Fatalf("Time(%d).Add(%d) = %d, want %d", tc.t, tc.d, got, tc.want)
			}
		})
	}

	subTests := []struct {
		name string
		t    env.Time
		u    env.Time
		want time.Duration
	}{
		{name: "normal positive diff", t: 1000, u: 250, want: 750},
		{name: "normal negative diff", t: 250, u: 1000, want: -750},
		{name: "equal", t: 42, u: 42, want: 0},
		{name: "max minus min saturates", t: env.MaxTime, u: env.MinTime, want: time.Duration(math.MaxInt64)},
		{name: "positive minus large negative saturates", t: 10, u: env.MinTime + 5, want: time.Duration(math.MaxInt64)},
		{name: "min minus max saturates", t: env.MinTime, u: env.MaxTime, want: time.Duration(math.MinInt64)},
		{name: "negative minus large positive saturates", t: -10, u: env.MaxTime - 5, want: time.Duration(math.MinInt64)},
		{name: "min minus min is zero", t: env.MinTime, u: env.MinTime, want: 0},
		{name: "max minus max is zero", t: env.MaxTime, u: env.MaxTime, want: 0},
	}
	for _, tc := range subTests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.t.Sub(tc.u); got != tc.want {
				t.Fatalf("Time(%d).Sub(%d) = %d, want %d", tc.t, tc.u, got, tc.want)
			}
		})
	}

	if got := env.Time(0).String(); got != "0ns" {
		t.Fatalf("Time(0).String() = %q, want %q", got, "0ns")
	}
	if got := env.Time(-42).String(); got != "-42ns" {
		t.Fatalf("Time(-42).String() = %q, want %q", got, "-42ns")
	}
	if got := env.Time(12345).String(); got != "12345ns" {
		t.Fatalf("Time(12345).String() = %q, want %q", got, "12345ns")
	}
}

func TestCluster(t *testing.T) {
	input := []env.NodeID{"n3", "n1", "n2", "n1", "n3"}
	origInput := slices.Clone(input)

	c := env.NewCluster("n2", input)

	if !slices.Equal(input, origInput) {
		t.Fatalf("NewCluster mutated caller slice: got %v, want %v", input, origInput)
	}
	if c.Self != "n2" {
		t.Fatalf("Self = %q, want %q", c.Self, "n2")
	}
	wantPeers := []env.NodeID{"n1", "n2", "n3"}
	if !slices.Equal(c.Peers, wantPeers) {
		t.Fatalf("Peers = %v, want %v", c.Peers, wantPeers)
	}
	if c.N() != 3 {
		t.Fatalf("N() = %d, want 3", c.N())
	}

	if got := c.Rank("n1"); got != 1 {
		t.Fatalf("Rank(n1) = %d, want 1", got)
	}
	if got := c.Rank("n2"); got != 2 {
		t.Fatalf("Rank(n2) = %d, want 2", got)
	}
	if got := c.Rank("n3"); got != 3 {
		t.Fatalf("Rank(n3) = %d, want 3", got)
	}
	if got := c.Rank("n4"); got != 0 {
		t.Fatalf("Rank(n4) = %d, want 0", got)
	}

	if !c.Contains("n1") || !c.Contains("n3") {
		t.Fatal("Contains returned false for member")
	}
	if c.Contains("absent") {
		t.Fatal("Contains returned true for absent node")
	}

	cp := c.PeersCopy()
	if !slices.Equal(cp, wantPeers) {
		t.Fatalf("PeersCopy() = %v, want %v", cp, wantPeers)
	}
	cp[0] = "tampered"
	if !slices.Equal(c.Peers, wantPeers) {
		t.Fatalf("mutating PeersCopy mutated Cluster.Peers: %v", c.Peers)
	}

	empty := env.NewCluster("n1", nil)
	if empty.N() != 0 {
		t.Fatalf("empty.N() = %d, want 0", empty.N())
	}
	if empty.Majority() != 0 {
		t.Fatalf("empty.Majority() = %d, want 0", empty.Majority())
	}
	if empty.ByzantineQuorum(0) != 0 || empty.ByzantineQuorum(1) != 0 {
		t.Fatal("empty.ByzantineQuorum should be 0")
	}

	majorityCases := []struct {
		peers []env.NodeID
		want  int
	}{
		{[]env.NodeID{"a"}, 1},
		{[]env.NodeID{"a", "b"}, 2},
		{[]env.NodeID{"a", "b", "c"}, 2},
		{[]env.NodeID{"a", "b", "c", "d"}, 3},
		{[]env.NodeID{"a", "b", "c", "d", "e"}, 3},
	}
	for _, tc := range majorityCases {
		cl := env.NewCluster("a", tc.peers)
		if got := cl.Majority(); got != tc.want {
			t.Fatalf("N=%d Majority() = %d, want %d", cl.N(), got, tc.want)
		}
	}

	bftCases := []struct {
		n    int
		f    int
		want int
	}{
		{n: 4, f: 1, want: 3},
		{n: 7, f: 2, want: 5},
		{n: 3, f: 0, want: 2},
		{n: 4, f: 0, want: 3},
		{n: 4, f: -1, want: 0},
	}
	for _, tc := range bftCases {
		peers := make([]env.NodeID, tc.n)
		for i := range peers {
			peers[i] = env.NodeID("p" + string(rune('a'+i)))
		}
		cl := env.NewCluster("pa", peers)
		if got := cl.ByzantineQuorum(tc.f); got != tc.want {
			t.Fatalf("N=%d f=%d ByzantineQuorum = %d, want %d", tc.n, tc.f, got, tc.want)
		}
	}
}

func TestFIFOShared(t *testing.T) {
	factories := []struct {
		name string
		new  func() env.FIFO[int]
	}{
		{name: "SliceFIFO", new: func() env.FIFO[int] { return env.NewSliceFIFO[int]() }},
		{name: "ChanFIFO", new: func() env.FIFO[int] { return env.NewChanFIFO[int](32) }},
	}

	for _, impl := range factories {
		t.Run(impl.name, func(t *testing.T) {
			q := impl.new()

			if q.Len() != 0 {
				t.Fatalf("initial Len() = %d, want 0", q.Len())
			}
			if v, ok := q.Pop(); ok || v != 0 {
				t.Fatalf("empty Pop() = (%d, %v), want (0, false)", v, ok)
			}

			for i := 1; i <= 10; i++ {
				q.Push(i)
				if q.Len() != i {
					t.Fatalf("Len() = %d, want %d", q.Len(), i)
				}
			}

			for i := 1; i <= 5; i++ {
				v, ok := q.Pop()
				if !ok || v != i {
					t.Fatalf("Pop() = (%d, %v), want (%d, true)", v, ok, i)
				}
			}
			if q.Len() != 5 {
				t.Fatalf("Len() after 5 pops = %d, want 5", q.Len())
			}

			for i := 11; i <= 15; i++ {
				q.Push(i)
			}
			for i := 6; i <= 15; i++ {
				v, ok := q.Pop()
				if !ok || v != i {
					t.Fatalf("Pop() = (%d, %v), want (%d, true)", v, ok, i)
				}
			}
			if q.Len() != 0 {
				t.Fatalf("Len() after full drain = %d, want 0", q.Len())
			}
			if v, ok := q.Pop(); ok || v != 0 {
				t.Fatalf("drained Pop() = (%d, %v), want (0, false)", v, ok)
			}

			q.Push(100)
			q.Push(200)
			q.Clear()
			if q.Len() != 0 {
				t.Fatalf("Len() after Clear = %d, want 0", q.Len())
			}
			if v, ok := q.Pop(); ok || v != 0 {
				t.Fatalf("Pop() after Clear = (%d, %v), want (0, false)", v, ok)
			}

			q.Push(300)
			if v, ok := q.Pop(); !ok || v != 300 {
				t.Fatalf("Pop() after reuse = (%d, %v), want (300, true)", v, ok)
			}
		})
	}
}

func TestSliceFIFOCompaction(t *testing.T) {
	q := env.NewSliceFIFO[int]()
	nextPush := 0
	nextPop := 0

	// Push an initial batch so head stays behind tail and hits compaction (head >= 64 && head*2 >= len).
	for i := 0; i < 120; i++ {
		q.Push(nextPush)
		nextPush++
	}
	for cycle := 0; cycle < 250; cycle++ {
		q.Push(nextPush)
		nextPush++

		v, ok := q.Pop()
		if !ok || v != nextPop {
			t.Fatalf("cycle %d: Pop() = (%d, %v), want (%d, true)", cycle, v, ok, nextPop)
		}
		nextPop++

		if cycle%3 == 0 && q.Len() > 10 {
			v2, ok2 := q.Pop()
			if !ok2 || v2 != nextPop {
				t.Fatalf("cycle %d extra pop: Pop() = (%d, %v), want (%d, true)", cycle, v2, ok2, nextPop)
			}
			nextPop++
		}
	}

	wantLen := nextPush - nextPop
	if q.Len() != wantLen {
		t.Fatalf("Len() = %d, want %d", q.Len(), wantLen)
	}
	for nextPop < nextPush {
		v, ok := q.Pop()
		if !ok || v != nextPop {
			t.Fatalf("final drain Pop() = (%d, %v), want (%d, true)", v, ok, nextPop)
		}
		nextPop++
	}
	if q.Len() != 0 {
		t.Fatalf("Len() after final drain = %d, want 0", q.Len())
	}
}

func TestChanFIFOPanics(t *testing.T) {
	for _, capVal := range []int{0, -1, -10} {
		func() {
			defer func() {
				if r := recover(); r == nil {
					t.Fatalf("NewChanFIFO(%d) did not panic", capVal)
				}
			}()
			_ = env.NewChanFIFO[int](capVal)
		}()
	}

	q := env.NewChanFIFO[int](2)
	q.Push(1)
	q.Push(2)
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("Push beyond ChanFIFO capacity did not panic")
		}
	}()
	q.Push(3)
}

func TestEnvelopeMarshalAndCanonicalBytes(t *testing.T) {
	orig := env.Envelope{
		Instance: "consensus-1",
		MsgID:    "msg-42",
		From:     "n1",
		Type:     "propose",
		Payload:  []byte{0x01, 0x02, 0x03, 0xff},
		Auth:     []byte{0xaa, 0xbb},
	}

	raw, err := env.MarshalEnvelope(orig)
	if err != nil {
		t.Fatalf("MarshalEnvelope error: %v", err)
	}
	got, err := env.UnmarshalEnvelope(raw)
	if err != nil {
		t.Fatalf("UnmarshalEnvelope error: %v", err)
	}
	if got.Instance != orig.Instance || got.MsgID != orig.MsgID || got.From != orig.From ||
		got.Type != orig.Type || !bytes.Equal(got.Payload, orig.Payload) || !bytes.Equal(got.Auth, orig.Auth) {
		t.Fatalf("roundtrip mismatch: got %+v, want %+v", got, orig)
	}

	if _, err := env.UnmarshalEnvelope([]byte("{bad-json")); err == nil {
		t.Fatal("UnmarshalEnvelope expected error on invalid JSON")
	}

	// CanonicalBytes must ignore Auth.
	withDiffAuth := orig
	withDiffAuth.Auth = []byte("completely-different-signature")
	if !bytes.Equal(orig.CanonicalBytes(), withDiffAuth.CanonicalBytes()) {
		t.Fatal("CanonicalBytes changed when only Auth changed")
	}

	// CanonicalBytes must be length-prefixed and collision-free across adjacent fields.
	pairs := [][2]env.Envelope{
		{
			{Instance: "ab", MsgID: "c"},
			{Instance: "a", MsgID: "bc"},
		},
		{
			{MsgID: "ab", From: "c"},
			{MsgID: "a", From: "bc"},
		},
		{
			{From: "ab", Type: "c"},
			{From: "a", Type: "bc"},
		},
		{
			{Type: "ab", Payload: []byte("c")},
			{Type: "a", Payload: []byte("bc")},
		},
	}
	for i, p := range pairs {
		if bytes.Equal(p[0].CanonicalBytes(), p[1].CanonicalBytes()) {
			t.Fatalf("pair %d produced colliding CanonicalBytes", i)
		}
	}
}

func TestCryptoHashAndKeyRings(t *testing.T) {
	msg := []byte("hello distributed world")
	if got, want := env.Hash(msg), sha256.Sum256(msg); got != want {
		t.Fatalf("Hash mismatch: got %x, want %x", got, want)
	}

	if _, err := env.GenerateKeyRings([]env.NodeID{"n1"}, nil); !errors.Is(err, env.ErrInvalidArgument) {
		t.Fatalf("GenerateKeyRings(nil rng) err = %v, want ErrInvalidArgument", err)
	}

	r1 := rand.New(rand.NewPCG(7, 11))
	r2 := rand.New(rand.NewPCG(7, 11))
	ringsA, err := env.GenerateKeyRings([]env.NodeID{"n3", "n1", "n2", "n1"}, r1)
	if err != nil {
		t.Fatalf("GenerateKeyRings A error: %v", err)
	}
	ringsB, err := env.GenerateKeyRings([]env.NodeID{"n2", "n3", "n1"}, r2)
	if err != nil {
		t.Fatalf("GenerateKeyRings B error: %v", err)
	}
	if len(ringsA) != 3 || len(ringsB) != 3 {
		t.Fatalf("unexpected ring count: %d, %d", len(ringsA), len(ringsB))
	}

	for _, id := range []env.NodeID{"n1", "n2", "n3"} {
		pubA, okA := ringsA[id].PublicKey(id)
		pubB, okB := ringsB[id].PublicKey(id)
		if !okA || !okB || !bytes.Equal(pubA, pubB) {
			t.Fatalf("PublicKey(%s) mismatch across peer orderings", id)
		}
		sigA := ringsA[id].Sign(msg)
		sigB := ringsB[id].Sign(msg)
		if !bytes.Equal(sigA, sigB) {
			t.Fatalf("Sign(%s) not deterministic across peer orderings", id)
		}
	}

	// Sign / Verify checks.
	sig := ringsA["n1"].Sign(msg)
	if !ringsA["n2"].Verify("n1", msg, sig) {
		t.Fatal("Verify rejected valid signature")
	}
	tamperedMsg := slices.Clone(msg)
	tamperedMsg[0] ^= 0xff
	if ringsA["n2"].Verify("n1", tamperedMsg, sig) {
		t.Fatal("Verify accepted tampered message")
	}
	tamperedSig := slices.Clone(sig)
	tamperedSig[0] ^= 0xff
	if ringsA["n2"].Verify("n1", msg, tamperedSig) {
		t.Fatal("Verify accepted tampered signature")
	}
	if ringsA["n2"].Verify("n1", msg, sig[:len(sig)-1]) {
		t.Fatal("Verify accepted short signature")
	}
	if ringsA["n2"].Verify("n3", msg, sig) {
		t.Fatal("Verify accepted signature under wrong peer")
	}
	if ringsA["n2"].Verify("unknown", msg, sig) {
		t.Fatal("Verify accepted unknown peer")
	}

	// PublicKey returns an independent clone.
	pub, ok := ringsA["n2"].PublicKey("n1")
	if !ok {
		t.Fatal("PublicKey(n1) returned false")
	}
	pub[0] ^= 0xff
	if !ringsA["n2"].Verify("n1", msg, sig) {
		t.Fatal("mutating returned PublicKey affected Verify")
	}
	if _, ok := ringsA["n2"].PublicKey("unknown"); ok {
		t.Fatal("PublicKey(unknown) returned true")
	}

	// MAC / VerifyMAC checks.
	tag12 := ringsA["n1"].MAC("n2", msg)
	if len(tag12) == 0 {
		t.Fatal("MAC returned empty tag")
	}
	if !ringsA["n2"].VerifyMAC("n1", msg, tag12) {
		t.Fatal("VerifyMAC rejected valid symmetric tag")
	}
	if !ringsA["n1"].VerifyMAC("n2", msg, tag12) {
		t.Fatal("VerifyMAC rejected valid symmetric tag on sender side")
	}
	if ringsA["n2"].VerifyMAC("n1", tamperedMsg, tag12) {
		t.Fatal("VerifyMAC accepted tampered message")
	}
	tamperedTag := slices.Clone(tag12)
	tamperedTag[0] ^= 0xff
	if ringsA["n2"].VerifyMAC("n1", msg, tamperedTag) {
		t.Fatal("VerifyMAC accepted tampered tag")
	}
	if ringsA["n3"].VerifyMAC("n1", msg, tag12) {
		t.Fatal("VerifyMAC accepted tag under wrong peer key")
	}
	if ringsA["n2"].VerifyMAC("unknown", msg, tag12) {
		t.Fatal("VerifyMAC accepted unknown peer")
	}
	if tag := ringsA["n1"].MAC("unknown", msg); tag != nil {
		t.Fatalf("MAC(unknown) = %v, want nil", tag)
	}

	// Nil KeyRing safety.
	var nilRing *env.KeyRing
	if sig := nilRing.Sign(msg); sig != nil {
		t.Fatalf("nilRing.Sign = %v, want nil", sig)
	}
	if nilRing.Verify("n1", msg, sig) {
		t.Fatal("nilRing.Verify returned true")
	}
	if pub, ok := nilRing.PublicKey("n1"); pub != nil || ok {
		t.Fatalf("nilRing.PublicKey = (%v, %v), want (nil, false)", pub, ok)
	}
	if tag := nilRing.MAC("n1", msg); tag != nil {
		t.Fatalf("nilRing.MAC = %v, want nil", tag)
	}
	if nilRing.VerifyMAC("n1", msg, tag12) {
		t.Fatal("nilRing.VerifyMAC returned true")
	}

	// BaseProcess smoke check.
	var bp env.BaseProcess
	bp.Init()
	bp.Recover()
	bp.OnMessage("n1", msg)
}
