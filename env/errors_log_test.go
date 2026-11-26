// Story: Wire error codes, log formatting, and env value types preserve invariants across arbitrary inputs.
//
// What these tests prove: built-in and custom CodeTable encoding, MaxErrorMessage truncation, RemoteError relaying/matching, deterministic FormatLogAttrs quoting, and CodeTable/FIFO/Time/Envelope properties.
// How they probe: unit boundary checks on error tables and log attrs paired with rapid property tests checking round-trips, big.Int reference arithmetic, and SliceFIFO/ChanFIFO model equivalence.
// What they do not prove: simulator event-loop integration (sim.Logger, crash/recovery) or live metric/log export pipelines (obs/).
package env_test

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/nickstrad/distributed-systems-algorithms/env"
	"pgregory.net/rapid"
)

func TestDefaultCodesBuiltInSentinels(t *testing.T) {
	codes := env.DefaultCodes()

	code, msg := codes.Encode(nil)
	if code != env.CodeOK || msg != "" {
		t.Fatalf("Encode(nil) = (%d, %q), want (%d, %q)", code, msg, env.CodeOK, "")
	}
	if err := codes.Decode(env.CodeOK, "ignored"); err != nil {
		t.Fatalf("Decode(CodeOK) = %v, want nil", err)
	}

	builtins := []struct {
		name     string
		sentinel error
		code     env.Code
	}{
		{name: "ErrTimeout", sentinel: env.ErrTimeout, code: env.CodeTimeout},
		{name: "ErrUnreachable", sentinel: env.ErrUnreachable, code: env.CodeUnreachable},
		{name: "ErrClosed", sentinel: env.ErrClosed, code: env.CodeClosed},
		{name: "ErrTooLarge", sentinel: env.ErrTooLarge, code: env.CodeTooLarge},
		{name: "ErrRemote", sentinel: env.ErrRemote, code: env.CodeRemote},
		{name: "ErrNotFound", sentinel: env.ErrNotFound, code: env.CodeNotFound},
		{name: "ErrIO", sentinel: env.ErrIO, code: env.CodeIO},
		{name: "ErrInvalidArgument", sentinel: env.ErrInvalidArgument, code: env.CodeInvalidArgument},
		{name: "ErrUnknownPeer", sentinel: env.ErrUnknownPeer, code: env.CodeUnknownPeer},
	}

	for _, tc := range builtins {
		t.Run(tc.name, func(t *testing.T) {
			gotCode, gotMsg := codes.Encode(tc.sentinel)
			if gotCode != tc.code {
				t.Fatalf("Encode(%v) code = %d, want %d", tc.sentinel, gotCode, tc.code)
			}
			if gotMsg != tc.sentinel.Error() {
				t.Fatalf("Encode(%v) msg = %q, want %q", tc.sentinel, gotMsg, tc.sentinel.Error())
			}

			decoded := codes.Decode(gotCode, gotMsg)
			if !errors.Is(decoded, tc.sentinel) {
				t.Fatalf("Decode(%d) does not match %v", gotCode, tc.sentinel)
			}
			if !errors.Is(decoded, env.ErrRemote) {
				t.Fatalf("Decode(%d) does not match ErrRemote", gotCode)
			}

			wrapped := fmt.Errorf("context: %w", tc.sentinel)
			wCode, wMsg := codes.Encode(wrapped)
			if wCode != tc.code {
				t.Fatalf("Encode(wrapped) code = %d, want %d", wCode, tc.code)
			}
			wDecoded := codes.Decode(wCode, wMsg)
			if !errors.Is(wDecoded, tc.sentinel) || !errors.Is(wDecoded, env.ErrRemote) {
				t.Fatalf("Decode(wrapped) failed errors.Is checks: %v", wDecoded)
			}
		})
	}

	other := errors.New("unregistered error")
	otherCode, otherMsg := codes.Encode(other)
	if otherCode != env.CodeRemote || otherMsg != "unregistered error" {
		t.Fatalf("Encode(other) = (%d, %q), want (%d, %q)", otherCode, otherMsg, env.CodeRemote, "unregistered error")
	}
	decodedOther := codes.Decode(otherCode, otherMsg)
	if !errors.Is(decodedOther, env.ErrRemote) {
		t.Fatalf("Decode(CodeRemote) does not match ErrRemote")
	}
}

func TestCustomCodeTable(t *testing.T) {
	appWrapped := fmt.Errorf("app conflict: %w", env.ErrNotFound)
	appIndependent := errors.New("app: stale term")

	tbl, err := env.NewCodeTable(
		env.CodeEntry{Code: env.CodeAppMin, Err: appWrapped},
		env.CodeEntry{Code: env.Code(255), Err: appIndependent},
	)
	if err != nil {
		t.Fatalf("NewCodeTable failed: %v", err)
	}

	// App entry wrapping a built-in sentinel takes precedence over the built-in code.
	code, _ := tbl.Encode(appWrapped)
	if code != env.CodeAppMin {
		t.Fatalf("Encode(appWrapped) = %d, want %d", code, env.CodeAppMin)
	}
	decoded := tbl.Decode(code, "conflict")
	if !errors.Is(decoded, appWrapped) || !errors.Is(decoded, env.ErrNotFound) || !errors.Is(decoded, env.ErrRemote) {
		t.Fatalf("Decode(%d) did not preserve sentinel chain: %v", code, decoded)
	}

	// Error matching both a built-in and an app sentinel picks the app entry first.
	joined := errors.Join(env.ErrTimeout, appIndependent)
	jCode, _ := tbl.Encode(joined)
	if jCode != env.Code(255) {
		t.Fatalf("Encode(joined) = %d, want 255", jCode)
	}

	// Entries returns a defensive copy in match order.
	entries := tbl.Entries()
	if len(entries) < 2 || entries[0].Code != env.CodeAppMin || entries[1].Code != env.Code(255) {
		t.Fatalf("unexpected Entries prefix: %v", entries)
	}
	entries[0] = env.CodeEntry{Code: env.CodeOK, Err: nil}
	entries[len(entries)-1] = env.CodeEntry{Code: 99, Err: errors.New("mutated")}

	fresh := tbl.Entries()
	if fresh[0].Code != env.CodeAppMin || fresh[0].Err != appWrapped {
		t.Fatalf("Entries mutation leaked into table: %+v", fresh[0])
	}
	if fresh[len(fresh)-1].Code != env.CodeRemote {
		t.Fatalf("Entries tail mutation leaked into table: %+v", fresh[len(fresh)-1])
	}

	// Invalid registrations.
	invalidCases := []struct {
		name    string
		entries []env.CodeEntry
	}{
		{
			name:    "code zero below CodeAppMin",
			entries: []env.CodeEntry{{Code: env.CodeOK, Err: errors.New("a")}},
		},
		{
			name:    "code remote below CodeAppMin",
			entries: []env.CodeEntry{{Code: env.CodeRemote, Err: errors.New("a")}},
		},
		{
			name:    "code CodeAppMin-1",
			entries: []env.CodeEntry{{Code: env.CodeAppMin - 1, Err: errors.New("a")}},
		},
		{
			name:    "nil error",
			entries: []env.CodeEntry{{Code: env.CodeAppMin, Err: nil}},
		},
		{
			name: "duplicate code",
			entries: []env.CodeEntry{
				{Code: env.CodeAppMin, Err: errors.New("a")},
				{Code: env.CodeAppMin, Err: errors.New("b")},
			},
		},
	}
	for _, tc := range invalidCases {
		t.Run(tc.name, func(t *testing.T) {
			got, regErr := env.NewCodeTable(tc.entries...)
			if regErr == nil {
				t.Fatalf("expected error, got table %v", got)
			}
			if !errors.Is(regErr, env.ErrInvalidArgument) {
				t.Fatalf("error %v does not wrap ErrInvalidArgument", regErr)
			}
		})
	}
}

func TestCodeTableMaxErrorMessageTruncation(t *testing.T) {
	codes := env.DefaultCodes()

	exactMsg := strings.Repeat("a", env.MaxErrorMessage)
	_, gotExact := codes.Encode(errors.New(exactMsg))
	if gotExact != exactMsg {
		t.Fatalf("exact length message altered: len=%d", len(gotExact))
	}

	longMsg := strings.Repeat("b", env.MaxErrorMessage+50)
	code, gotLong := codes.Encode(errors.New(longMsg))
	if code != env.CodeRemote {
		t.Fatalf("code = %d, want %d", code, env.CodeRemote)
	}
	if len(gotLong) != env.MaxErrorMessage {
		t.Fatalf("len(msg) = %d, want %d", len(gotLong), env.MaxErrorMessage)
	}
	if gotLong != longMsg[:env.MaxErrorMessage] {
		t.Fatalf("truncated msg mismatch")
	}
}

func TestRemoteErrorBehavior(t *testing.T) {
	codes := env.DefaultCodes()

	re := codes.Decode(env.CodeTimeout, "timed out")
	if got, want := re.Error(), "remote error (code 2): timed out"; got != want {
		t.Fatalf("Error() = %q, want %q", got, want)
	}
	if !errors.Is(re, env.ErrTimeout) {
		t.Fatalf("expected RemoteError to match ErrTimeout")
	}
	if !errors.Is(re, env.ErrRemote) {
		t.Fatalf("expected RemoteError to match ErrRemote")
	}
	if errors.Is(re, env.ErrClosed) {
		t.Fatalf("unexpected match with ErrClosed")
	}

	// Unknown wire code matches ErrRemote only.
	unknown := codes.Decode(env.Code(200), "custom remote")
	if got, want := unknown.Error(), "remote error (code 200): custom remote"; got != want {
		t.Fatalf("unknown Error() = %q, want %q", got, want)
	}
	if !errors.Is(unknown, env.ErrRemote) {
		t.Fatalf("unknown code should match ErrRemote")
	}
	for _, s := range []error{
		env.ErrTimeout, env.ErrUnreachable, env.ErrClosed, env.ErrTooLarge,
		env.ErrNotFound, env.ErrIO, env.ErrInvalidArgument, env.ErrUnknownPeer,
	} {
		if errors.Is(unknown, s) {
			t.Fatalf("unknown code unexpectedly matched %v", s)
		}
	}
	var asRE *env.RemoteError
	if !errors.As(unknown, &asRE) || asRE.Code != env.Code(200) {
		t.Fatalf("errors.As failed: %v", unknown)
	}

	// Relayed *RemoteError re-encodes to its specific sentinel code, not CodeRemote.
	relayed := codes.Decode(env.CodeNotFound, "missing key")
	reCode, _ := codes.Encode(relayed)
	if reCode != env.CodeNotFound {
		t.Fatalf("relayed Encode = %d, want %d", reCode, env.CodeNotFound)
	}
	wrappedRelay := fmt.Errorf("hop2: %w", relayed)
	wCode, _ := codes.Encode(wrappedRelay)
	if wCode != env.CodeNotFound {
		t.Fatalf("wrapped relayed Encode = %d, want %d", wCode, env.CodeNotFound)
	}

	// Same for custom app sentinel relayed through a custom CodeTable.
	appErr := errors.New("app: custom")
	customTbl, err := env.NewCodeTable(env.CodeEntry{Code: env.CodeAppMin + 3, Err: appErr})
	if err != nil {
		t.Fatalf("NewCodeTable failed: %v", err)
	}
	appRelayed := customTbl.Decode(env.CodeAppMin+3, "boom")
	appCode, _ := customTbl.Encode(fmt.Errorf("hop: %w", appRelayed))
	if appCode != env.CodeAppMin+3 {
		t.Fatalf("relayed app Encode = %d, want %d", appCode, env.CodeAppMin+3)
	}
}

func TestLogLevelString(t *testing.T) {
	tests := []struct {
		level env.Level
		want  string
	}{
		{env.LevelDebug, "debug"},
		{env.LevelInfo, "info"},
		{env.LevelWarn, "warn"},
		{env.LevelError, "error"},
		{env.Level(4), "unknown"},
		{env.Level(255), "unknown"},
	}
	for _, tc := range tests {
		if got := tc.level.String(); got != tc.want {
			t.Fatalf("Level(%d).String() = %q, want %q", tc.level, got, tc.want)
		}
	}
}

func TestLogAttrConstructors(t *testing.T) {
	if got := env.Str("k", "v"); got != (env.Attr{Key: "k", Value: "v"}) {
		t.Fatalf("Str = %+v", got)
	}
	if got := env.Int("n", -42); got != (env.Attr{Key: "n", Value: "-42"}) {
		t.Fatalf("Int = %+v", got)
	}
	if got := env.Int64("i64", math.MaxInt64); got != (env.Attr{Key: "i64", Value: "9223372036854775807"}) {
		t.Fatalf("Int64 = %+v", got)
	}
	if got := env.Bool("b1", true); got != (env.Attr{Key: "b1", Value: "true"}) {
		t.Fatalf("Bool(true) = %+v", got)
	}
	if got := env.Bool("b0", false); got != (env.Attr{Key: "b0", Value: "false"}) {
		t.Fatalf("Bool(false) = %+v", got)
	}
	if got := env.Dur("d", 1500*time.Nanosecond); got != (env.Attr{Key: "d", Value: "1500ns"}) {
		t.Fatalf("Dur = %+v", got)
	}
	if got := env.Dur("dneg", -5*time.Nanosecond); got != (env.Attr{Key: "dneg", Value: "-5ns"}) {
		t.Fatalf("Dur(neg) = %+v", got)
	}
	if got := env.Err(nil); got != (env.Attr{Key: "err", Value: "<nil>"}) {
		t.Fatalf("Err(nil) = %+v", got)
	}
	if got := env.Err(errors.New("disk full")); got != (env.Attr{Key: "err", Value: "disk full"}) {
		t.Fatalf("Err(non-nil) = %+v", got)
	}
}

func TestNopLogger(t *testing.T) {
	var l env.Logger = env.NopLogger{}
	attrs := []env.Attr{env.Str("node", "n1"), env.Int("term", 3)}
	l.Log(env.LevelInfo, "msg", attrs...)
	l.Debug("d", attrs...)
	l.Info("i", attrs...)
	l.Warn("w", attrs...)
	l.Error("e", attrs...)

	var nop env.NopLogger
	nop.Log(env.LevelDebug, "")
	nop.Debug("")
	nop.Info("")
	nop.Warn("")
	nop.Error("")
}

func TestFormatLogAttrs(t *testing.T) {
	attrs := []env.Attr{
		{Key: "", Value: "ignored"},
		env.Str("plain", "abc"),
		env.Str("empty", ""),
		env.Str("space", "hello world"),
		env.Str("tab", "a\tb"),
		env.Str("cr", "a\rb"),
		env.Str("nl", "line1\nline2"),
		env.Str("quote", `say "hi"`),
		env.Str("eq", "k=v"),
		{Key: "", Value: "also ignored"},
	}

	got1 := env.FormatLogAttrs("boot ok", attrs...)
	got2 := env.FormatLogAttrs("boot ok", attrs...)
	if got1 != got2 {
		t.Fatalf("FormatLogAttrs not deterministic:\n%s\n!=\n%s", got1, got2)
	}

	want := `msg="boot ok" plain=abc empty="" space="hello world" tab="a\tb" cr="a\rb" nl="line1\nline2" quote="say \"hi\"" eq="k=v"`
	if got1 != want {
		t.Fatalf("FormatLogAttrs =\n  %s\nwant\n  %s", got1, want)
	}

	if gotEmpty := env.FormatLogAttrs("", env.Str("", "skip")); gotEmpty != `msg=""` {
		t.Fatalf("FormatLogAttrs empty msg = %q, want %q", gotEmpty, `msg=""`)
	}
	if gotPlain := env.FormatLogAttrs("ready", env.Int("port", 8080)); gotPlain != "msg=ready port=8080" {
		t.Fatalf("FormatLogAttrs plain = %q, want %q", gotPlain, "msg=ready port=8080")
	}
}

func TestPropCodeTableRoundTrip(t *testing.T) {
	appErrA := errors.New("app: stale ballot")
	appErrB := errors.New("app: quorum lost")
	tbl, err := env.NewCodeTable(
		env.CodeEntry{Code: env.CodeAppMin, Err: appErrA},
		env.CodeEntry{Code: env.CodeAppMin + 17, Err: appErrB},
	)
	if err != nil {
		t.Fatalf("NewCodeTable failed: %v", err)
	}

	sentinels := []struct {
		err  error
		code env.Code
	}{
		{env.ErrTimeout, env.CodeTimeout},
		{env.ErrUnreachable, env.CodeUnreachable},
		{env.ErrClosed, env.CodeClosed},
		{env.ErrTooLarge, env.CodeTooLarge},
		{env.ErrRemote, env.CodeRemote},
		{env.ErrNotFound, env.CodeNotFound},
		{env.ErrIO, env.CodeIO},
		{env.ErrInvalidArgument, env.CodeInvalidArgument},
		{env.ErrUnknownPeer, env.CodeUnknownPeer},
		{appErrA, env.CodeAppMin},
		{appErrB, env.CodeAppMin + 17},
	}

	rapid.Check(t, func(t *rapid.T) {
		choice := rapid.SampledFrom(sentinels).Draw(t, "sentinel")
		depth := rapid.IntRange(0, 3).Draw(t, "wrapDepth")
		wrapped := choice.err
		for i := range depth {
			prefix := rapid.String().Draw(t, fmt.Sprintf("prefix%d", i))
			wrapped = fmt.Errorf("%s: %w", prefix, wrapped)
		}

		code, msg := tbl.Encode(wrapped)
		if code != choice.code {
			t.Fatalf("Encode code = %d, want %d", code, choice.code)
		}
		if len(msg) > env.MaxErrorMessage {
			t.Fatalf("Encode msg length %d exceeds MaxErrorMessage %d", len(msg), env.MaxErrorMessage)
		}

		decoded := tbl.Decode(code, msg)
		if !errors.Is(decoded, choice.err) {
			t.Fatalf("Decode(%d) lost errors.Is(%v)", code, choice.err)
		}
		if !errors.Is(decoded, env.ErrRemote) {
			t.Fatalf("Decode(%d) lost errors.Is(ErrRemote)", code)
		}

		reCode, reMsg := tbl.Encode(decoded)
		if reCode != code {
			t.Fatalf("re-encode code = %d, want %d", reCode, code)
		}
		if len(reMsg) > env.MaxErrorMessage {
			t.Fatalf("re-encode msg length %d exceeds MaxErrorMessage %d", len(reMsg), env.MaxErrorMessage)
		}
	})
}

func TestPropFIFOModelEquivalence(t *testing.T) {
	const chanCap = 1024
	type opKind uint8
	const (
		opPush opKind = iota
		opPop
		opClear
		opLen
	)
	type fifoOp struct {
		kind opKind
		val  int
	}

	opGen := rapid.Custom(func(t *rapid.T) fifoOp {
		roll := rapid.IntRange(0, 9).Draw(t, "roll")
		switch {
		case roll <= 3:
			return fifoOp{kind: opPush, val: rapid.Int().Draw(t, "val")}
		case roll <= 6:
			return fifoOp{kind: opPop}
		case roll <= 8:
			return fifoOp{kind: opLen}
		default:
			return fifoOp{kind: opClear}
		}
	})

	rapid.Check(t, func(t *rapid.T) {
		ops := rapid.SliceOfN(opGen, 0, 350).Draw(t, "ops")
		sf := env.NewSliceFIFO[int]()
		cf := env.NewChanFIFO[int](chanCap)
		var model []int

		for step, op := range ops {
			switch op.kind {
			case opPush:
				if len(model) >= chanCap {
					continue
				}
				model = append(model, op.val)
				sf.Push(op.val)
				cf.Push(op.val)

			case opPop:
				var wantVal int
				var wantOk bool
				if len(model) > 0 {
					wantVal = model[0]
					wantOk = true
					model = model[1:]
				}
				gotS, okS := sf.Pop()
				gotC, okC := cf.Pop()
				if okS != wantOk || gotS != wantVal {
					t.Fatalf("step %d: SliceFIFO.Pop() = (%d, %v), want (%d, %v)", step, gotS, okS, wantVal, wantOk)
				}
				if okC != wantOk || gotC != wantVal {
					t.Fatalf("step %d: ChanFIFO.Pop() = (%d, %v), want (%d, %v)", step, gotC, okC, wantVal, wantOk)
				}

			case opClear:
				model = nil
				sf.Clear()
				cf.Clear()

			case opLen:
				if sf.Len() != len(model) || cf.Len() != len(model) {
					t.Fatalf("step %d: Len mismatch: slice=%d chan=%d model=%d", step, sf.Len(), cf.Len(), len(model))
				}
			}

			if sf.Len() != len(model) || cf.Len() != len(model) {
				t.Fatalf("step %d post-check: Len mismatch: slice=%d chan=%d model=%d", step, sf.Len(), cf.Len(), len(model))
			}
		}
	})
}

func TestPropTimeAddSubMonotonic(t *testing.T) {
	extremesT := []env.Time{env.MinTime, env.MinTime + 1, -1, 0, 1, env.MaxTime - 1, env.MaxTime}
	extremesD := []time.Duration{math.MinInt64, math.MinInt64 + 1, -1, 0, 1, math.MaxInt64 - 1, math.MaxInt64}

	drawTime := func(t *rapid.T, label string) env.Time {
		if rapid.Bool().Draw(t, label+"_extreme") {
			return rapid.SampledFrom(extremesT).Draw(t, label)
		}
		return env.Time(rapid.Int64().Draw(t, label))
	}
	drawDur := func(t *rapid.T, label string) time.Duration {
		if rapid.Bool().Draw(t, label+"_extreme") {
			return rapid.SampledFrom(extremesD).Draw(t, label)
		}
		return time.Duration(rapid.Int64().Draw(t, label))
	}

	clampInt64 := func(x *big.Int) int64 {
		minB := big.NewInt(math.MinInt64)
		maxB := big.NewInt(math.MaxInt64)
		if x.Cmp(minB) < 0 {
			return math.MinInt64
		}
		if x.Cmp(maxB) > 0 {
			return math.MaxInt64
		}
		return x.Int64()
	}

	rapid.Check(t, func(t *rapid.T) {
		t1 := drawTime(t, "t1")
		t2 := drawTime(t, "t2")
		d1 := drawDur(t, "d1")
		d2 := drawDur(t, "d2")

		// Exact saturating Add vs big.Int specification.
		sumBig := new(big.Int).Add(big.NewInt(int64(t1)), big.NewInt(int64(d1)))
		wantAdd := env.Time(clampInt64(sumBig))
		gotAdd := t1.Add(d1)
		if gotAdd != wantAdd {
			t.Fatalf("Time(%d).Add(%d) = %d, want %d", t1, d1, gotAdd, wantAdd)
		}

		// Direction / monotonicity of Add.
		if d1 >= 0 && gotAdd < t1 {
			t.Fatalf("Time(%d).Add(%d) = %d wrapped downward", t1, d1, gotAdd)
		}
		if d1 <= 0 && gotAdd > t1 {
			t.Fatalf("Time(%d).Add(%d) = %d wrapped upward", t1, d1, gotAdd)
		}
		if t1 <= t2 && t1.Add(d1) > t2.Add(d1) {
			t.Fatalf("Add not monotonic in time: (%d).Add(%d) > (%d).Add(%d)", t1, d1, t2, d1)
		}
		if d1 <= d2 && t1.Add(d1) > t1.Add(d2) {
			t.Fatalf("Add not monotonic in duration: (%d).Add(%d) > (%d).Add(%d)", t1, d1, t1, d2)
		}

		// Exact saturating Sub vs big.Int specification.
		diffBig := new(big.Int).Sub(big.NewInt(int64(t1)), big.NewInt(int64(t2)))
		wantSub := time.Duration(clampInt64(diffBig))
		gotSub := t1.Sub(t2)
		if gotSub != wantSub {
			t.Fatalf("Time(%d).Sub(%d) = %d, want %d", t1, t2, gotSub, wantSub)
		}
		if t1 >= t2 && gotSub < 0 {
			t.Fatalf("Time(%d).Sub(%d) = %d wrapped negative", t1, t2, gotSub)
		}
		if t1 <= t2 && gotSub > 0 {
			t.Fatalf("Time(%d).Sub(%d) = %d wrapped positive", t1, t2, gotSub)
		}
		if t1.Sub(t1) != 0 {
			t.Fatalf("Time(%d).Sub(self) = %d, want 0", t1, t1.Sub(t1))
		}
	})
}

func TestPropEnvelopeCanonicalAndRoundTrip(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		e := env.Envelope{
			Instance: env.InstanceID(rapid.String().Draw(t, "instance")),
			MsgID:    rapid.String().Draw(t, "msgID"),
			From:     env.NodeID(rapid.String().Draw(t, "from")),
			Type:     rapid.String().Draw(t, "type"),
			Payload:  rapid.SliceOf(rapid.Byte()).Draw(t, "payload"),
			Auth:     rapid.SliceOf(rapid.Byte()).Draw(t, "auth"),
		}

		data, err := env.MarshalEnvelope(e)
		if err != nil {
			t.Fatalf("MarshalEnvelope failed: %v", err)
		}
		got, err := env.UnmarshalEnvelope(data)
		if err != nil {
			t.Fatalf("UnmarshalEnvelope failed: %v", err)
		}
		if got.Instance != e.Instance || got.MsgID != e.MsgID || got.From != e.From || got.Type != e.Type ||
			!bytes.Equal(got.Payload, e.Payload) || !bytes.Equal(got.Auth, e.Auth) {
			t.Fatalf("round-trip mismatch:\ngot:  %+v\nwant: %+v", got, e)
		}

		canon := e.CanonicalBytes()
		if !bytes.Equal(got.CanonicalBytes(), canon) {
			t.Fatalf("CanonicalBytes changed after JSON round-trip")
		}

		// CanonicalBytes ignores Auth.
		altAuth := rapid.SliceOf(rapid.Byte()).Draw(t, "altAuth")
		withAltAuth := e
		withAltAuth.Auth = altAuth
		if !bytes.Equal(withAltAuth.CanonicalBytes(), canon) {
			t.Fatalf("CanonicalBytes changed when only Auth changed")
		}

		// CanonicalBytes differs whenever any non-Auth field differs.
		altInst := env.InstanceID(rapid.String().Filter(func(s string) bool { return s != string(e.Instance) }).Draw(t, "altInst"))
		eInst := e
		eInst.Instance = altInst
		if bytes.Equal(eInst.CanonicalBytes(), canon) {
			t.Fatalf("CanonicalBytes collision on Instance: %q vs %q", e.Instance, altInst)
		}

		altMsgID := rapid.String().Filter(func(s string) bool { return s != e.MsgID }).Draw(t, "altMsgID")
		eMsgID := e
		eMsgID.MsgID = altMsgID
		if bytes.Equal(eMsgID.CanonicalBytes(), canon) {
			t.Fatalf("CanonicalBytes collision on MsgID: %q vs %q", e.MsgID, altMsgID)
		}

		altFrom := env.NodeID(rapid.String().Filter(func(s string) bool { return s != string(e.From) }).Draw(t, "altFrom"))
		eFrom := e
		eFrom.From = altFrom
		if bytes.Equal(eFrom.CanonicalBytes(), canon) {
			t.Fatalf("CanonicalBytes collision on From: %q vs %q", e.From, altFrom)
		}

		altType := rapid.String().Filter(func(s string) bool { return s != e.Type }).Draw(t, "altType")
		eType := e
		eType.Type = altType
		if bytes.Equal(eType.CanonicalBytes(), canon) {
			t.Fatalf("CanonicalBytes collision on Type: %q vs %q", e.Type, altType)
		}

		altPayload := rapid.SliceOf(rapid.Byte()).Filter(func(b []byte) bool { return !bytes.Equal(b, e.Payload) }).Draw(t, "altPayload")
		ePayload := e
		ePayload.Payload = altPayload
		if bytes.Equal(ePayload.CanonicalBytes(), canon) {
			t.Fatalf("CanonicalBytes collision on Payload")
		}
	})
}
