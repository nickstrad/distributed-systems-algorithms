// Story: Fuzz resilience of wire Envelope unmarshaling and canonical encoding.
//
// What these tests prove: arbitrary untrusted byte payloads never panic inside
// UnmarshalEnvelope or Envelope.CanonicalBytes, and valid envelopes round-trip
// with byte-identical canonical signing inputs.
// How they probe: native Go coverage-guided fuzzing (FuzzEnvelopeUnmarshal)
// seeded with valid, empty, and malformed JSON inputs.
// What they do not prove: does not verify cryptographic key security or network
// framing limits.
package env_test

import (
	"bytes"
	"testing"

	"github.com/nickstrad/distributed-systems-algorithms/env"
)

func FuzzEnvelopeUnmarshal(f *testing.F) {
	seeds := [][]byte{
		// Valid envelopes.
		[]byte(`{}`),
		[]byte(`{"instance":"inst-1","msg_id":"m1","from":"n1","type":"vote","payload":"aGVsbG8=","auth":"c2ln"}`),
		[]byte(`{"instance":"","msg_id":"","from":"","type":"","payload":null,"auth":null}`),
		[]byte(`{"from":"node-\u00e9","type":"ping","extra_field":123}`),
		[]byte(`{"payload":"","auth":""}`),
		// Malformed / edge inputs.
		[]byte(``),
		[]byte(`{`),
		[]byte(`[]`),
		[]byte(`null`),
		[]byte(`"string"`),
		[]byte(`{"payload":"not-valid-base64!"}`),
		[]byte(`{"from":12345}`),
		[]byte(`{"instance":true}`),
		[]byte("\x00\xff\xfe{"),
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		e, err := env.UnmarshalEnvelope(data)
		canon1 := e.CanonicalBytes()
		if err != nil {
			return
		}

		b, err := env.MarshalEnvelope(e)
		if err != nil {
			t.Fatalf("MarshalEnvelope failed on valid unmarshaled envelope %+v: %v", e, err)
		}

		e2, err := env.UnmarshalEnvelope(b)
		if err != nil {
			t.Fatalf("re-UnmarshalEnvelope failed: %v", err)
		}
		if e2.Instance != e.Instance || e2.MsgID != e.MsgID || e2.From != e.From || e2.Type != e.Type ||
			!bytes.Equal(e2.Payload, e.Payload) || !bytes.Equal(e2.Auth, e.Auth) {
			t.Fatalf("round-trip mismatch:\ngot:  %+v\nwant: %+v", e2, e)
		}
		if !bytes.Equal(e2.CanonicalBytes(), canon1) {
			t.Fatalf("CanonicalBytes mismatch after re-marshal")
		}
	})
}
