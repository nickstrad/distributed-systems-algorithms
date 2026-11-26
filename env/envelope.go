package env

import (
	"encoding/binary"
	"encoding/json"
)

// Envelope is the generic wire container carried across point-to-point links.
// Instance scopes concurrent module instances, MsgID supports deduplication,
// and Auth holds an optional MAC tag or digital signature over CanonicalBytes.
type Envelope struct {
	Instance InstanceID `json:"instance,omitempty"`
	MsgID    string     `json:"msg_id,omitempty"`
	From     NodeID     `json:"from,omitempty"`
	Type     string     `json:"type,omitempty"`
	Payload  []byte     `json:"payload,omitempty"`
	Auth     []byte     `json:"auth,omitempty"`
}

// MarshalEnvelope serializes e to JSON bytes.
func MarshalEnvelope(e Envelope) ([]byte, error) {
	return json.Marshal(e)
}

// UnmarshalEnvelope deserializes JSON bytes into an Envelope.
func UnmarshalEnvelope(data []byte) (Envelope, error) {
	var e Envelope
	err := json.Unmarshal(data, &e)
	return e, err
}

// CanonicalBytes returns a deterministic, unambiguous length-prefixed byte
// encoding of all fields except Auth, suitable for MAC and signature inputs.
func (e Envelope) CanonicalBytes() []byte {
	total := 4*5 + len(e.Instance) + len(e.MsgID) + len(e.From) + len(e.Type) + len(e.Payload)
	buf := make([]byte, 0, total)
	buf = appendLenString(buf, string(e.Instance))
	buf = appendLenString(buf, e.MsgID)
	buf = appendLenString(buf, string(e.From))
	buf = appendLenString(buf, e.Type)
	buf = appendLenBytes(buf, e.Payload)
	return buf
}

func appendLenString(dst []byte, s string) []byte {
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(s)))
	return append(dst, s...)
}

func appendLenBytes(dst, b []byte) []byte {
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(b)))
	return append(dst, b...)
}
