package env

import (
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"slices"
)

// Hash returns the SHA-256 digest of data.
func Hash(data []byte) [32]byte {
	return sha256.Sum256(data)
}

// KeyRing holds a node's Ed25519 signing key, all peers' Ed25519 public keys,
// and pairwise symmetric HMAC-SHA256 keys. Ed25519 signing is deterministic,
// keeping authenticated and Byzantine simulations bit-for-bit replayable.
type KeyRing struct {
	Self    NodeID
	priv    ed25519.PrivateKey
	pubs    map[NodeID]ed25519.PublicKey
	macKeys map[NodeID][]byte
}

// Sign returns a deterministic Ed25519 signature over msg.
func (k *KeyRing) Sign(msg []byte) []byte {
	if k == nil || len(k.priv) != ed25519.PrivateKeySize {
		return nil
	}
	return ed25519.Sign(k.priv, msg)
}

// Verify checks whether sig is a valid Ed25519 signature by from over msg.
func (k *KeyRing) Verify(from NodeID, msg, sig []byte) bool {
	if k == nil || len(sig) != ed25519.SignatureSize {
		return false
	}
	pub, ok := k.pubs[from]
	if !ok {
		return false
	}
	return ed25519.Verify(pub, msg, sig)
}

// PublicKey returns a copy of id's Ed25519 public key if known.
func (k *KeyRing) PublicKey(id NodeID) (ed25519.PublicKey, bool) {
	if k == nil {
		return nil, false
	}
	pub, ok := k.pubs[id]
	if !ok {
		return nil, false
	}
	return slices.Clone(pub), true
}

// MAC computes an HMAC-SHA256 tag over msg using the shared key with peer.
func (k *KeyRing) MAC(peer NodeID, msg []byte) []byte {
	if k == nil {
		return nil
	}
	key, ok := k.macKeys[peer]
	if !ok {
		return nil
	}
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write(msg)
	return mac.Sum(nil)
}

// VerifyMAC checks whether tag is a valid HMAC-SHA256 over msg for peer.
func (k *KeyRing) VerifyMAC(peer NodeID, msg, tag []byte) bool {
	expected := k.MAC(peer, msg)
	if len(expected) == 0 {
		return false
	}
	return hmac.Equal(expected, tag)
}

// GenerateKeyRings deterministically generates Ed25519 keypairs and pairwise
// HMAC-SHA256 keys for peers using rng. Input order does not affect output.
func GenerateKeyRings(peers []NodeID, rng *rand.Rand) (map[NodeID]*KeyRing, error) {
	if rng == nil {
		return nil, fmt.Errorf("%w: nil rng", ErrInvalidArgument)
	}
	sorted := slices.Clone(peers)
	slices.Sort(sorted)
	sorted = slices.Compact(sorted)

	reader := &randReader{rng: rng}
	privs := make(map[NodeID]ed25519.PrivateKey, len(sorted))
	pubs := make(map[NodeID]ed25519.PublicKey, len(sorted))
	for _, id := range sorted {
		pub, priv, err := ed25519.GenerateKey(reader)
		if err != nil {
			return nil, err
		}
		privs[id] = priv
		pubs[id] = pub
	}

	rings := make(map[NodeID]*KeyRing, len(sorted))
	for _, id := range sorted {
		rings[id] = &KeyRing{
			Self:    id,
			priv:    privs[id],
			pubs:    make(map[NodeID]ed25519.PublicKey, len(sorted)),
			macKeys: make(map[NodeID][]byte, len(sorted)),
		}
		for _, peer := range sorted {
			rings[id].pubs[peer] = slices.Clone(pubs[peer])
		}
	}

	for i := 0; i < len(sorted); i++ {
		for j := i; j < len(sorted); j++ {
			key := make([]byte, 32)
			_, _ = reader.Read(key)
			a, b := sorted[i], sorted[j]
			rings[a].macKeys[b] = slices.Clone(key)
			if a != b {
				rings[b].macKeys[a] = slices.Clone(key)
			}
		}
	}
	return rings, nil
}

type randReader struct {
	rng *rand.Rand
}

func (r *randReader) Read(p []byte) (int, error) {
	n := len(p)
	for len(p) >= 8 {
		binary.LittleEndian.PutUint64(p[:8], r.rng.Uint64())
		p = p[8:]
	}
	if len(p) > 0 {
		var buf [8]byte
		binary.LittleEndian.PutUint64(buf[:], r.rng.Uint64())
		copy(p, buf[:])
	}
	return n, nil
}
