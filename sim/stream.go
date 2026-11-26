package sim

import (
	"hash/fnv"
	"math/rand/v2"
)

// NewStream derives an isolated deterministic PCG stream from (seed, label).
// Separate labels ensure network draws never perturb node or workload draws.
func NewStream(seed int64, label string) *rand.Rand {
	h := fnv.New64a()
	_, _ = h.Write([]byte(label))
	return rand.New(rand.NewPCG(uint64(seed), h.Sum64()))
}
