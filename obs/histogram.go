package obs

import "math/bits"

const (
	exactBuckets = 256
	subBuckets   = 128 // 1/128 (<0.79%) relative width per power-of-two octave
)

// Bucket is one non-empty histogram range [Lo, Hi] (inclusive) with its sample count.
type Bucket struct {
	Lo    int64
	Hi    int64
	Count int64
}

// Histogram is a fixed-size, integer-only log-linear histogram with zero
// heap allocations on Record. Values 0..255 receive exact single-integer
// buckets; higher values divide each power-of-two octave into 128 equal-width
// sub-buckets (<0.79% relative error).
type Histogram struct {
	lo          int64
	hi          int64
	minIdx      int
	buckets     []int64
	count       int64
	clampedLow  int64
	clampedHigh int64
}

// NewHistogram creates a Histogram covering integer values in [lo, hi] (with 0 <= lo <= hi).
func NewHistogram(lo, hi int64) *Histogram {
	if lo < 0 {
		lo = 0
	}
	if hi < lo {
		hi = lo
	}
	minIdx := bucketIndex(lo)
	maxIdx := bucketIndex(hi)
	return &Histogram{
		lo:      lo,
		hi:      hi,
		minIdx:  minIdx,
		buckets: make([]int64, maxIdx-minIdx+1),
	}
}

// NewDurationHistogram creates a Histogram covering 1 µs through 60 s in nanoseconds.
func NewDurationHistogram() *Histogram {
	return NewHistogram(1_000, 60_000_000_000)
}

// NewCountHistogram creates a Histogram covering integer counts 0 through 1,000,000.
func NewCountHistogram() *Histogram {
	return NewHistogram(0, 1_000_000)
}

// Record records v into the histogram with zero heap allocations.
// Out-of-range values clamp into the edge bucket and increment ClampedLow/ClampedHigh.
func (h *Histogram) Record(v int64) {
	if h == nil || len(h.buckets) == 0 {
		return
	}
	if v < h.lo {
		h.clampedLow++
		v = h.lo
	} else if v > h.hi {
		h.clampedHigh++
		v = h.hi
	}
	idx := bucketIndex(v) - h.minIdx
	if idx < 0 {
		idx = 0
	} else if idx >= len(h.buckets) {
		idx = len(h.buckets) - 1
	}
	h.buckets[idx]++
	h.count++
}

// Count returns the total number of recorded samples.
func (h *Histogram) Count() int64 { return h.count }

// ClampedLow returns the number of samples below lo.
func (h *Histogram) ClampedLow() int64 { return h.clampedLow }

// ClampedHigh returns the number of samples above hi.
func (h *Histogram) ClampedHigh() int64 { return h.clampedHigh }

// Quantile returns the upper bound of the bucket containing basis-point quantile
// bp (0..10000, e.g. 5000 = p50, 9900 = p99), using 128-bit integer arithmetic.
// Returns 0 when the histogram is empty.
func (h *Histogram) Quantile(bp int64) int64 {
	if h == nil || h.count == 0 {
		return 0
	}
	if bp < 0 {
		bp = 0
	} else if bp > 10_000 {
		bp = 10_000
	}
	// rank = ceil(count * bp / 10000) in 128-bit integer arithmetic.
	hi, lo := bits.Mul64(uint64(h.count), uint64(bp))
	q, r := bits.Div64(hi, lo, 10_000)
	if r > 0 {
		q++
	}
	if q == 0 {
		q = 1
	}
	target := int64(q)
	var cum int64
	for i, c := range h.buckets {
		cum += c
		if cum >= target {
			return h.bucketHi(i + h.minIdx)
		}
	}
	return h.hi
}

// Buckets returns all non-empty buckets in ascending order.
func (h *Histogram) Buckets() []Bucket {
	if h == nil || h.count == 0 {
		return nil
	}
	out := make([]Bucket, 0, 16)
	for i, c := range h.buckets {
		if c == 0 {
			continue
		}
		idx := i + h.minIdx
		lo, hi := bucketBounds(idx)
		if lo < h.lo {
			lo = h.lo
		}
		if hi > h.hi {
			hi = h.hi
		}
		out = append(out, Bucket{Lo: lo, Hi: hi, Count: c})
	}
	return out
}

func (h *Histogram) bucketHi(idx int) int64 {
	_, hi := bucketBounds(idx)
	if hi > h.hi {
		return h.hi
	}
	return hi
}

func bucketIndex(v int64) int {
	if v < exactBuckets {
		if v < 0 {
			return 0
		}
		return int(v)
	}
	e := bits.Len64(uint64(v)) - 8
	m := v >> e
	return exactBuckets + (e-1)*subBuckets + int(m-subBuckets)
}

func bucketBounds(idx int) (lo, hi int64) {
	if idx < exactBuckets {
		return int64(idx), int64(idx)
	}
	off := idx - exactBuckets
	e := off/subBuckets + 1
	m := int64(off%subBuckets + subBuckets)
	lo = m << e
	hi = ((m + 1) << e) - 1
	return lo, hi
}
