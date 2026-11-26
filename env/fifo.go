package env

// FIFO is a swappable first-in, first-out queue abstraction used to back a
// process's local event dispatcher. Swapping implementations lets the same
// simulator or runtime use an unbounded slice queue (default for DST) or a
// channel-backed queue.
type FIFO[T any] interface {
	Push(v T)
	Pop() (T, bool)
	Len() int
	Clear()
}

// SliceFIFO is an unbounded slice-backed FIFO queue with amortized O(1) Push
// and Pop. Popped slots are zeroed so references are released for GC.
type SliceFIFO[T any] struct {
	items []T
	head  int
}

// NewSliceFIFO creates an empty SliceFIFO.
func NewSliceFIFO[T any]() *SliceFIFO[T] {
	return &SliceFIFO[T]{}
}

// Push appends v to the tail of the queue.
func (q *SliceFIFO[T]) Push(v T) {
	q.items = append(q.items, v)
}

// Pop removes and returns the head element, or the zero value and false if empty.
func (q *SliceFIFO[T]) Pop() (T, bool) {
	if q.head >= len(q.items) {
		var zero T
		return zero, false
	}
	v := q.items[q.head]
	var zero T
	q.items[q.head] = zero
	q.head++
	if q.head == len(q.items) {
		q.items = q.items[:0]
		q.head = 0
	} else if q.head >= 64 && q.head*2 >= len(q.items) {
		rem := len(q.items) - q.head
		copy(q.items, q.items[q.head:])
		clear(q.items[rem:])
		q.items = q.items[:rem]
		q.head = 0
	}
	return v, true
}

// Len returns the number of queued elements.
func (q *SliceFIFO[T]) Len() int {
	return len(q.items) - q.head
}

// Clear drops all queued elements and releases their references.
func (q *SliceFIFO[T]) Clear() {
	clear(q.items)
	q.items = q.items[:0]
	q.head = 0
}

// ChanFIFO is a bounded FIFO queue backed by a buffered Go channel.
// In single-threaded DST, Push panics if the channel is full rather than
// deadlocking the event loop.
type ChanFIFO[T any] struct {
	ch chan T
}

// NewChanFIFO creates a ChanFIFO with the given positive buffer capacity.
func NewChanFIFO[T any](capacity int) *ChanFIFO[T] {
	if capacity <= 0 {
		panic("env: ChanFIFO capacity must be positive")
	}
	return &ChanFIFO[T]{ch: make(chan T, capacity)}
}

// Push enqueues v onto the underlying buffered channel, panicking if full.
func (q *ChanFIFO[T]) Push(v T) {
	select {
	case q.ch <- v:
	default:
		panic("env: ChanFIFO capacity exceeded")
	}
}

// Pop dequeues the oldest element without blocking.
func (q *ChanFIFO[T]) Pop() (T, bool) {
	select {
	case v := <-q.ch:
		return v, true
	default:
		var zero T
		return zero, false
	}
}

// Len returns the number of elements currently buffered in the channel.
func (q *ChanFIFO[T]) Len() int {
	return len(q.ch)
}

// Clear drains all buffered elements from the channel.
func (q *ChanFIFO[T]) Clear() {
	for {
		select {
		case <-q.ch:
		default:
			return
		}
	}
}
