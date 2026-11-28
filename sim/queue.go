package sim

import (
	"fmt"

	"github.com/nickstrad/distributed-systems-algorithms/env"
)

// simEvent is a scheduled callback in the simulator's indexed min-heap.
type simEvent struct {
	at     env.Time
	seq    uint64
	hi     int // current index in eventQueue; -1 when not queued
	kind   string
	detail string
	fn     func()
}

// eventQueue is an indexed binary min-heap ordered by (at, seq).
// Storing each event's heap index enables O(log n) cancellation.
type eventQueue []*simEvent

func (q eventQueue) less(i, j int) bool {
	if q[i].at != q[j].at {
		return q[i].at < q[j].at
	}
	return q[i].seq < q[j].seq
}

func (q eventQueue) swap(i, j int) {
	q[i], q[j] = q[j], q[i]
	q[i].hi = i
	q[j].hi = j
}

// push schedules a detached event. Timers use the stored index for cancellation.
//
//	pre: e.hi == -1
//	post: (*q)[e.hi] == e
func (q *eventQueue) push(e *simEvent) {
	if e.hi != -1 {
		panic(fmt.Sprintf("sim: enqueue event %d with existing heap index %d", e.seq, e.hi))
	}
	e.hi = len(*q)
	*q = append(*q, e)
	q.siftUp(e.hi)
}

func (q *eventQueue) pop() *simEvent {
	old := *q
	n := len(old)
	if n == 0 {
		return nil
	}
	top := old[0]
	last := old[n-1]
	old[n-1] = nil
	*q = old[:n-1]
	top.hi = -1
	if n > 1 {
		(*q)[0] = last
		last.hi = 0
		q.siftDown(0)
	}
	return top
}

func (q *eventQueue) remove(e *simEvent) bool {
	i := e.hi
	if i < 0 || i >= len(*q) || (*q)[i] != e {
		return false
	}
	old := *q
	n := len(old) - 1
	if i == n {
		old[n] = nil
		*q = old[:n]
		e.hi = -1
		return true
	}
	last := old[n]
	old[n] = nil
	*q = old[:n]
	(*q)[i] = last
	last.hi = i
	e.hi = -1
	if !q.siftDown(i) {
		q.siftUp(i)
	}
	return true
}

func (q eventQueue) siftUp(i int) {
	for i > 0 {
		parent := (i - 1) / 2
		if !q.less(i, parent) {
			break
		}
		q.swap(i, parent)
		i = parent
	}
}

func (q eventQueue) siftDown(i int) bool {
	start := i
	n := len(q)
	for {
		left := 2*i + 1
		if left >= n {
			break
		}
		smallest := left
		if right := left + 1; right < n && q.less(right, left) {
			smallest = right
		}
		if !q.less(smallest, i) {
			break
		}
		q.swap(i, smallest)
		i = smallest
	}
	return i > start
}
