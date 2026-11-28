// Story: A scheduled event retains a unique heap slot until removal.
//
// What these tests prove: duplicate insertion fails before the heap changes and a removed event can be scheduled again.
// How they probe: insert an event twice, catch the internal contract violation, then remove and reinsert the event.
// What they do not prove: ordering across arbitrary event schedules.
package sim

import "testing"

func TestHeapRejectsDuplicateEvent(t *testing.T) {
	var q eventQueue
	e := &simEvent{seq: 42, hi: -1}
	q.push(e)
	func() {
		defer func() {
			if recover() == nil {
				t.Error("duplicate push succeeded")
			}
		}()
		q.push(e)
	}()
	if len(q) != 1 || e.hi != 0 || q[e.hi] != e {
		t.Fatalf("duplicate push changed heap: len=%d index=%d", len(q), e.hi)
	}
	if !q.remove(e) || e.hi != -1 {
		t.Fatalf("removal left event attached: index=%d", e.hi)
	}
	q.push(e)
	if q.pop() != e || e.hi != -1 {
		t.Fatal("pop failed to detach reinserted event")
	}
}
