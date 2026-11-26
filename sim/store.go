package sim

import (
	"slices"
	"strings"

	"github.com/nickstrad/distributed-systems-algorithms/env"
)

// memStore implements durable per-node env.Store inside the simulator.
// State persists across Crash and Recover calls and clones slices on I/O.
type memStore struct {
	data map[string][]byte
}

func newMemStore() *memStore {
	return &memStore{data: make(map[string][]byte)}
}

func (m *memStore) Store(key string, val []byte) error {
	m.data[key] = slices.Clone(val)
	return nil
}

func (m *memStore) Retrieve(key string) ([]byte, bool, error) {
	v, ok := m.data[key]
	if !ok {
		return nil, false, nil
	}
	return slices.Clone(v), true, nil
}

func (m *memStore) Delete(key string) error {
	delete(m.data, key)
	return nil
}

func (m *memStore) Keys(prefix string) ([]string, error) {
	out := make([]string, 0, len(m.data))
	for k := range m.data {
		if strings.HasPrefix(k, prefix) {
			out = append(out, k)
		}
	}
	slices.Sort(out)
	return out, nil
}

var _ env.Store = (*memStore)(nil)
