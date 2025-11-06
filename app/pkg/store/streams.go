package store

import "sync"

type StreamEntry struct {
	Id     string
	Keys   []string
	Values []string
}

type StreamStore struct {
	Data map[string][]*StreamEntry
	rwm  sync.RWMutex
}

func NewStreamStore() *StreamStore {
	return &StreamStore{Data: make(map[string][]*StreamEntry)}
}

func (s *StreamStore) Add(key string, entry *StreamEntry) {
	s.rwm.Lock()
	defer s.rwm.Unlock()
	s.Data[key] = append(s.Data[key], entry)
}
