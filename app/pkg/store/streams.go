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

func (s *StreamStore) GetLastId(key string) (string, bool) {
	s.rwm.RLock()
	defer s.rwm.RUnlock()
	if len(s.Data[key]) == 0 {
		return "", false
	}
	return s.Data[key][len(s.Data[key])-1].Id, true
}
