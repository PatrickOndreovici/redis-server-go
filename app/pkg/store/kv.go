package store

import (
	"sync"
	"time"
)

type Entry struct {
	Data      []byte
	CreatedAt time.Time
	ExpiresAt time.Time // Zero value means no expiration
}

type KVStore struct {
	mu   sync.RWMutex
	data map[string]Entry
}

func NewKVStore() *KVStore {
	return &KVStore{
		data: make(map[string]Entry),
	}
}

func (s *KVStore) Set(key string, value []byte, expiration time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry := Entry{
		Data:      value,
		CreatedAt: time.Now(),
	}

	if expiration > 0 {
		entry.ExpiresAt = time.Now().Add(expiration)
	}

	s.data[key] = entry
}

func (s *KVStore) Get(key string) ([]byte, bool) {
	s.mu.RLock()
	entry, exists := s.data[key]
	s.mu.RUnlock()
	if !exists {
		return nil, false
	}

	if !entry.ExpiresAt.IsZero() && time.Now().After(entry.ExpiresAt) {
		s.mu.Lock()
		entry, exists = s.data[key]
		// double check
		// TODO: Is this ok ?
		if exists && !entry.ExpiresAt.IsZero() && time.Now().After(entry.ExpiresAt) {
			delete(s.data, key)
		}
		s.mu.Unlock()
		return nil, false
	}

	return entry.Data, true
}
