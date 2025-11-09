package store

import (
	"sort"
	"strconv"
	"strings"
	"sync"
)

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

func (s *StreamStore) XRange(streamKey, start, end string) ([]*StreamEntry, bool) {
	s.rwm.RLock()
	defer s.rwm.RUnlock()

	arr := s.Data[streamKey]
	if len(arr) == 0 {
		return []*StreamEntry{}, true // Empty stream is valid, not an error
	}

	// Find first entry >= start
	startIdx := sort.Search(len(arr), func(i int) bool {
		return compareIds(arr[i].Id, start) >= 0
	})

	// Find first entry > end
	endIdx := sort.Search(len(arr), func(i int) bool {
		return compareIds(arr[i].Id, end) > 0
	})

	// Adjust endIdx to be inclusive (last valid index)
	if endIdx == 0 {
		// All elements are > end, so nothing matches
		return []*StreamEntry{}, true
	}
	endIdx-- // Now endIdx points to last element <= end

	// Check if range is valid
	if startIdx > endIdx || startIdx >= len(arr) {
		return []*StreamEntry{}, true // No elements in range
	}

	// Build result (inclusive range)
	rangeSize := endIdx - startIdx + 1
	res := make([]*StreamEntry, rangeSize)
	for i := 0; i < rangeSize; i++ {
		entry := *arr[startIdx+i]
		res[i] = &entry
	}

	return res, true
}

func compareIds(a, b string) int {
	aParts := strings.Split(a, "-")
	bParts := strings.Split(b, "-")

	aTs, _ := strconv.ParseInt(aParts[0], 10, 64)
	bTs, _ := strconv.ParseInt(bParts[0], 10, 64)

	if aTs < bTs {
		return -1
	} else if aTs > bTs {
		return 1
	}

	aSeq, _ := strconv.Atoi(aParts[1])
	bSeq, _ := strconv.Atoi(bParts[1])

	if aSeq < bSeq {
		return -1
	} else if aSeq > bSeq {
		return 1
	}
	return 0
}
