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
		return nil, false
	}
	startIdx := sort.Search(len(arr), func(i int) bool {
		return compareIds(arr[i].Id, start) >= 0
	})

	endIdx := sort.Search(len(arr), func(i int) bool {
		return compareIds(arr[i].Id, end) > 0
	}) - 1
	if startIdx == endIdx {
		return nil, false
	}
	res := make([]*StreamEntry, endIdx-startIdx+1)
	for i := range res {
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

func (s *StreamStore) XReadStreams(streamKeys, ids []string) [][]*StreamEntry {
	s.rwm.RLock()
	defer s.rwm.RUnlock()
	n := len(streamKeys)
	wg := sync.WaitGroup{}
	results := make([][]*StreamEntry, n)
	index := 0
	for i := 0; i < n; i++ {
		streamKey := streamKeys[i]
		id := ids[i]
		wg.Add(1)
		go s.XReadStream(streamKey, id, index, results, &wg)
		index++
	}
	wg.Wait()
	return results
}

func (s *StreamStore) XReadStream(streamKey, id string, index int, results [][]*StreamEntry, wg *sync.WaitGroup) {
	arr := s.Data[streamKey]
	if len(arr) == 0 {
		results[index] = nil
		return
	}
	idx := sort.Search(len(arr), func(i int) bool {
		return compareIds(arr[i].Id, id) > 0
	})
	results[index] = arr[idx:]
	wg.Done()
}
