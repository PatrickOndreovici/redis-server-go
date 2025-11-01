package lists

import (
	"sync"
)

type ListsStore struct {
	mutex sync.Mutex
	data  map[string][]string
}

func NewListsStore() *ListsStore {
	return &ListsStore{
		data: make(map[string][]string),
	}
}

func (ls *ListsStore) RPush(key string, values ...string) int {
	ls.mutex.Lock()
	defer ls.mutex.Unlock()
	ls.data[key] = append(ls.data[key], values...)
	return len(ls.data[key])
}

func (ls *ListsStore) LRange(key string, start, end int) []string {
	ls.mutex.Lock()
	defer ls.mutex.Unlock()

	list, exists := ls.data[key]
	if !exists {
		return []string{}
	}

	length := len(list)
	if length == 0 {
		return []string{}
	}

	if start < 0 {
		start = length + start
	}
	if end < 0 {
		end = length + end
	}

	if start < 0 {
		start = 0
	}
	if end >= length {
		end = length - 1
	}

	if start > end || start >= length {
		return []string{}
	}

	result := make([]string, end-start+1)
	copy(result, list[start:end+1])

	return result
}

func (ls *ListsStore) LPush(key string, values ...string) int {
	ls.mutex.Lock()
	defer ls.mutex.Unlock()
	newArr := make([]string, len(ls.data[key])+len(values))
	for i, value := range values {
		newArr[len(values)-i-1] = value
	}
	copy(newArr[len(values):], ls.data[key])
	ls.data[key] = newArr
	return len(ls.data[key])
}

func (ls *ListsStore) GetLength(key string) int {
	ls.mutex.Lock()
	defer ls.mutex.Unlock()
	length := len(ls.data[key])
	return length
}

func (ls *ListsStore) LPop(key string) []string {
	ls.mutex.Lock()
	defer ls.mutex.Unlock()
	ls.data[key] = ls.data[key[1:]]
	return ls.data[key]
}
