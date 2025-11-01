package lists

import "sync"

type ListsStore struct {
	mutex sync.Mutex
	data  map[string][]string
}

func NewListsStore() *ListsStore {
	return &ListsStore{
		data: make(map[string][]string),
	}
}

func (ls *ListsStore) RPush(key, value string) int {
	ls.mutex.Lock()
	defer ls.mutex.Unlock()
	ls.data[key] = append(ls.data[key], value)
	return len(ls.data[key])
}
