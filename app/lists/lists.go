package lists

import (
	"sync"
	"time"
)

type Waiter struct {
	key       string
	ch        chan string
	expire    time.Time
	createdAt time.Time
}

type ListsStore struct {
	mutex   sync.Mutex
	data    map[string][]string
	waiters map[string][]*Waiter
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
	ls.wakeOldestWaiter(key)
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
	ls.wakeOldestWaiter(key)
	return len(ls.data[key])
}

func (ls *ListsStore) GetLength(key string) int {
	ls.mutex.Lock()
	defer ls.mutex.Unlock()
	length := len(ls.data[key])
	return length
}

func (ls *ListsStore) LPop(key string, numberOfPops int) []string {
	ls.mutex.Lock()
	defer ls.mutex.Unlock()
	if len(ls.data[key]) == 0 {
		return []string{}
	}
	if numberOfPops <= 0 {
		return []string{}
	}
	if numberOfPops > len(ls.data[key]) {
		numberOfPops = len(ls.data[key])
	}
	var removedValue = ls.data[key][0:numberOfPops]
	ls.data[key] = ls.data[key][numberOfPops:]
	return removedValue
}

func (ls *ListsStore) BLPop(key string, timeout time.Duration) string {
	ls.mutex.Lock()

	// Fast path: list already has elements
	if len(ls.data[key]) > 0 {
		value := ls.data[key][0]
		ls.data[key] = ls.data[key][1:]
		ls.mutex.Unlock()
		return value
	}

	// Setup waiter
	waiter := &Waiter{
		key:       key,
		ch:        make(chan string, 1),
		createdAt: time.Now(),
		expire:    time.Now().Add(timeout),
	}
	if ls.waiters == nil {
		ls.waiters = make(map[string][]*Waiter)
	}
	ls.waiters[key] = append(ls.waiters[key], waiter)
	ls.mutex.Unlock()

	// Wait for push or timeout
	if timeout == 0 {
		// Block indefinitely
		return <-waiter.ch
	} else {
		select {
		case value := <-waiter.ch:
			return value
		case <-time.After(timeout):
			// Remove waiter on timeout
			ls.mutex.Lock()
			defer ls.mutex.Unlock()
			queue := ls.waiters[key]
			newQueue := make([]*Waiter, 0, len(queue))
			for _, w := range queue {
				if w != waiter {
					newQueue = append(newQueue, w)
				}
			}
			if len(newQueue) == 0 {
				delete(ls.waiters, key)
			} else {
				ls.waiters[key] = newQueue
			}
			return ""
		}
	}
}

func (ls *ListsStore) wakeOldestWaiter(key string) {
	waiters := ls.waiters[key]
	if len(waiters) == 0 {
		return
	}

	list := ls.data[key]
	if len(list) == 0 {
		return
	}

	waiter := waiters[0]
	ls.waiters[key] = waiters[1:]
	if len(ls.waiters[key]) == 0 {
		delete(ls.waiters, key)
	}

	value := list[0]
	ls.data[key] = list[1:]

	select {
	case waiter.ch <- value:
	default:
	}
}
