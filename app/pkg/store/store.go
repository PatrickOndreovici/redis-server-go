package store

type Store struct {
	KV          *KVStore
	Lists       *ListsStore
	StreamStore *StreamStore
}
