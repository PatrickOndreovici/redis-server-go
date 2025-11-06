package store

type KeyType int

const (
	None KeyType = iota
	String
	List
	Stream
)

var KeyTypeName = map[KeyType]string{
	None:   "none",
	String: "string",
	List:   "list",
	Stream: "stream",
}

type KeyTypeStore struct {
	KeyTypes map[string]KeyType
}

func NewKeyTypeStore() *KeyTypeStore {
	return &KeyTypeStore{
		KeyTypes: make(map[string]KeyType),
	}
}

func (s *KeyTypeStore) Register(key string, keyType KeyType) {
	s.KeyTypes[key] = keyType
}

func (s *KeyTypeStore) Get(key string) string {
	return KeyTypeName[s.KeyTypes[key]]
}
