package handler

import (
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/pkg/protocol"
	"github.com/codecrafters-io/redis-starter-go/app/pkg/store"
)

func Set(args []string, store *store.KVStore) (protocol.RespValue, *protocol.Error) {
	if len(args) < 2 {
		return nil, &protocol.Error{Message: "Missing key and value"}
	}
	key := args[0]
	value := args[1]

	var ttl time.Duration = 0

	if len(args) > 3 && strings.ToUpper(args[2]) == "PX" {
		millis, err := strconv.Atoi(args[3])
		if err != nil {
			return nil, &protocol.Error{Message: err.Error()}
		}
		ttl = time.Duration(millis) * time.Millisecond
	}
	store.Set(key, []byte(value), ttl)
	return &protocol.SimpleString{Data: "OK"}, nil
}

func Get(args []string, store *store.KVStore) (protocol.RespValue, *protocol.Error) {
	if len(args) == 0 {
		return nil, &protocol.Error{Message: "Missing key"}
	}
	key := args[0]
	value, ok := store.Get(key)
	if !ok {
		return &protocol.NullBulkString{}, nil
	}

	return &protocol.BulkString{Data: string(value)}, nil
}
