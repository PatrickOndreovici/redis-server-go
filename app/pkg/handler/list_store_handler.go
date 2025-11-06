package handler

import (
	"strconv"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/pkg/protocol"
	"github.com/codecrafters-io/redis-starter-go/app/pkg/store"
)

func RPush(args []string, store *store.ListsStore) (protocol.RespValue, *protocol.Error) {
	if len(args) < 2 {
		return nil, &protocol.Error{Message: "ERR wrong number of arguments for 'RPUSH'"}
	}
	key := args[0]
	length := store.RPush(key, args[1:]...)
	return &protocol.IntegerBulkString{Data: int64(length)}, nil
}

func LRange(args []string, store *store.ListsStore) (protocol.RespValue, *protocol.Error) {
	if len(args) < 3 {
		return nil, &protocol.Error{Message: "ERR wrong number of arguments for 'LRANGE'"}
	}
	key := args[0]
	start, err := strconv.Atoi(args[1])
	if err != nil {
		return nil, &protocol.Error{Message: err.Error()}
	}
	end, err := strconv.Atoi(args[2])
	if err != nil {
		return nil, &protocol.Error{Message: err.Error()}
	}

	res := store.LRange(key, start, end)
	return &protocol.Array{res}, nil
}

func LPush(args []string, store *store.ListsStore) (protocol.RespValue, *protocol.Error) {
	if len(args) < 2 {
		return nil, &protocol.Error{Message: "ERR wrong number of arguments for 'LPUSH'"}
	}
	key := args[0]

	length := store.LPush(key, args[1:]...)

	return &protocol.IntegerBulkString{Data: int64(length)}, nil
}

func LLen(args []string, store *store.ListsStore) (protocol.RespValue, *protocol.Error) {
	if len(args) < 1 {
		return nil, &protocol.Error{Message: "ERR wrong number of arguments for 'LLEN'"}
	}
	key := args[0]
	length := store.GetLength(key)
	return &protocol.IntegerBulkString{Data: int64(length)}, nil
}

func LPop(args []string, store *store.ListsStore) (protocol.RespValue, *protocol.Error) {
	if len(args) < 1 {
		return nil, &protocol.Error{Message: "ERR wrong number of arguments for 'LPOP'"}
	}
	key := args[0]
	numberOfPos := 1
	if len(args) > 1 {
		n, err := strconv.Atoi(args[1])
		if err != nil {
			return nil, &protocol.Error{Message: err.Error()}
		}
		numberOfPos = n
	}
	res := store.LPop(key, numberOfPos)
	if len(res) == 1 {
		return &protocol.BulkString{Data: res[0]}, nil
	}
	return &protocol.Array{res}, nil
}

func BLPop(args []string, store *store.ListsStore) (protocol.RespValue, *protocol.Error) {
	if len(args) < 2 {
		return nil, &protocol.Error{Message: "ERR wrong number of arguments for 'BLPOP'"}
	}

	key := args[0]
	timeoutSec, err := strconv.ParseFloat(args[1], 64)
	if err != nil {
		return nil, &protocol.Error{Message: "ERR invalid timeout for 'BLPOP'"}
	}

	timeout := time.Duration(timeoutSec * float64(time.Second))

	value := store.BLPop(key, timeout)
	if value == "" {
		// Timed out — return null bulk string (as Redis does)
		return &protocol.Array{}, nil
	}

	// Redis returns an array of [key, value] when BLPOP succeeds
	return &protocol.Array{Data: []string{key, value}}, nil
}
