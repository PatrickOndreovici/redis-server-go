package handler

import (
	"github.com/codecrafters-io/redis-starter-go/app/pkg/protocol"
	"github.com/codecrafters-io/redis-starter-go/app/pkg/store"
)

func XAdd(args []string, streamStore *store.StreamStore) (protocol.RespValue, *protocol.Error) {
	if len(args) < 4 || len(args)%2 == 1 {
		return nil, &protocol.Error{Message: "ERR wrong number of arguments for 'XADD'"}
	}
	streamKey := args[0]
	Id := args[1]
	keys := make([]string, len(args)/2-1)
	values := make([]string, len(args)/2-1)
	for i := 2; i < len(args); i += 2 {
		keys[i-2] = args[i]
		values[i-2] = args[i+1]
	}

	streamStore.Add(streamKey, &store.StreamEntry{
		Id:     Id,
		Keys:   keys,
		Values: values,
	})

	return &protocol.BulkString{Data: Id}, nil
}
