package handler

import (
	"strconv"
	"strings"

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

	generatedId, ok := generateNextId(streamKey, Id, streamStore)

	if !ok {
		return nil, &protocol.Error{Message: "ERR wrong number of arguments for 'XADD'"}
	}
	streamStore.Add(streamKey, &store.StreamEntry{
		Id:     generatedId,
		Keys:   keys,
		Values: values,
	})

	return &protocol.BulkString{Data: Id}, nil
}

func generateNextId(key, newId string, streamStore *store.StreamStore) (string, bool) {
	lastId, exits := streamStore.GetLastId(key)
	splittedIds := strings.Split(newId, "-")
	if len(splittedIds) != 2 {
		return "", false
	}
	ts, err := strconv.Atoi(splittedIds[0])
	if err != nil {
		return "", false
	}
	sequenceNumber, err := strconv.Atoi(splittedIds[1])
	if err != nil {
		return "", false
	}
	if !exits {
		if (ts == 0 && sequenceNumber <= 0) || ts < 0 || sequenceNumber < 0 {
			return "", false
		}
		return newId, true
	} else {
		prevTs, _ := strconv.Atoi(strings.Split(lastId, "-")[0])
		prevSequenceNumber, _ := strconv.Atoi(strings.Split(lastId, "-")[1])
		if ts < prevTs {
			return "", false
		}
		if ts == prevTs && sequenceNumber <= prevSequenceNumber {
			return "", false
		}
		return newId, true
	}
}
