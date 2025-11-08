package handler

import (
	"errors"
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

	generatedId, err := generateNextId(streamKey, Id, streamStore)

	if err != nil {
		return nil, &protocol.Error{Message: err.Error()}
	}
	streamStore.Add(streamKey, &store.StreamEntry{
		Id:     generatedId,
		Keys:   keys,
		Values: values,
	})

	return &protocol.BulkString{Data: Id}, nil
}

func generateNextId(key, newId string, streamStore *store.StreamStore) (string, error) {
	lastId, exits := streamStore.GetLastId(key)
	splittedIds := strings.Split(newId, "-")
	if len(splittedIds) != 2 {
		return "", errors.New("ERR wrong number of arguments for 'XADD'")
	}
	ts, err := strconv.Atoi(splittedIds[0])
	if err != nil {
		return "", errors.New("ERR wrong number of arguments for 'XADD'")
	}
	sequenceNumber, err := strconv.Atoi(splittedIds[1])
	if err != nil {
		return "", errors.New("ERR wrong number of arguments for 'XADD'")
	}
	if (ts == 0 && sequenceNumber <= 0) || ts < 0 || sequenceNumber < 0 {
		return "", errors.New("ERR The ID specified in XADD must be greater than 0-0")
	}
	if !exits {
		return newId, nil
	} else {
		prevTs, _ := strconv.Atoi(strings.Split(lastId, "-")[0])
		prevSequenceNumber, _ := strconv.Atoi(strings.Split(lastId, "-")[1])
		if ts < prevTs {
			return "", errors.New("ERR The ID specified in XADD is equal or smaller than the target stream top item")
		}
		if ts == prevTs && sequenceNumber <= prevSequenceNumber {
			return "", errors.New("ERR The ID specified in XADD is equal or smaller than the target stream top item")
		}
		return newId, nil
	}
}
