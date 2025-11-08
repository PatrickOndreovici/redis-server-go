package handler

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

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

	return &protocol.BulkString{Data: generatedId}, nil
}

func generateNextId(key, newId string, streamStore *store.StreamStore) (string, error) {
	lastId, exists := streamStore.GetLastId(key)

	if newId == "*" {
		now := time.Now().UnixMilli()
		var nextSeq int

		if !exists {
			nextSeq = 0
		} else {
			prevParts := strings.Split(lastId, "-")
			prevTs, _ := strconv.ParseInt(prevParts[0], 10, 64)
			prevSeq, _ := strconv.Atoi(prevParts[1])

			if now < prevTs {
				now = prevTs
				nextSeq = prevSeq + 1
			} else if now == prevTs {
				nextSeq = prevSeq + 1
			} else {
				nextSeq = 0
			}
		}
		return fmt.Sprintf("%d-%d", now, nextSeq), nil
	}

	splittedIds := strings.Split(newId, "-")
	if len(splittedIds) != 2 {
		return "", errors.New("ERR wrong number of arguments for 'XADD'")
	}

	tsPart := splittedIds[0]
	seqPart := splittedIds[1]

	ts, err := strconv.ParseInt(tsPart, 10, 64)
	if err != nil {
		return "", errors.New("ERR wrong number of arguments for 'XADD'")
	}

	if seqPart == "*" {
		var nextSeq int

		if !exists {
			if ts == 0 {
				nextSeq = 1
			} else {
				nextSeq = 0
			}
		} else {
			prevParts := strings.Split(lastId, "-")
			prevTs, _ := strconv.ParseInt(prevParts[0], 10, 64)
			prevSeq, _ := strconv.Atoi(prevParts[1])

			if ts < prevTs {
				return "", errors.New("ERR The ID specified in XADD is equal or smaller than the target stream top item")
			}

			if ts == prevTs {
				nextSeq = prevSeq + 1
			} else {
				// Different timestamp
				if ts == 0 {
					nextSeq = 1
				} else {
					nextSeq = 0
				}
			}
		}

		return fmt.Sprintf("%d-%d", ts, nextSeq), nil
	}

	sequenceNumber, err := strconv.Atoi(seqPart)
	if err != nil {
		return "", errors.New("ERR wrong number of arguments for 'XADD'")
	}
	if (ts == 0 && sequenceNumber <= 0) || ts < 0 || sequenceNumber < 0 {
		return "", errors.New("ERR The ID specified in XADD must be greater than 0-0")
	}

	if !exists {
		return newId, nil
	}

	prevTs, _ := strconv.ParseInt(strings.Split(lastId, "-")[0], 10, 64)
	prevSeq, _ := strconv.Atoi(strings.Split(lastId, "-")[1])

	if ts < prevTs || (ts == prevTs && sequenceNumber <= prevSeq) {
		return "", errors.New("ERR The ID specified in XADD is equal or smaller than the target stream top item")
	}

	return newId, nil
}
