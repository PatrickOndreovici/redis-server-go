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

func XRange(args []string, streamStore *store.StreamStore) (protocol.RespValue, *protocol.Error) {
	if len(args) < 3 {
		return nil, &protocol.Error{Message: "ERR wrong number of arguments for 'XRANGE'"}
	}
	streamKey := args[0]
	start := args[1]
	end := args[2]

	if !strings.Contains(start, "-") {
		start += "-0"
	}
	if !strings.Contains(end, "-") {
		end += "-18446744073709551615"
	}

	results, ok := streamStore.XRange(streamKey, start, end)
	if !ok {
		return nil, &protocol.Error{Message: "ERR XRange failed"}
	}

	outerArray := make([]protocol.RespValue, len(results))

	for i, entry := range results {
		kvArray := make([]protocol.RespValue, len(entry.Keys)+len(entry.Values))
		idx := 0
		for j := 0; j < len(entry.Keys); j++ {
			kvArray[idx] = &protocol.BulkString{Data: entry.Keys[j]}
			idx++
			kvArray[idx] = &protocol.BulkString{Data: entry.Values[j]}
			idx++
		}

		entryArray := &protocol.Array{
			Elements: []protocol.RespValue{
				&protocol.BulkString{Data: entry.Id},
				&protocol.Array{Elements: kvArray},
			},
		}

		outerArray[i] = entryArray
	}

	return &protocol.Array{Elements: outerArray}, nil
}

func XReadStreams(args []string, streamStore *store.StreamStore) (protocol.RespValue, *protocol.Error) {
	if len(args) < 2 || len(args)%2 == 1 {
		return nil, &protocol.Error{Message: "ERR wrong number of arguments for 'XREAD'"}
	}

	keys := make([]string, len(args)/2)
	ids := make([]string, len(args)/2)

	for i := 0; i < len(args)/2; i++ {
		keys[i] = args[i]
		ids[i] = args[(len(args)/2)+i]
	}

	results := streamStore.XReadStreams(keys, ids)
	respResponse := &protocol.Array{}
	respResponse.Elements = make([]protocol.RespValue, len(results))
	for i := 0; i < len(results); i++ {
		respResponse.Elements[i] = mapStreamToRespArray(args[i], results[i])
	}

	return respResponse, nil
}

func mapStreamToRespArray(streamKey string, stream []*store.StreamEntry) *protocol.Array {
	streamArray := make([]protocol.RespValue, len(stream))

	for i, entry := range stream {
		// Build inner key-value array: ["temperature", "95"]
		kvArray := make([]protocol.RespValue, len(entry.Keys)*2)
		for j := 0; j < len(entry.Keys); j++ {
			kvArray[j*2] = &protocol.BulkString{Data: entry.Keys[j]}
			kvArray[j*2+1] = &protocol.BulkString{Data: entry.Values[j]}
		}

		// Each entry = ["0-1", ["temperature", "95"]]
		entryArray := &protocol.Array{
			Elements: []protocol.RespValue{
				&protocol.BulkString{Data: entry.Id},
				&protocol.Array{Elements: kvArray},
			},
		}
		streamArray[i] = entryArray
	}

	// Whole stream: ["stream_key", [ ["0-1", ["temperature", "95"]] ]]
	return &protocol.Array{
		Elements: []protocol.RespValue{
			&protocol.BulkString{Data: streamKey},
			&protocol.Array{Elements: streamArray},
		},
	}
}
