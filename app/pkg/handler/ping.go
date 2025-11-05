package handler

import "github.com/codecrafters-io/redis-starter-go/app/pkg/protocol"

func Ping() (protocol.RespValue, *protocol.Error) {
	return &protocol.SimpleString{Data: "PONG"}, nil
}
