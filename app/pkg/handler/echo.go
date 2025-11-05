package handler

import "github.com/codecrafters-io/redis-starter-go/app/pkg/protocol"

func Echo(args []string) (protocol.RespValue, *protocol.Error) {
	if len(args) == 0 {
		return nil, &protocol.Error{Message: "ERR wrong number of arguments for 'ECHO'"}
	}

	return &protocol.BulkString{Data: args[0]}, nil
}
