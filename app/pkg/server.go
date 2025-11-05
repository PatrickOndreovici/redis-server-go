package pkg

import (
	"fmt"
	"net"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/pkg/handler"
	"github.com/codecrafters-io/redis-starter-go/app/pkg/protocol"
	"github.com/codecrafters-io/redis-starter-go/app/pkg/store"
)

type Server struct {
	Addr  string
	Store *store.Store
}

func NewServer(addr string, store *store.Store) *Server {
	return &Server{Addr: addr, Store: store}
}

func (s *Server) ListenAndServe() error {
	ln, err := net.Listen("tcp", s.Addr)
	if err != nil {
		return err
	}
	defer ln.Close()
	fmt.Println("Server running on port 6379")
	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}
		respProtocol := protocol.NewRespProtocol(conn)
		go s.handleConnection(respProtocol)
	}
	return nil
}

func (s *Server) handleConnection(rp *protocol.RespProtocol) {
	defer rp.Conn.Close()

	for {
		input, err := rp.Read()
		if err != nil {
			fmt.Println("Error reading input:", err)
			return
		}
		if len(input) == 0 {
			continue
		}

		cmd := strings.ToUpper(input[0])
		args := input[1:]

		var resp protocol.RespValue
		var respErr *protocol.Error

		switch cmd {
		case "PING":
			resp = &protocol.SimpleString{Data: "PONG"}

		case "ECHO":
			if len(args) != 1 {
				respErr = &protocol.Error{Message: "ERR wrong number of arguments for 'ECHO'"}
			} else {
				resp = &protocol.BulkString{Data: args[0]}
			}

		case "SET":
			resp, respErr = handler.Set(args, s.Store.KV)

		case "GET":
			resp, respErr = handler.Get(args, s.Store.KV)

		case "LPUSH":
			resp, respErr = handler.LPush(args, s.Store.Lists)

		case "RPUSH":
			resp, respErr = handler.RPush(args, s.Store.Lists)

		case "LPOP":
			resp, respErr = handler.LPop(args, s.Store.Lists)

		case "LLEN":
			resp, respErr = handler.LLen(args, s.Store.Lists)

		case "LRANGE":
			resp, respErr = handler.LRange(args, s.Store.Lists)

		case "BLPOP":
			resp, respErr = handler.BLPop(args, s.Store.Lists)

		case "TYPE":
			if len(args) <= 1 {
				respErr = &protocol.Error{Message: "ERR wrong number of arguments for 'TYPE'"}
			}
			_, ok := s.Store.KV.Get(args[1])
			if ok {
				resp = &protocol.SimpleString{Data: "string"}
			} else {
				resp = &protocol.SimpleString{Data: "none"}
			}

		default:
			respErr = &protocol.Error{Message: fmt.Sprintf("ERR unknown command '%s'", cmd)}
		}

		if respErr != nil {
			_ = rp.Write(respErr)
		} else if resp != nil {
			_ = rp.Write(resp)
		}
	}
}
