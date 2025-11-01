package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/lists"
)

var DB sync.Map = sync.Map{}
var Lists = make(map[string][]string)

type CommandFunc func(args []string) string

type Protocol struct {
	conn     net.Conn
	reader   *bufio.Reader
	commands map[string]CommandFunc
}

type StoreValue struct {
	value      string
	created_at int64
	expire_at  int64
}

// Reads exactly N bytes + discards trailing CRLF
func (p *Protocol) readBulkString(length int) (string, error) {
	buf := make([]byte, length)
	_, err := p.reader.Read(buf)
	if err != nil {
		return "", err
	}

	// Discard trailing \r\n
	_, err = p.reader.Discard(2)
	if err != nil {
		return "", err
	}

	return string(buf), nil
}

// RESP array parser
func (p *Protocol) ReadCommand() ([]string, error) {
	line, err := p.reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimSpace(line)

	if len(line) == 0 || line[0] != '*' {
		return nil, fmt.Errorf("expected array")
	}

	var numElements int
	fmt.Sscanf(line, "*%d", &numElements)
	args := make([]string, 0, numElements)

	for i := 0; i < numElements; i++ {
		lenLine, err := p.reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		lenLine = strings.TrimSpace(lenLine)
		if lenLine[0] != '$' {
			return nil, fmt.Errorf("expected bulk string")
		}

		var strLen int
		fmt.Sscanf(lenLine, "$%d", &strLen)
		dataLine, err := p.readBulkString(strLen)
		if err != nil {
			return nil, err
		}
		args = append(args, dataLine)
	}

	return args, nil
}

// RESP writers
func (p *Protocol) WriteSimpleString(s string) {
	p.conn.Write([]byte("+" + s + "\r\n"))
}

func (p *Protocol) WriteError(s string) {
	p.conn.Write([]byte("-" + s + "\r\n"))
}

func (p *Protocol) WriteBulkString(s string) {
	if s == "" {
		p.conn.Write([]byte("$-1\r\n")) // null bulk string
		return
	}
	p.conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)))
}

// Handle one client
func (p *Protocol) Handle() {
	defer p.conn.Close()

	for {
		args, err := p.ReadCommand()
		if err != nil {
			p.WriteError("ERR " + err.Error())
			return
		}

		cmd := strings.ToUpper(args[0])
		handler, ok := p.commands[cmd]
		if !ok {
			p.WriteError(fmt.Sprintf("ERR unknown command '%s'", cmd))
			continue
		}

		response := handler(args[1:])

		// Determine correct response type
		switch cmd {
		case "PING", "SET":
			p.WriteSimpleString(response)
		case "RPUSH":
			// Response is already in integer format ":<number>"
			p.conn.Write([]byte(response + "\r\n"))
		default:
			if strings.HasPrefix(response, "ERR") {
				p.WriteError(response)
			} else {
				p.WriteBulkString(response)
			}
		}
	}
}

func main() {
	listStore := lists.NewListsStore()
	commands := map[string]CommandFunc{
		"PING": func(args []string) string {
			if len(args) > 0 {
				return args[0]
			}
			return "PONG"
		},
		"ECHO": func(args []string) string {
			if len(args) > 0 {
				return args[0]
			}
			return ""
		},
		"SET": func(args []string) string {
			if len(args) < 2 {
				return "ERR wrong number of arguments for 'SET'"
			}
			key := args[0]
			value := StoreValue{
				value:      args[1],
				created_at: time.Now().UnixMilli(),
				expire_at:  -1,
			}

			if len(args) > 3 && strings.ToUpper(args[2]) == "PX" {
				millis, err := strconv.Atoi(args[3])
				if err == nil {
					value.expire_at = value.created_at + int64(millis)
				} else {
					return "ERR invalid PX value"
				}
			}

			DB.Store(key, value)
			return "OK"
		},
		"GET": func(args []string) string {
			if len(args) < 1 {
				return "ERR wrong number of arguments for 'GET'"
			}
			key := args[0]
			value, ok := DB.Load(key)
			if !ok {
				return "" // null bulk string
			}

			sv, ok := value.(StoreValue)
			if !ok {
				return "ERR internal type mismatch"
			}

			if sv.expire_at > 0 && time.Now().UnixMilli() > sv.expire_at {
				DB.Delete(key)
				return "" // expired → null bulk string
			}

			return sv.value
		},
		"RPUSH": func(args []string) string {
			if len(args) < 2 {
				return "ERR wrong number of arguments for 'RPUSH'"
			}
			key := args[0]
			value := args[1]

			newLen := listStore.RPush(key, value)

			return fmt.Sprintf(":%d", newLen)
		},
	}

	fmt.Println("Server running on port 6379")

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}

		proto := &Protocol{
			conn:     conn,
			reader:   bufio.NewReader(conn),
			commands: commands,
		}

		go proto.Handle()
	}
}
