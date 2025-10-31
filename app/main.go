package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
)

type CommandFunc func(args []string) string

type Protocol struct {
	conn     net.Conn
	reader   *bufio.Reader
	commands map[string]CommandFunc
}

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
		// Read length line
		lenLine, err := p.reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		lenLine = strings.TrimSpace(lenLine)
		if lenLine[0] != '$' {
			return nil, fmt.Errorf("expected bulk string")
		}

		// Parse length
		var strLen int
		fmt.Sscanf(lenLine, "$%d", &strLen)

		// Read exactly strLen bytes
		dataLine, err := p.readBulkString(strLen)
		if err != nil {
			return nil, err
		}

		args = append(args, dataLine)
	}

	return args, nil
}

// Write a simple string response
func (p *Protocol) WriteSimpleString(s string) {
	p.conn.Write([]byte("+" + s + "\r\n"))
}

// Write an error response
func (p *Protocol) WriteError(s string) {
	p.conn.Write([]byte("-" + s + "\r\n"))
}

// Handle one client connection
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
		p.WriteSimpleString(response)
	}
}

func main() {

	commands := map[string]CommandFunc{
		"PING": func(args []string) string {
			if len(args) > 0 {
				return args[0] // PING <msg>
			}
			return "PONG"
		},
		"ECHO": func(args []string) string {
			if len(args) > 0 {
				return args[0] // ECHO <msg>
			}
			return ""
		},
	}

	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

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
