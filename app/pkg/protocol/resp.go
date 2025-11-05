package protocol

import (
	"bufio"
	"fmt"
	"net"
	"strings"
)

type RespValue interface {
	ToBytes() []byte
}

type SimpleString struct {
	Data string
}

func (s *SimpleString) ToBytes() []byte {
	return []byte(fmt.Sprintf("+%s\r\n", s.Data))
}

type BulkString struct {
	Data string
}

func (s *BulkString) ToBytes() []byte {
	return []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(s.Data), s.Data))
}

type Array struct {
	Data []string
}

func (a *Array) ToBytes() []byte {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("*%d\r\n", len(a.Data)))
	for _, v := range a.Data {
		sb.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(v), v))
	}
	return []byte(sb.String())
}

type Error struct {
	Message string
}

func (e *Error) ToBytes() []byte {
	return []byte(fmt.Sprintf("-%s\r\n", e.Message))
}

type NullBulkString struct {
}

func (n *NullBulkString) ToBytes() []byte {
	return []byte("$-1\r\n")
}

type IntegerBulkString struct {
	Data int64
}

func (i *IntegerBulkString) ToBytes() []byte {
	return []byte(fmt.Sprintf(":%d\r\n", i.Data))
}

type RespProtocol struct {
	Conn   net.Conn
	Reader *bufio.Reader
}

func NewRespProtocol(conn net.Conn) *RespProtocol {
	return &RespProtocol{
		Conn:   conn,
		Reader: bufio.NewReader(conn),
	}
}

func (rp *RespProtocol) Write(value RespValue) error {
	_, err := rp.Conn.Write(value.ToBytes())
	return err
}

// Reads exactly N bytes + discards trailing CRLF
func (rp *RespProtocol) readBulkString(length int) (string, error) {
	buf := make([]byte, length)
	_, err := rp.Reader.Read(buf)
	if err != nil {
		return "", err
	}

	// Discard trailing \r\n
	_, err = rp.Reader.Discard(2)
	if err != nil {
		return "", err
	}

	return string(buf), nil
}

func (rp *RespProtocol) Read() ([]string, error) {
	line, err := rp.Reader.ReadString('\n')
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
		lenLine, err := rp.Reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		lenLine = strings.TrimSpace(lenLine)
		if lenLine[0] != '$' {
			return nil, fmt.Errorf("expected bulk string")
		}

		var strLen int
		fmt.Sscanf(lenLine, "$%d", &strLen)
		dataLine, err := rp.readBulkString(strLen)
		if err != nil {
			return nil, err
		}
		args = append(args, dataLine)
	}

	return args, nil
}
