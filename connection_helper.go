package gorethink

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"

	p "github.com/dancannon/gorethink/ql2"
)

// Write 'data' to conn
func (c *Connection) writeData(data []byte) error {
	_, err := c.conn.Write(data[:])
	if err != nil {
		return RqlConnectionError{err.Error()}
	}

	return nil
}

func (c *Connection) writeHandshakeReq() error {
	pos := 0
	dataLen := 4 + 4 + len(c.opts.AuthKey) + 4

	data := c.buf.takeSmallBuffer(dataLen)
	if data == nil {
		return RqlDriverError{ErrBusyBuffer.Error()}
	}

	// Send the protocol version to the server as a 4-byte little-endian-encoded integer
	binary.LittleEndian.PutUint32(data[pos:], uint32(p.VersionDummy_V0_3))
	pos += 4

	// Send the length of the auth key to the server as a 4-byte little-endian-encoded integer
	binary.LittleEndian.PutUint32(data[pos:], uint32(len(c.opts.AuthKey)))
	pos += 4

	// Send the auth key as an ASCII string
	if len(c.opts.AuthKey) > 0 {
		pos += copy(data[pos:], c.opts.AuthKey)
	}

	// Send the protocol type as a 4-byte little-endian-encoded integer
	binary.LittleEndian.PutUint32(data[pos:], uint32(p.VersionDummy_JSON))
	pos += 4

	return c.writeData(data)
}

func (c *Connection) readHandshakeSuccess() error {
	reader := bufio.NewReader(c.conn)
	line, err := reader.ReadBytes('\x00')
	if err != nil {
		if err == io.EOF {
			return fmt.Errorf("Unexpected EOF: %s", string(line))
		}
		return RqlConnectionError{err.Error()}
	}
	// convert to string and remove trailing NUL byte
	response := string(line[:len(line)-1])
	if response != "SUCCESS" {
		// we failed authorization or something else terrible happened
		return RqlDriverError{fmt.Sprintf("Server dropped connection with message: \"%s\"", response)}
	}

	return nil
}

func (c *Connection) writeQuery(token int64, q []byte) error {
	pos := 0
	dataLen := 8 + 4 + len(q)

	data := c.buf.takeSmallBuffer(dataLen)
	if data == nil {
		return RqlDriverError{ErrBusyBuffer.Error()}
	}

	// Send the protocol version to the server as a 4-byte little-endian-encoded integer
	binary.LittleEndian.PutUint64(data[pos:], uint64(token))
	pos += 8

	// Send the length of the auth key to the server as a 4-byte little-endian-encoded integer
	binary.LittleEndian.PutUint32(data[pos:], uint32(len(q)))
	pos += 4

	// Send the auth key as an ASCII string
	pos += copy(data[pos:], q)

	return c.writeData(data)
}
