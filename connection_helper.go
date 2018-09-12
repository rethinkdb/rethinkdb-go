package rethinkdb

import (
	"golang.org/x/net/context"
	"io"
)

// Write 'data' to conn
func (c *Connection) writeData(data []byte) error {
	_, err := c.Conn.Write(data[:])

	return err
}

func (c *Connection) read(buf []byte) (total int, err error) {
	return io.ReadFull(c.Conn, buf)
}

func (c *Connection) contextFromConnectionOpts() context.Context {
	sum := c.opts.ReadTimeout + c.opts.WriteTimeout
	if c.opts.ReadTimeout == 0 || c.opts.WriteTimeout == 0 {
		return context.Background()
	}
	ctx, _ := context.WithTimeout(context.Background(), sum)
	return ctx
}
