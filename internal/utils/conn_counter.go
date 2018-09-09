package utils

import (
	"fmt"
	"net"
	"os"
	"regexp"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var connsCount int64
var printer sync.Once

var mu sync.Mutex
var createdStacks = make(map[string]int)
var reg = regexp.MustCompile(`0[xX][0-9a-fA-F]+`)

type connCounting struct {
	net.Conn

	closed bool
}

// Socket leak debug net.Conn wrapper
func NewCountingConn(conn net.Conn) net.Conn {
	c := &connCounting{
		Conn:   conn,
		closed: false,
	}
	runtime.SetFinalizer(c, func(cc *connCounting) {
		if !cc.closed {
			atomic.AddInt64(&connsCount, -1)
			cc.closed = true
		}
	})

	atomic.AddInt64(&connsCount, 1)
	printer.Do(func() {
		go func() {
			t := time.NewTicker(time.Second)
			f, err := os.Create("sockets.ticker")
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to create sockets.ticker file: %v\n", err)
				return
			}
			for {
				<-t.C
				fmt.Fprintf(f, "Connections count: %v\n", atomic.LoadInt64(&connsCount))
				f.Sync()
			}
		}()
	})

	st := string(debug.Stack())
	st = st[strings.Index(st, "\n")+1:]
	st = reg.ReplaceAllString(st, "")

	mu.Lock()
	_, has := createdStacks[st]
	if !has {
		createdStacks[st] = 1
	} else {
		createdStacks[st]++
	}
	printStacks()
	mu.Unlock()

	return c
}

func (c *connCounting) Close() error {
	if !c.closed {
		atomic.AddInt64(&connsCount, -1)
		c.closed = true
	}
	return c.Conn.Close()
}

func printStacks() {
	f, _ := os.Create("sockets.created")
	for s, c := range createdStacks {
		fmt.Fprintf(f, "%v:\n%v\n\n", c, s)
	}
	f.Sync()
	f.Close()
}
