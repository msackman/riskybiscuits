package riskybiscuits

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"github.com/coder/websocket"
	"github.com/google/uuid"
)

type DynamicSourceHandler interface {
	// This callback is invoked (in a fresh Go-routine) whenever the
	// LSP indicates to the Hub that there is a fresh source snapshot
	// that can be retrieved.
	SourceHasChanged(*DynamicSourceHubConn)
}

type DynamicSourceHubConn struct {
	conn                *websocket.Conn
	handler             DynamicSourceHandler
	lock                sync.Mutex
	requestCounter      uint64
	outstandingRequests map[uint64]chan<- []byte
	closed              chan struct{}
}

func NewDynamicSourceConn(conn *websocket.Conn, srcId uuid.UUID, handler DynamicSourceHandler) (*DynamicSourceHubConn, error) {
	conn.SetReadLimit(1 << 24) // 16MB
	server := &DynamicSourceHubConn{
		conn:                conn,
		handler:             handler,
		requestCounter:      1,
		outstandingRequests: make(map[uint64]chan<- []byte),
		closed:              make(chan struct{}),
	}
	ctx := context.Background()
	if err := conn.Write(ctx, websocket.MessageBinary, srcId[:]); err != nil {
		return nil, err
	}
	go server.loop()
	return server, nil
}

// Shutdown the connection. Idempotent.
func (server *DynamicSourceHubConn) Shutdown() error {
	server.lock.Lock()
	conn := server.conn
	server.conn = nil

	closed := server.closed
	server.closed = nil
	if closed != nil {
		close(closed)
	}
	server.lock.Unlock()

	if conn == nil {
		return nil
	}
	return conn.Close(websocket.StatusNormalClosure, "")
}

var ErrClosed = errors.New("Closed")

// Fetch the current source snapshot from the LSP. This is blocking,
// and can be called from any Go-routine.
func (server *DynamicSourceHubConn) FetchSource() (*zip.Reader, error) {
	result := make(chan []byte, 1)

	server.lock.Lock()
	conn := server.conn
	counter := server.requestCounter
	server.requestCounter++
	server.outstandingRequests[counter] = result
	server.lock.Unlock()

	if conn == nil {
		return nil, ErrClosed
	}

	data := make([]byte, 9)
	binary.BigEndian.PutUint64(data, counter)
	ctx := context.Background()
	err := conn.Write(ctx, websocket.MessageBinary, data)
	if err != nil {
		server.Shutdown()
		return nil, err
	}

	data, ok := <-result
	if !ok {
		return nil, ErrClosed
	}
	reader := bytes.NewReader(data)
	return zip.NewReader(reader, int64(len(data)))
}

func (server *DynamicSourceHubConn) loop() {
	server.lock.Lock()
	conn := server.conn
	closed := server.closed
	handler := server.handler
	server.lock.Unlock()

	if conn == nil || closed == nil {
		return
	}

	defer server.Shutdown()

	ctx := context.Background()
	for {
		msgType, data, err := conn.Read(ctx)
		if err != nil {
			fmt.Println(err)
			return
		} else if msgType != websocket.MessageBinary {
			fmt.Println(errors.New("Not a binary message"))
			return
		}

		if len(data) == 1 && data[0] == 0x0 {
			go handler.SourceHasChanged(server)

		} else if len(data) < 8 {
			fmt.Println(errors.New("Received bad data"))
			return

		} else {
			request := binary.BigEndian.Uint64(data[:8])
			server.lock.Lock()
			ch, found := server.outstandingRequests[request]
			if found {
				delete(server.outstandingRequests, request)
			}
			server.lock.Unlock()
			if found {
				ch <- data[8:]
			} else {
				fmt.Println("Warning: received a reply to an unknown request - ignoring it.")
			}
		}

		select {
		case <-closed:
			return
		default:
		}
	}
}
