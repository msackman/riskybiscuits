package riskybiscuits

import (
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/coder/websocket"
	"github.com/google/uuid"
)

type DynamicSourceProvider interface {
	// This callback is invoked whenever a fresh source snapshot is
	// requested. It is invoked within the connection's read-loop and
	// so blocks processing of any further requests.
	CreateSourceSnapshot(*zip.Writer) error
}

type DynamicSourceLSPConn struct {
	conn     *websocket.Conn
	provider DynamicSourceProvider
	srcId    uuid.UUID
	lock     sync.Mutex
	closed   chan struct{}
}

func NewDynamicSourceLSPConn(conn *websocket.Conn, provider DynamicSourceProvider) (*DynamicSourceLSPConn, error) {
	msgType, data, err := conn.Read(context.Background())
	if err != nil {
		return nil, err
	} else if msgType != websocket.MessageBinary {
		return nil, errors.New("Not a binary message")
	} else if len(data) != len(uuid.UUID{}) {
		return nil, errors.New("First message was not a uuid")
	}
	var srcId uuid.UUID
	copy(srcId[:], data)

	client := &DynamicSourceLSPConn{
		conn:     conn,
		provider: provider,
		srcId:    srcId,
		closed:   make(chan struct{}),
	}

	go client.loop()
	return client, nil
}

func (client *DynamicSourceLSPConn) SrcId() uuid.UUID {
	return client.srcId
}

// Send a signal to the hub that a new source snapshot is
// available. This can be invoked from any Go-routine.
func (client *DynamicSourceLSPConn) SignalSourceHasChanged() error {
	client.lock.Lock()
	conn := client.conn
	client.lock.Unlock()

	if conn == nil {
		return ErrClosed
	}

	err := conn.Write(context.Background(), websocket.MessageBinary, []byte{0})
	if err != nil {
		client.Shutdown()
		return err
	}

	return nil
}

// Shutdown the connection. Idempotent.
func (client *DynamicSourceLSPConn) Shutdown() error {
	client.lock.Lock()
	conn := client.conn
	client.conn = nil

	closed := client.closed
	client.closed = nil
	if closed != nil {
		close(closed)
	}
	client.lock.Unlock()

	if conn == nil {
		return nil
	}
	return conn.Close(websocket.StatusNormalClosure, "")
}

// Blocks until the connection is closed. Returns immediately if connection is already closed.
func (client *DynamicSourceLSPConn) AwaitClosed() {
	client.lock.Lock()
	closed := client.closed
	client.lock.Unlock()

	if closed == nil {
		return
	}
	<-closed
}

func (client *DynamicSourceLSPConn) loop() {
	client.lock.Lock()
	conn := client.conn
	closed := client.closed
	provider := client.provider
	client.lock.Unlock()

	if conn == nil || closed == nil {
		return
	}

	defer client.Shutdown()

	ctx := context.Background()
	for {
		msgType, data, err := conn.Read(ctx)
		if err != nil {
			switch websocket.CloseStatus(err) {
			case websocket.StatusNormalClosure,
				websocket.StatusGoingAway:
			default:
				fmt.Printf("conn.Read: %v; status %v\n", err, websocket.CloseStatus(err))
			}
			return
		} else if msgType != websocket.MessageBinary {
			fmt.Println(errors.New("Not a binary message"))
			return
		}

		if len(data) != 9 || data[8] != 0x0 {
			fmt.Println(errors.New("Received bad data"))
			return
		}

		buf := new(bytes.Buffer)
		buf.Write(data[:8])
		ar := zip.NewWriter(buf)
		err = provider.CreateSourceSnapshot(ar)
		if err != nil {
			fmt.Println(err)
			return
		}
		err = ar.Close()
		if err != nil && err.Error() != "zip: writer closed twice" {
			fmt.Println(err)
			return
		}
		err = conn.Write(ctx, websocket.MessageBinary, buf.Bytes())
		if err != nil {
			fmt.Println(err)
			return
		}

		select {
		case <-closed:
			return
		default:
		}
	}
}
