package riskybiscuits_test

import (
	"archive/zip"
	"context"
	"crypto/rand"
	"io"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/go-quicktest/qt"
	"github.com/google/uuid"
	"github.com/msackman/riskybiscuits"
)

type Handler struct {
	f func(conn *riskybiscuits.DynamicSourceHubConn)
}

func (h *Handler) SourceHasChanged(conn *riskybiscuits.DynamicSourceHubConn) {
	h.f(conn)
}

var _ riskybiscuits.DynamicSourceHandler = (*Handler)(nil)

type Provider struct {
	f func(ar *zip.Writer) error
}

func (p *Provider) CreateSourceSnapshot(ar *zip.Writer) error {
	return p.f(ar)
}

func TestXxx(t *testing.T) {
	ctx := context.Background()
	path := "/" + rand.Text()

	errs := make(chan error, 10)
	defer func() {
		if !t.Failed() {
			select {
			case err := <-errs:
				t.Fatal(err)
			default:
			}
		}
	}()

	handler := &Handler{}

	srcId, err := uuid.NewRandom()
	if err != nil {
		t.Fatal(err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			errs <- err
			return
		}

		_, err = riskybiscuits.NewDynamicSourceConn(conn, srcId, handler)
		if err != nil {
			errs <- err
			return
		}
	})
	go http.ListenAndServe("localhost:8888", mux)

	// wait for the server to get up and running
	time.Sleep(50 * time.Millisecond)

	conn, _, err := websocket.Dial(ctx, "ws://localhost:8888"+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	provider := &Provider{}

	srcProvider, err := riskybiscuits.NewDynamicSourceLSPConn(conn, provider)
	if err != nil {
		t.Fatal(err)
	}
	qt.Assert(t, qt.DeepEquals(srcProvider.SrcId(), srcId))

	{
		// check the client can signal the server
		pinged := int64(0)
		handler.f = func(conn *riskybiscuits.DynamicSourceHubConn) {
			atomic.StoreInt64(&pinged, 1)
		}
		err = srcProvider.SignalSourceHasChanged()
		if err != nil {
			t.Fatal(err)
		}
		for range 100 {
			time.Sleep(10 * time.Millisecond)
			if atomic.LoadInt64(&pinged) == 1 {
				break
			}
		}
		qt.Assert(t, qt.Equals(atomic.LoadInt64(&pinged), 1))
	}

	{
		// check the server can request the archive from the client
		archiveGotChan := make(chan *zip.Reader, 1)
		handler.f = func(conn *riskybiscuits.DynamicSourceHubConn) {
			ar, err := conn.FetchSource()
			if err != nil {
				errs <- err
				return
			}
			archiveGotChan <- ar
		}

		provider.f = func(ar *zip.Writer) error {
			w, err := ar.Create("hello")
			if err != nil {
				errs <- err
				return err
			}
			_, err = w.Write([]byte("world"))
			if err != nil {
				errs <- err
				return err
			}
			return nil
		}

		err = srcProvider.SignalSourceHasChanged()
		if err != nil {
			t.Fatal(err)
		}

		var archiveGot *zip.Reader
		for range 100 {
			time.Sleep(10 * time.Millisecond)
			select {
			case archiveGot = <-archiveGotChan:
			default:
			}
			if archiveGot != nil {
				break
			}
		}
		qt.Assert(t, qt.IsNotNil(archiveGot))
		file, err := archiveGot.Open("hello")
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(file)
		if err != nil {
			t.Fatal(err)
		}
		qt.Assert(t, qt.DeepEquals(data, []byte("world")))
	}
}
