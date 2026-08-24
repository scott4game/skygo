package gate

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/scott4game/skygo/frame"
)

func TestServerRoundTrip(t *testing.T) {
	opened := make(chan *Conn, 1)
	server, err := New(Options{Address: "127.0.0.1:0"}, HandlerFuncs{
		Open: func(conn *Conn) { opened <- conn },
		Message: func(ctx context.Context, conn *Conn, payload []byte) {
			_ = conn.Send(ctx, append([]byte("reply:"), payload...))
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := server.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer server.Stop(context.Background())
	raw, err := net.Dial("tcp", server.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	select {
	case <-opened:
	case <-time.After(time.Second):
		t.Fatal("open callback timeout")
	}
	if err := frame.Write(raw, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	payload, err := frame.Read(raw)
	if err != nil {
		t.Fatal(err)
	}
	if string(payload) != "reply:hello" {
		t.Fatalf("payload=%q", payload)
	}
}
