package actor

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestTrySendReportsFullMailbox(t *testing.T) {
	system := NewSystem(SystemOptions{})
	service, ref, err := system.Reserve("try-send-full", ServiceOptions{MailboxSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	note := NewNotification[int]("note")
	release := make(chan struct{})
	started := make(chan struct{})
	if err := RegisterNotification(service, note, func(_ context.Context, value int) error {
		if value == 1 {
			close(started)
			<-release
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := service.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := note.Send(context.Background(), ref, 1); err != nil {
		t.Fatal(err)
	}
	<-started
	if err := note.TrySend(context.Background(), ref, 2); err != nil {
		t.Fatal(err)
	}
	if err := note.TrySend(context.Background(), ref, 3); !errors.Is(err, ErrMailboxFull) {
		t.Fatalf("TrySend error = %v, want ErrMailboxFull", err)
	}
	close(release)
	if err := service.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestStopTimeoutStillFinalizesAndUnpublishes(t *testing.T) {
	system := NewSystem(SystemOptions{})
	service, ref, err := system.Reserve("stop-finalizes", ServiceOptions{})
	if err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	started := make(chan struct{})
	note := NewNotification[struct{}]("block")
	if err := RegisterNotification(service, note, func(context.Context, struct{}) error {
		close(started)
		<-release
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := service.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := note.Send(context.Background(), ref, struct{}{}); err != nil {
		t.Fatal(err)
	}
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	err = service.Stop(ctx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Stop error = %v", err)
	}
	close(release)
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if _, err := system.Resolve("stop-finalizes"); errors.Is(err, ErrServiceNotFound) {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("service remained published after runtime completed")
}
