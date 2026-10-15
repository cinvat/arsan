package bus_test

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/cinvat/arsan/internal/bus"
)

// These exercise a real NATS server. They are skipped unless one is reachable,
// because a green test suite that silently proves nothing is worse than an
// honest skip.
//
//	go install github.com/nats-io/nats-server/v2@latest && nats-server -js -p 4222
//	NATS_URL=nats://127.0.0.1:4222 go test ./internal/bus/...
func natsURL(t *testing.T) string {
	t.Helper()
	url := os.Getenv("NATS_URL")
	if url == "" {
		t.Skip("set NATS_URL to run: nats-server -js -p 4222")
	}
	return url
}

func connect(t *testing.T) *bus.NATS {
	t.Helper()
	url := natsURL(t)
	b, err := bus.ConnectNATS(url, 5*time.Second)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { b.Close() })
	return b
}

func TestPublishDeliversToSubscriber(t *testing.T) {
	b := connect(t)
	ctx := context.Background()

	// Unique topic per run so a shared server does not cross-talk.
	topic := "test.deliver." + time.Now().Format("150405.000000000")

	got := make(chan bus.Command, 1)
	stop, err := b.Subscribe(ctx, topic, func(_ context.Context, c bus.Command) error {
		got <- c
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer stop()

	want := bus.Command{ID: "c1", Topic: topic, Target: "i-1", Payload: []byte(`{"x":1}`)}
	if err := b.Publish(ctx, want); err != nil {
		t.Fatalf("publish: %v", err)
	}

	select {
	case cmd := <-got:
		if cmd.ID != want.ID || cmd.Target != want.Target {
			t.Fatalf("got %+v, want %+v", cmd, want)
		}
		if string(cmd.Payload) != string(want.Payload) {
			t.Fatalf("payload = %q, want %q", cmd.Payload, want.Payload)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("command was never delivered")
	}
}

// The whole point of the bus: a command published before the controller exists
// must still be there when it starts.
func TestCommandSurvivesConsumerStartingLater(t *testing.T) {
	b := connect(t)
	ctx := context.Background()

	topic := "test.late." + time.Now().Format("150405.000000000")
	if err := b.Publish(ctx, bus.Command{ID: "early", Topic: topic}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	got := make(chan bus.Command, 1)
	stop, err := b.Subscribe(ctx, topic, func(_ context.Context, c bus.Command) error {
		select {
		case got <- c:
		default:
		}
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer stop()

	select {
	case cmd := <-got:
		if cmd.ID != "early" {
			t.Fatalf("got %s, want early", cmd.ID)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("a command published before any consumer was never delivered")
	}
}

// A handler that fails must be retried, and the attempt counter must climb.
// This is what makes "handlers are idempotent" a requirement rather than a hope.
func TestFailedHandlerIsRedelivered(t *testing.T) {
	b := connect(t)
	ctx := context.Background()

	topic := "test.retry." + time.Now().Format("150405.000000000")

	var mu sync.Mutex
	var attempts []int
	failUntil := 2

	stop, err := b.Subscribe(ctx, topic, func(_ context.Context, c bus.Command) error {
		mu.Lock()
		defer mu.Unlock()
		attempts = append(attempts, c.Attempt)
		if c.Attempt < failUntil {
			return errTransient
		}
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer stop()

	if err := b.Publish(ctx, bus.Command{ID: "retry", Topic: topic}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	deadline := time.After(30 * time.Second)
	for {
		mu.Lock()
		n := len(attempts)
		mu.Unlock()
		if n >= failUntil {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("handler ran %d times, want at least %d", n, failUntil)
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func TestPublishRejectsInvalidCommand(t *testing.T) {
	b := connect(t)
	ctx := context.Background()

	if err := b.Publish(ctx, bus.Command{Topic: "x"}); err == nil {
		t.Fatal("a command with no ID must be rejected")
	}
	if err := b.Publish(ctx, bus.Command{ID: "x"}); err == nil {
		t.Fatal("a command with no topic must be rejected")
	}
}

func TestPublishAfterCloseFails(t *testing.T) {
	b := connect(t)
	if err := b.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := b.Publish(context.Background(), bus.Command{ID: "c", Topic: "t"}); err == nil {
		t.Fatal("Publish after Close must fail rather than silently drop")
	}
}

type transientError struct{}

func (transientError) Error() string { return "transient" }

var errTransient = transientError{}

// A consumer must keep its position across a reconnect, or a controller
// restart replays the whole stream. Idempotent handlers make replay safe, but
// it is still waste, and the durable consumer exists to avoid it.
func TestConsumerResumesPositionAfterReconnect(t *testing.T) {
	url := natsURL(t)
	ctx := context.Background()
	topic := "test.resume." + time.Now().Format("150405.000000000")

	b, err := bus.ConnectNATS(url, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	var seen []string

	_, err = b.Subscribe(ctx, topic, func(_ context.Context, c bus.Command) error {
		mu.Lock()
		seen = append(seen, c.ID)
		mu.Unlock()
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	for _, id := range []string{"one", "two", "three"} {
		if err := b.Publish(ctx, bus.Command{ID: id, Topic: topic}); err != nil {
			t.Fatal(err)
		}
	}

	waitFor(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(seen) == 3
	}, "all three commands delivered")

	// Reconnect with a new connection. The consumer name is unchanged, so the
	// new connection resumes rather than starting over.
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	b2, err := bus.ConnectNATS(url, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer b2.Close()

	if err := b2.Publish(ctx, bus.Command{ID: "four", Topic: topic}); err != nil {
		t.Fatal(err)
	}
	stop2, err := b2.Subscribe(ctx, topic, func(_ context.Context, c bus.Command) error {
		mu.Lock()
		seen = append(seen, c.ID)
		mu.Unlock()
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer stop2()

	waitFor(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(seen) == 4
	}, "delivery continued after reconnect")

	mu.Lock()
	defer mu.Unlock()
	// The first three must not be redelivered to the second consumer.
	for _, id := range []string{"one", "two", "three"} {
		n := 0
		for _, s := range seen {
			if s == id {
				n++
			}
		}
		if n != 1 {
			t.Errorf("command %q delivered %d times after a reconnect, want 1", id, n)
		}
	}
}

func waitFor(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// Request gets one answer from whoever serves the subject, or a timeout when
// nobody does. Silence is the failure signal the probe reads as NOTREADY.
func TestRequestAnswers(t *testing.T) {
	b := connect(t)
	ctx := t.Context()

	stop, err := b.Answer(ctx, "test.echo", func(_ context.Context, raw []byte) []byte {
		return append([]byte("echo:"), raw...)
	})
	if err != nil {
		t.Fatal(err)
	}
	defer stop()

	reply, err := b.Request(ctx, "test.echo", []byte("hi"))
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	if string(reply) != "echo:hi" {
		t.Fatalf("reply = %q", reply)
	}
}

func TestRequestTimesOutWhenNobodyAnswers(t *testing.T) {
	b := connect(t)
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()

	if _, err := b.Request(ctx, "test.nobody-home", []byte("hi")); err == nil {
		t.Fatal("expected a timeout for an unserved subject")
	}
}
