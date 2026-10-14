package bus

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// NATS is a Bus backed by NATS JetStream.
//
// JetStream rather than core NATS is the requirement, not a preference: core
// NATS is fire-and-forget, which cannot survive a controller restart mid-build.
// JetStream persists every command and acknowledges only once it is on disk.
type NATS struct {
	js     jetstream.JetStream
	stream jetstream.Stream
	conn   *nats.Conn

	mu      sync.Mutex
	closed  bool
	stopped []func()
}

// ConnectNATS dials NATS and ensures the command stream exists.
func ConnectNATS(url string, timeout time.Duration) (*NATS, error) {
	if url == "" {
		return nil, fmt.Errorf("bus: no NATS URL configured")
	}
	opts := []nats.Option{nats.MaxReconnects(-1)}
	if timeout > 0 {
		opts = append(opts, nats.Timeout(timeout))
	}
	conn, err := nats.Connect(url, opts...)
	if err != nil {
		return nil, fmt.Errorf("bus: connect NATS: %w", err)
	}

	js, err := jetstream.New(conn)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("bus: jetstream: %w", err)
	}

	setupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := js.CreateOrUpdateStream(setupCtx, jetstream.StreamConfig{
		Name: StreamName,
		// Every command topic lives under cmd.*.
		Subjects: []string{"cmd.>"},
		// LimitsPolicy keeps a command after it is acked, so a consumer can
		// replay from the stream after a crash. This is the property that
		// makes the bus a queue rather than a socket.
		Retention: jetstream.LimitsPolicy,
		// One replica is enough for a single control plane and keeps the
		// deployment to one NATS process. Raise it when running more.
		Replicas: 1,
		// Bound the stream so an unconsumed backlog cannot fill the disk.
		MaxAge: 72 * time.Hour,
	})
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("bus: create stream %s: %w", StreamName, err)
	}

	return &NATS{js: js, stream: stream, conn: conn}, nil
}

// Publish sends a command and waits for the JetStream acknowledgement.
//
// The acknowledgement is what makes this transactional from the caller's point
// of view: when Publish returns nil, the command is persisted on the server. If
// the caller dies after that point, the command still runs.
func (n *NATS) Publish(ctx context.Context, cmd Command) error {
	if err := cmd.Validate(); err != nil {
		return err
	}
	if cmd.EnqueuedAt.IsZero() {
		cmd.EnqueuedAt = time.Now().UTC()
	}
	cmd.Attempt = 1

	if err := ctx.Err(); err != nil {
		return err
	}
	n.mu.Lock()
	closed := n.closed
	n.mu.Unlock()
	if closed {
		return ErrClosed
	}

	body, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("bus: encode command: %w", err)
	}

	// Bound the wait. On expiry this returns an error rather than silently
	// losing the command, which is what lets the caller report failure
	// honestly instead of returning 202 for work that was never queued.
	ackCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	if _, err := n.js.Publish(ackCtx, Subject(cmd.Topic), body); err != nil {
		return fmt.Errorf("bus: publish %s: %w", cmd.Topic, err)
	}
	return nil
}

// PublishAsync sends a command and returns without waiting for the ack.
func (n *NATS) PublishAsync(ctx context.Context, cmd Command) error {
	if err := cmd.Validate(); err != nil {
		return err
	}
	if cmd.EnqueuedAt.IsZero() {
		cmd.EnqueuedAt = time.Now().UTC()
	}
	cmd.Attempt = 1

	n.mu.Lock()
	closed := n.closed
	n.mu.Unlock()
	if closed {
		return ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	body, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("bus: encode command: %w", err)
	}
	if _, err := n.js.PublishAsync(Subject(cmd.Topic), body); err != nil {
		return fmt.Errorf("bus: publish %s: %w", cmd.Topic, err)
	}
	return nil
}

// Subscribe registers a durable consumer for a topic.
//
// The consumer is durable, so its position survives a controller restart and
// pending commands are not replayed from the beginning of the stream.
func (n *NATS) Subscribe(ctx context.Context, topic string, h Handler) (func(), error) {
	return n.SubscribeAs(ctx, topic, "", h)
}

// SubscribeAs is Subscribe with an explicit subscriber name. An empty name
// gives the shared default, which is what a single instance of a role wants.
func (n *NATS) SubscribeAs(ctx context.Context, topic, subscriber string, h Handler) (func(), error) {
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return nil, ErrClosed
	}
	n.mu.Unlock()

	consumer, err := n.stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Name: func() string {
			if subscriber == "" {
				return ConsumerName(topic)
			}
			return ConsumerNameAs(topic, subscriber)
		}(),
		FilterSubject: Subject(topic),
		// Explicit ack: a command is only retired when the handler says so.
		AckPolicy: jetstream.AckExplicitPolicy,
		// How long the server waits for an ack before redelivering. Must
		// exceed the slowest handler or work gets run twice concurrently.
		AckWait: 30 * time.Second,
		// After this many deliveries the message is marked delivered and
		// left visible in the stream, so a permanently failing command
		// stops consuming an attempt budget instead of spinning forever.
		MaxDeliver: 3,
		// Start from the beginning of the stream. Safe because commands are
		// idempotent and because the durable consumer remembers its
		// position across restarts.
		DeliverPolicy: jetstream.DeliverAllPolicy,
		// Bound in-flight work so a slow handler cannot drain the whole
		// backlog into memory.
		MaxAckPending: 64,
		Replicas:      1,
	})
	if err != nil {
		return nil, fmt.Errorf("bus: subscribe %s: %w", topic, err)
	}

	cc, err := consumer.Consume(
		func(msg jetstream.Msg) { handle(msg, h) },
		jetstream.PullMaxMessages(1),
	)
	if err != nil {
		return nil, fmt.Errorf("bus: consume %s: %w", topic, err)
	}

	stop := func() { cc.Stop() }

	n.mu.Lock()
	n.stopped = append(n.stopped, stop)
	n.mu.Unlock()

	return stop, nil
}

// handle runs one command and acks or naks it.
func handle(msg jetstream.Msg, h Handler) {
	var cmd Command
	if err := json.Unmarshal(msg.Data(), &cmd); err != nil {
		// Unparseable, so no handler can ever act on it. Ack to drop it
		// rather than let it block the consumer.
		_ = msg.Ack()
		return
	}
	if md, err := msg.Metadata(); err == nil && md.NumDelivered > 1 {
		cmd.Attempt = int(md.NumDelivered)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	if err := h(ctx, cmd); err != nil {
		// NAK asks for redelivery. Handlers are idempotent, so this is safe.
		_ = msg.Nak()
		return
	}
	_ = msg.Ack()
}

// Close drains subscriptions and closes the connection.
func (n *NATS) Close() error {
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return nil
	}
	n.closed = true
	stopped := n.stopped
	n.stopped = nil
	n.mu.Unlock()

	for _, stop := range stopped {
		stop()
	}
	n.conn.Close()
	return nil
}

// Ensure NATS satisfies the interface.
var _ Bus = (*NATS)(nil)
