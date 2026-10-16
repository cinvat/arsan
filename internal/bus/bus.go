// Package bus is the command bus between the api and the controller.
//
// # Why a bus at all
//
// The api validates and persists records; the controller decides where they
// run. If the api called the controller's libraries directly, placement
// decisions would end up inside an HTTP handler and neither half could be
// tested alone. The bus is the only sanctioned path between them, which is what
// makes "api never places" a structural property rather than a convention.
//
// In production these are separate executables, so the bus must span processes.
// NATS JetStream provides that, with acknowledged and persisted delivery.
//
// # Delivery
//
// At-least-once. JetStream persists a message before acknowledging it, and a
// consumer that dies mid-handler has its message redelivered. So every handler
// must be idempotent: a crash between "side effect done" and "Ack" runs the
// handler again, and the compare-and-swap transitions are what make the repeat
// a no-op rather than a second build.
package bus

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

// ErrClosed is returned by Publish and Subscribe after Close.
var ErrClosed = errors.New("bus: closed")

// Command is one unit of work handed from the api to the controller.
//
// It carries an identifier and the target it concerns, never a snapshot of the
// record. A retried command must act on current state, so re-reading is the
// handler's job and the payload stays minimal.
type Command struct {
	// ID is unique and stable across redelivery.
	ID string `json:"id"`
	// Topic is the command name, e.g. "instance.create".
	Topic string `json:"topic"`
	// Target scopes the command to one aggregate, e.g. an instance ID.
	Target string `json:"target"`
	// Payload is the command's own data, never a copy of the record.
	Payload []byte `json:"payload"`
	// EnqueuedAt is when the command was first accepted.
	EnqueuedAt time.Time `json:"enqueued_at"`
	// Attempt counts deliveries, starting at 1.
	Attempt int `json:"attempt"`
}

// Handler processes one command.
//
// Returning nil Acks. Returning an error leaves the message unacked, so JetStream
// redelivers it. A handler that returns an error for a permanent condition
// should park the work instead of failing forever.
type Handler func(ctx context.Context, cmd Command) error

// Bus is the command transport.
//
// Implementations must be safe for concurrent use.
type Bus interface {
	// Publish sends a command and returns only once the server has
	// persisted and acknowledged it. A caller may therefore report success
	// to its own client as soon as Publish returns.
	Publish(ctx context.Context, cmd Command) error

	// PublishAsync sends a command without waiting for the server to persist
	// it. It returns an error only if the command itself is unusable, not
	// when the queue is slow.
	//
	// Use it where the caller has a safe fallback for a lost command and
	// should not take on the queue's latency. Do not use it where the command
	// must not be lost.
	PublishAsync(ctx context.Context, cmd Command) error

	// Subscribe registers a handler for a topic. The returned cancel
	// function stops delivery; call it on shutdown.
	Subscribe(ctx context.Context, topic string, h Handler) (cancel func(), err error)

	// SubscribeAs is Subscribe with an explicit subscriber name.
	//
	// JetStream delivers a message to exactly one consumer per name, so two
	// subscribers sharing a name split the work. That is right for replicas of
	// the same role and wrong for two independent subscribers that each need
	// every message — the api and controller never overlap today because their
	// topic sets are disjoint, but a test observing the bus certainly does.
	SubscribeAs(ctx context.Context, topic, subscriber string, h Handler) (cancel func(), err error)

	// Request sends payload to subject and waits for one reply, bounded by ctx.
	//
	// This is core NATS, not JetStream: no persistence, no redelivery. It is
	// for questions, not commands — asking an agent what it can do, not
	// telling the controller what to do. If nobody answers in time the caller
	// gets an error, which for the probe simply means NOTREADY.
	Request(ctx context.Context, subject string, payload []byte) ([]byte, error)

	// Close releases the connection and waits for in-flight work.
	Close() error
}

// Topics carried on the bus.
//
// Commands are requests for the controller to act. They are not events: an
// event describes something that already happened.
const (
	TopicInstanceCreate  = "instance.create"
	TopicInstanceDelete  = "instance.delete"
	TopicInstanceStart   = "instance.start"
	TopicInstanceStop    = "instance.stop"
	TopicInstanceMigrate = "instance.migrate"
	// Storage commands converge one VM's volumes to its record: attach and
	// detach work live on a running guest, boot moves need it stopped.
	TopicInstanceAttachVolume = "instance.attach-volume"
	TopicInstanceDetachVolume = "instance.detach-volume"
	TopicInstanceSetBoot      = "instance.set-boot"
	// TopicNodeRegistered is published by the api after a node's bootstrap
	// is accepted, so the controller starts managing it.
	TopicNodeRegistered = "node.registered"
)

// Subject maps a topic onto the JetStream subject namespace.
func Subject(topic string) string { return "cmd." + topic }

// StreamName is the JetStream stream carrying every command.
const StreamName = "ARSAN_COMMANDS"

// ConsumerName builds the durable consumer name for a topic.
//
// The name is stable and NOT per-process, on purpose: a durable consumer that
// changed name on restart would lose its position and replay the entire stream.
// Replay is safe here because handlers are idempotent, but it is wasteful, so
// the name must survive a restart.
//
// A consequence worth knowing: two controllers sharing this name COMPETE for
// messages rather than each receiving a copy. For a replica group that is the
// behaviour you want, since the work is divided. It is wrong for two
// independent subscribers that both need every message, and those must not exist:
// api and controller use disjoint topic sets, so they never overlap.
func ConsumerName(topic string) string { return ConsumerNameAs(topic, "") }

// ConsumerNameAs is ConsumerName with an explicit subscriber suffix.
func ConsumerNameAs(topic, subscriber string) string {
	if subscriber == "" {
		return sanitize("arsan-" + topic)
	}
	return sanitize("arsan-" + topic + "-" + subscriber)
}

// sanitize folds a name into the alphanumerics and -_/= that NATS permits.
func sanitize(s string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			return r
		case r == '-' || r == '_' || r == '/' || r == '=':
			return r
		default:
			return '-'
		}
	}, s)
}

// Validate rejects a command that could not be handled anyway.
func (c Command) Validate() error {
	if c.ID == "" {
		return fmt.Errorf("bus: command needs an ID")
	}
	if c.Topic == "" {
		return fmt.Errorf("bus: command needs a topic")
	}
	return nil
}
