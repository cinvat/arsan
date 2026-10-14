package message

import (
	"sync"
	"time"
)

type Topic string

const (
	TopicInstanceCreated Topic = "instance.created"
	TopicInstancePlaced  Topic = "instance.placed"
	TopicInstanceState   Topic = "instance.state"
	TopicNodeDown        Topic = "node.down"
)

type Event struct {
	Topic   Topic
	At      time.Time
	Payload any
}

type subscriber struct {
	ch chan Event
}

type Bus struct {
	mu      sync.RWMutex
	subs    map[Topic][]*subscriber
	dropped int64
}

const DefaultQueueSize = 64

func New() *Bus {
	return &Bus{subs: make(map[Topic][]*subscriber)}
}

func (b *Bus) Subscribe(topic Topic, size int) (<-chan Event, func()) {
	if size <= 0 {
		size = DefaultQueueSize
	}
	sub := &subscriber{ch: make(chan Event, size)}
	b.mu.Lock()
	b.subs[topic] = append(b.subs[topic], sub)
	b.mu.Unlock()

	return sub.ch, func() {
		b.mu.Lock()
		list := b.subs[topic]
		for i, s := range list {
			if s == sub {
				b.subs[topic] = append(list[:i], list[i+1:]...)
				close(sub.ch)
				break
			}
		}
		b.mu.Unlock()
	}
}

func (b *Bus) Publish(ev Event) {
	if ev.At.IsZero() {
		ev.At = time.Now().UTC()
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	for _, sub := range b.subs[ev.Topic] {
		select {
		case sub.ch <- ev:
		default:
			b.dropped++
		}
	}
}

func (b *Bus) Dropped() int64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.dropped
}
