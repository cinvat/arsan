package handler

import (
	"bytes"
	"encoding/json"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/cinvat/arsan/api/v1alpha1/object"
	"github.com/cinvat/arsan/internal/core"
)

// snapshotInterval is how often the watcher re-reads. The stream is
// level-triggered, not edge-triggered: every tick compares against the last
// sent bytes and only writes on change, so an idle cluster costs one store
// read per tick and zero bandwidth.
const snapshotInterval = 2 * time.Second

// WatchVirtualMachines streams the VM list the same way: one NDJSON snapshot
// per line, level-triggered, until the client goes away. The ?state= filter
// applies to the stream, so a client can watch PENDING drain to zero.
func (h *Handler) WatchVirtualMachines(c *gin.Context) {
	c.Header("Content-Type", "application/x-ndjson")
	c.Header("Cache-Control", "no-cache")
	c.Writer.WriteHeader(http.StatusOK)
	c.Writer.Flush()

	state := c.Query("state")
	t := time.NewTicker(snapshotInterval)
	defer t.Stop()

	var last []byte
	send := func() bool {
		insts, err := h.instances.List(c.Request.Context())
		if err != nil {
			return true
		}
		if state != "" {
			insts = filterByState(insts, state)
		}
		objs := make([]object.VirtualMachine, 0, len(insts))
		for _, i := range insts {
			objs = append(objs, object.NewVirtualMachine(i))
		}
		raw, err := json.Marshal(map[string]any{"virtualmachines": objs})
		if err != nil {
			return true
		}
		if bytes.Equal(raw, last) {
			return true
		}
		last = raw
		if _, err := c.Writer.Write(append(raw, '\n')); err != nil {
			return false
		}
		c.Writer.Flush()
		return true
	}

	if !send() {
		return
	}
	for {
		select {
		case <-c.Request.Context().Done():
			return
		case <-t.C:
			if !send() {
				return
			}
		}
	}
}

// WatchNodes streams the node list as newline-delimited JSON, one snapshot
// per line, until the client goes away:
//
//	{"nodes":[{...},{...}]}
//	{"nodes":[{...}]}
//
// One object per line keeps every event parseable the moment its newline
// lands — no framing protocol, no buffering surprises. It streams over
// HTTP/2 when the server speaks it (see cmd/api.go) and plain chunked
// HTTP/1 otherwise; the client must not care which.
func (h *Handler) WatchNodes(c *gin.Context) {
	c.Header("Content-Type", "application/x-ndjson")
	c.Header("Cache-Control", "no-cache")
	c.Writer.WriteHeader(http.StatusOK)
	c.Writer.Flush()

	t := time.NewTicker(snapshotInterval)
	defer t.Stop()

	var last []byte
	send := func() bool {
		nodes, err := h.nodes.List(c.Request.Context())
		if err != nil {
			return true
		}
		if nodes == nil {
			nodes = []core.Node{}
		}
		objs := make([]object.Node, 0, len(nodes))
		for _, n := range nodes {
			objs = append(objs, object.NewNode(n))
		}
		raw, err := json.Marshal(map[string]any{"nodes": objs})
		if err != nil {
			return true
		}
		if bytes.Equal(raw, last) {
			return true
		}
		last = raw
		if _, err := c.Writer.Write(append(raw, '\n')); err != nil {
			return false
		}
		c.Writer.Flush()
		return true
	}

	// The first snapshot goes out immediately, so `watch` never stares at a
	// blank screen while waiting to learn nothing changed.
	if !send() {
		return
	}
	for {
		select {
		case <-c.Request.Context().Done():
			return
		case <-t.C:
			if !send() {
				return
			}
		}
	}
}
