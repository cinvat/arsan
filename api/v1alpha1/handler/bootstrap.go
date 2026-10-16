package handler

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/cinvat/arsan/internal/bus"
	"github.com/cinvat/arsan/internal/core"
	"github.com/cinvat/arsan/pkg/util"
	"github.com/gin-gonic/gin"
)

// Bootstrap is the one request an agent may make without a prior credential.
//
// It is the kubelet model: the agent knows its own identity and capacity, and
// presents a shared token to be believed. The api validates the token as an
// authentication concern, upserts the node, and tells the controller over the
// bus. The agent is the gRPC server, so the controller dials it afterwards using
// the address in this request.
type Bootstrap struct {
	// Token is the shared bootstrap secret, compared against the one the
	// api process was started with. There is no token table: the api
	// holds one value in its own configuration.
	Token string
	// Nodes is the api's CRUD view of a node record.
	Nodes core.NodeService
	// Bus carries the notification to the controller.
	Bus bus.Bus
}

// registerNodeRequest is what an agent sends on every bootstrap: identity
// and its primary IP for display. Nothing here is load-bearing: capacity
// arrives via the probe, routing via the node id.
type registerNodeRequest struct {
	Token string `json:"token"`
	Name  string `json:"name"`
	IP    string `json:"ip"`
}

func (b *Bootstrap) RegisterNode(c *gin.Context) {
	var req registerNodeRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, errBody("bad_request", errors.New(err.Error())))
		return
	}

	if err := b.checkToken(req.Token); err != nil {
		c.JSON(http.StatusUnauthorized, errBody("unauthenticated", err))
		return
	}
	if req.Name == "" {
		c.JSON(http.StatusBadRequest, errBody("bad_request", errors.New("name required")))
		return
	}

	ctx := c.Request.Context()

	// Upsert. Re-registering leaves state, spec and reservations alone, so an
	// agent restarting does not reset a host that is mid-build.
	node, err := b.Nodes.Register(ctx, core.RegisterNodeRequest{Name: req.Name, IP: req.IP})
	if err != nil {
		c.JSON(http.StatusInternalServerError, errBody("internal", err))
		return
	}

	// Tell the controller, without waiting for the ack.
	//
	// Registration does not need to block on delivery. The node is NOTREADY
	// and excluded from placement, so if the announcement is lost nothing
	// bad can happen to a VM; the agent re-bootstraps on its next start, and
	// the reconciliation sweep retries. Blocking here would make an agent's
	// start-up time depend on the controller's queue latency for no safety
	// gain.
	if err := b.Bus.PublishAsync(ctx, bus.Command{
		ID:      util.NewID("cmd"),
		Topic:   bus.TopicNodeRegistered,
		Target:  node.ID,
		Payload: mustJSON(map[string]string{"node": node.ID}),
	}); err != nil {
		// Only a malformed command fails here; a slow queue does not.
		c.JSON(http.StatusServiceUnavailable, errBody("publish_failed", err))
		return
	}

	// The session ends here: the agent keeps no session, and every later
	// call reaches it over the bus at agent.<node-id>.
	c.JSON(http.StatusOK, node)
}

// checkToken compares the presented token against the configured one.
//
// Constant time, because a byte-wise compare leaks the secret's length and a
// prefix one byte at a time.
func (b *Bootstrap) checkToken(presented string) error {
	if b.Token == "" {
		return errors.New("bootstrap is not configured on this api")
	}
	if presented == "" {
		return errors.New("token required")
	}
	if subtle.ConstantTimeCompare([]byte(presented), []byte(b.Token)) != 1 {
		return errors.New("invalid token")
	}
	return nil
}

// usageRequest carries reported usage. It is informational only: available
// capacity is derived from reservations, so a stale or wrong report cannot
// cause an overcommit.
type usageRequest struct {
	Usage core.Resources `json:"usage"`
}

// RecordUsage stores what an agent reports it is using. It does not
// re-authenticate, because the node record already exists.
//
// It does not affect readiness: the controller decides that by probing the
// agent over gRPC, and this only proves the request arrived.
func (h *Handler) RecordUsage(c *gin.Context) {
	var req usageRequest
	// An empty body is valid: a report with nothing to send.
	if c.Request.ContentLength > 0 && !bind(c, &req) {
		return
	}
	if err := h.nodes.RecordUsage(c.Request.Context(), c.Param("id"), req.Usage); err != nil {
		writeErr(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		// Only ever called with a map of strings, which cannot fail to
		// marshal. Panicking keeps a caller-side bug from becoming a
		// silently empty command payload.
		panic("bus: payload must marshal: " + err.Error())
	}
	return b
}
