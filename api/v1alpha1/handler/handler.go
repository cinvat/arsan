// Package handler holds the v1alpha1 HTTP handlers.
//
// Handlers parse, call a service, and render JSON. They hold no domain logic
// and no state, and they map core sentinel errors onto status codes.
package handler

import (
	"errors"
	"net/http"

	"github.com/cinvat/arsan/internal/core"
	"github.com/gin-gonic/gin"
)

// Handler holds the services this version's routes call.
type Handler struct {
	instances core.InstanceService
	nodes     core.NodeService
	scheduler core.Scheduler
}

func New(instances core.InstanceService, nodes core.NodeService, scheduler core.Scheduler) *Handler {
	return &Handler{instances: instances, nodes: nodes, scheduler: scheduler}
}

func Health(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

func (h *Handler) ListNodes(c *gin.Context) {
	nodes, err := h.nodes.List(c.Request.Context())
	if err != nil {
		writeErr(c, err)
		return
	}
	c.JSON(http.StatusOK, nodes)
}

// ----------------------------------------------------------------- servers

func (h *Handler) CreateServer(c *gin.Context) {
	var req core.CreateInstanceRequest
	if !bind(c, &req) {
		return
	}
	if req.Name == "" || req.ImageRef == "" {
		writeErr(c, core.ErrInvalidRequest)
		return
	}

	inst, err := h.instances.Create(c.Request.Context(), req)
	if err != nil {
		writeErr(c, err)
		return
	}
	// Accepted, not Created: the build is asynchronous. Clients poll
	// GET /servers/{id} until the state settles.
	c.JSON(http.StatusAccepted, inst)
}

func (h *Handler) ListServers(c *gin.Context) {
	insts, err := h.instances.List(c.Request.Context())
	if err != nil {
		writeErr(c, err)
		return
	}
	if state := c.Query("state"); state != "" {
		filtered := insts[:0:0]
		for _, i := range insts {
			if string(i.State) == state {
				filtered = append(filtered, i)
			}
		}
		insts = filtered
	}
	c.JSON(http.StatusOK, insts)
}

func (h *Handler) GetServer(c *gin.Context) {
	inst, err := h.instances.Get(c.Request.Context(), c.Param("id"))
	if err != nil {
		writeErr(c, err)
		return
	}
	c.JSON(http.StatusOK, inst)
}

func (h *Handler) DeleteServer(c *gin.Context) {
	if err := h.instances.Delete(c.Request.Context(), c.Param("id")); err != nil {
		writeErr(c, err)
		return
	}
	c.Status(http.StatusNoContent)
}

func (h *Handler) StartServer(c *gin.Context) {
	if err := h.instances.Start(c.Request.Context(), c.Param("id")); err != nil {
		writeErr(c, err)
		return
	}
	c.JSON(http.StatusAccepted, gin.H{"status": "starting"})
}

func (h *Handler) StopServer(c *gin.Context) {
	if err := h.instances.Stop(c.Request.Context(), c.Param("id")); err != nil {
		writeErr(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "stopped"})
}

// ----------------------------------------------------------------- helpers

func bind(c *gin.Context, dst any) bool {
	if err := c.ShouldBindJSON(dst); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"code":    "bad_request",
			"message": err.Error(),
		})
		return false
	}
	return true
}

// writeErr maps core sentinels onto status codes. This switch is the whole
// reason the sentinels survive the service layers: a flattened error would
// turn an actionable 409 into a 500.
func writeErr(c *gin.Context, err error) {
	switch {
	case errors.Is(err, core.ErrInvalidRequest), errors.Is(err, core.ErrRequestRejected):
		write(c, http.StatusBadRequest, "bad_request", err)
	case errors.Is(err, core.ErrInvalidTransition):
		write(c, http.StatusConflict, "invalid_state", err)
	case errors.Is(err, core.ErrNoValidNode):
		write(c, http.StatusConflict, "no_valid_host", err)
	case errors.Is(err, core.ErrCapacityConflict):
		write(c, http.StatusConflict, "capacity_conflict", err)
	case errors.Is(err, core.ErrInstanceNotFound), errors.Is(err, core.ErrNodeNotFound):
		write(c, http.StatusNotFound, "not_found", err)
	case errors.Is(err, core.ErrUnsupportedPlatform):
		write(c, http.StatusFailedDependency, "unsupported_platform", err)
	default:
		write(c, http.StatusInternalServerError, "internal", err)
	}
}
