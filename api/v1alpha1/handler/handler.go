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
	flavors   core.FlavorService
}

func New(instances core.InstanceService, nodes core.NodeService, flavors core.FlavorService) *Handler {
	return &Handler{instances: instances, nodes: nodes, flavors: flavors}
}

func Health(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

func (h *Handler) ListNodes(c *gin.Context) {
	if _, ok := c.GetQuery("watch"); ok {
		h.WatchNodes(c)
		return
	}
	nodes, err := h.nodes.List(c.Request.Context())
	if err != nil {
		writeErr(c, err)
		return
	}
	c.JSON(http.StatusOK, nodes)
}

// ----------------------------------------------------------------- servers

func (h *Handler) CreateVirtualMachine(c *gin.Context) {
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

func (h *Handler) ListVirtualMachines(c *gin.Context) {
	if _, ok := c.GetQuery("watch"); ok {
		h.WatchVirtualMachines(c)
		return
	}
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

func (h *Handler) GetVirtualMachine(c *gin.Context) {
	inst, err := h.instances.Get(c.Request.Context(), c.Param("id"))
	if err != nil {
		writeErr(c, err)
		return
	}
	c.JSON(http.StatusOK, inst)
}

func (h *Handler) DeleteVirtualMachine(c *gin.Context) {
	if err := h.instances.Delete(c.Request.Context(), c.Param("id")); err != nil {
		writeErr(c, err)
		return
	}
	c.Status(http.StatusNoContent)
}

func (h *Handler) StartVirtualMachine(c *gin.Context) {
	if err := h.instances.Start(c.Request.Context(), c.Param("id")); err != nil {
		writeErr(c, err)
		return
	}
	c.JSON(http.StatusAccepted, gin.H{"status": "starting"})
}

func (h *Handler) StopVirtualMachine(c *gin.Context) {
	if err := h.instances.Stop(c.Request.Context(), c.Param("id")); err != nil {
		writeErr(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "stopped"})
}

// AttachDisk enqueues an extra data disk. Size is a Quantity string ("50GB");
// the controller provisions and hotplugs it, or redefines a stopped guest.
func (h *Handler) AttachDisk(c *gin.Context) {
	var req core.CreateDiskRequest
	if !bind(c, &req) {
		return
	}
	if err := h.instances.AttachDisk(c.Request.Context(), c.Param("id"), req); err != nil {
		writeErr(c, err)
		return
	}
	c.JSON(http.StatusAccepted, gin.H{"status": "attaching"})
}

func (h *Handler) DetachDisk(c *gin.Context) {
	if err := h.instances.DetachDisk(c.Request.Context(), c.Param("id"), c.Param("name")); err != nil {
		writeErr(c, err)
		return
	}
	c.JSON(http.StatusAccepted, gin.H{"status": "detaching"})
}

// AttachCdrom enqueues a read-only iso. Boot order itself only changes at
// define time: attach, set boot to cdrom, start.
func (h *Handler) AttachCdrom(c *gin.Context) {
	var req struct {
		Image string `json:"image"`
	}
	if !bind(c, &req) {
		return
	}
	if err := h.instances.AttachCdrom(c.Request.Context(), c.Param("id"), req.Image); err != nil {
		writeErr(c, err)
		return
	}
	c.JSON(http.StatusAccepted, gin.H{"status": "attaching"})
}

func (h *Handler) DetachCdrom(c *gin.Context) {
	if err := h.instances.DetachCdrom(c.Request.Context(), c.Param("id")); err != nil {
		writeErr(c, err)
		return
	}
	c.JSON(http.StatusAccepted, gin.H{"status": "detaching"})
}

// SetBootDevice selects what boots next, disk or cdrom. The VM must be
// stopped: boot order lives in the defined XML.
func (h *Handler) SetBootDevice(c *gin.Context) {
	var req struct {
		Device string `json:"device"`
	}
	if !bind(c, &req) {
		return
	}
	if err := h.instances.SetBootDevice(c.Request.Context(), c.Param("id"), req.Device); err != nil {
		writeErr(c, err)
		return
	}
	c.JSON(http.StatusAccepted, gin.H{"status": "setting boot device"})
}

func (h *Handler) CreateFlavor(c *gin.Context) {
	var req core.CreateFlavorRequest
	if !bind(c, &req) {
		return
	}
	if req.Name == "" {
		writeErr(c, core.ErrInvalidRequest)
		return
	}
	f, err := h.flavors.Create(c.Request.Context(), req)
	if err != nil {
		writeErr(c, err)
		return
	}
	c.JSON(http.StatusCreated, f)
}

func (h *Handler) ListFlavors(c *gin.Context) {
	out, err := h.flavors.List(c.Request.Context())
	if err != nil {
		writeErr(c, err)
		return
	}
	if out == nil {
		out = []core.Flavor{}
	}
	c.JSON(http.StatusOK, out)
}

func (h *Handler) GetFlavor(c *gin.Context) {
	f, err := h.flavors.Get(c.Request.Context(), c.Param("name"))
	if err != nil {
		writeErr(c, err)
		return
	}
	c.JSON(http.StatusOK, f)
}

func (h *Handler) DeleteFlavor(c *gin.Context) {
	if err := h.flavors.Delete(c.Request.Context(), c.Param("name")); err != nil {
		writeErr(c, err)
		return
	}
	c.Status(http.StatusNoContent)
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
	case errors.Is(err, core.ErrInstanceNotFound), errors.Is(err, core.ErrNodeNotFound), errors.Is(err, core.ErrFlavorNotFound):
		write(c, http.StatusNotFound, "not_found", err)
	case errors.Is(err, core.ErrUnsupportedPlatform):
		write(c, http.StatusFailedDependency, "unsupported_platform", err)
	default:
		write(c, http.StatusInternalServerError, "internal", err)
	}
}
