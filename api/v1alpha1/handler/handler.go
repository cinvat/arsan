// Package handler holds the v1alpha1 HTTP handlers.
//
// Handlers parse, call a service, and render JSON. They hold no domain logic
// and no state, and they map core sentinel errors onto status codes.
//
// They speak the kloudapi object contract (apiVersion/kind/metadata/spec/
// status): the wire shape is the envelope, while the services underneath work
// in arsan's core types. The conversion lives in object; handlers only call it.
package handler

import (
	"errors"
	"net/http"

	"github.com/cinvat/arsan/api/v1alpha1/object"
	"github.com/cinvat/arsan/internal/core"
	"github.com/gin-gonic/gin"
	"github.com/samber/lo"
)

// filterByState keeps the VMs in a state, so list and watch agree on what
// ?state= means instead of drifting into two implementations.
func filterByState(insts []core.Instance, state string) []core.Instance {
	return lo.Filter(insts, func(i core.Instance, _ int) bool {
		return string(i.State) == state
	})
}

// Handler holds the services this version's routes call.
type Handler struct {
	instances core.InstanceService
	nodes     core.NodeService
	flavors   core.FlavorService
	volumes   core.VolumeService
}

func New(instances core.InstanceService, nodes core.NodeService, flavors core.FlavorService, volumes core.VolumeService) *Handler {
	return &Handler{instances: instances, nodes: nodes, flavors: flavors, volumes: volumes}
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
	out := lo.Map(nodes, func(n core.Node, _ int) object.Node { return object.NewNode(n) })
	c.JSON(http.StatusOK, out)
}

// GetNode returns one node. Discovery advertises `get` for nodes, so the verb
// has to exist: an advertised verb with no route is a broken promise.
func (h *Handler) GetNode(c *gin.Context) {
	n, err := h.nodes.Get(c.Request.Context(), c.Param("id"))
	if err != nil {
		writeErr(c, err)
		return
	}
	c.JSON(http.StatusOK, object.NewNode(n))
}

// ----------------------------------------------------------------- servers

func (h *Handler) CreateVirtualMachine(c *gin.Context) {
	var obj object.VirtualMachine
	if !bind(c, &obj) {
		return
	}
	req := obj.ToCreateRequest()
	if req.Name == "" || (req.ImageRef == "" && len(req.Volumes) == 0) {
		writeErr(c, core.ErrInvalidRequest)
		return
	}

	inst, err := h.instances.Create(c.Request.Context(), req)
	if err != nil {
		writeErr(c, err)
		return
	}
	// Accepted, not Created: the build is asynchronous. Clients poll
	// GET the virtualmachine until the state settles.
	c.JSON(http.StatusAccepted, object.NewVirtualMachine(inst))
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
		insts = filterByState(insts, state)
	}
	out := lo.Map(insts, func(i core.Instance, _ int) object.VirtualMachine { return object.NewVirtualMachine(i) })
	c.JSON(http.StatusOK, out)
}

func (h *Handler) GetVirtualMachine(c *gin.Context) {
	inst, err := h.instances.Get(c.Request.Context(), c.Param("id"))
	if err != nil {
		writeErr(c, err)
		return
	}
	c.JSON(http.StatusOK, object.NewVirtualMachine(inst))
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

// PatchVirtualMachine applies a merge patch to a VM's mutable options and
// returns the object as the client asked for it. Today that is a resize
// (spec.flavor or spec.flavorRef); anything immutable is refused by the
// service, not silently ignored.
func (h *Handler) PatchVirtualMachine(c *gin.Context) {
	id := c.Param("id")
	inst, err := h.instances.Get(c.Request.Context(), id)
	if err != nil {
		writeErr(c, err)
		return
	}
	patched, ok := patchedObject(c, object.NewVirtualMachine(inst))
	if !ok {
		return
	}
	req := core.UpdateInstanceRequest{FlavorRef: patched.Spec.FlavorRef}
	if patched.Spec.Flavor != nil {
		req.Flavor = &core.Flavor{VCPU: patched.Spec.Flavor.VCPU, RAM: patched.Spec.Flavor.Memory.MiB()}
	}
	if err := h.instances.Update(c.Request.Context(), id, req); err != nil {
		writeErr(c, err)
		return
	}
	c.JSON(http.StatusAccepted, patched)
}

// RebootVirtualMachine restarts a running guest: a subresource action, not a
// spec change, so it is addressed as <vm>/reboot.
func (h *Handler) RebootVirtualMachine(c *gin.Context) {
	if err := h.instances.Reboot(c.Request.Context(), c.Param("id")); err != nil {
		writeErr(c, err)
		return
	}
	c.JSON(http.StatusAccepted, gin.H{"status": "rebooting"})
}

// AttachVolume enqueues attaching a prepared volume, optionally taking the
// boot position. Live when the guest runs, redefine when it does not.
func (h *Handler) AttachVolume(c *gin.Context) {
	var req struct {
		VolumeID  string `json:"volume_id"`
		BootIndex *int   `json:"boot_index"`
	}
	if !bind(c, &req) {
		return
	}
	if err := h.instances.AttachVolume(c.Request.Context(), c.Param("id"), req.VolumeID, req.BootIndex); err != nil {
		writeErr(c, err)
		return
	}
	c.JSON(http.StatusAccepted, gin.H{"status": "attaching"})
}

func (h *Handler) DetachVolume(c *gin.Context) {
	if err := h.instances.DetachVolume(c.Request.Context(), c.Param("id"), c.Param("volumeId")); err != nil {
		writeErr(c, err)
		return
	}
	c.JSON(http.StatusAccepted, gin.H{"status": "detaching"})
}

// SetBootVolume moves the single boot position to another attached volume.
// The VM must be stopped: boot order lives in the defined XML.
func (h *Handler) SetBootVolume(c *gin.Context) {
	var req struct {
		VolumeID string `json:"volume_id"`
	}
	if !bind(c, &req) {
		return
	}
	if err := h.instances.SetBootVolume(c.Request.Context(), c.Param("id"), req.VolumeID); err != nil {
		writeErr(c, err)
		return
	}
	c.JSON(http.StatusAccepted, gin.H{"status": "setting boot volume"})
}

func (h *Handler) CreateVolume(c *gin.Context) {
	var obj object.Volume
	if !bind(c, &obj) {
		return
	}
	v, err := h.volumes.Create(c.Request.Context(), obj.ToCreateRequest())
	if err != nil {
		writeErr(c, err)
		return
	}
	c.JSON(http.StatusCreated, object.NewVolume(v))
}

func (h *Handler) ListVolumes(c *gin.Context) {
	vs, err := h.volumes.List(c.Request.Context())
	if err != nil {
		writeErr(c, err)
		return
	}
	out := lo.Map(vs, func(v core.Volume, _ int) object.Volume { return object.NewVolume(v) })
	c.JSON(http.StatusOK, out)
}

func (h *Handler) GetVolume(c *gin.Context) {
	v, err := h.volumes.Get(c.Request.Context(), c.Param("id"))
	if err != nil {
		writeErr(c, err)
		return
	}
	c.JSON(http.StatusOK, object.NewVolume(v))
}

func (h *Handler) DeleteVolume(c *gin.Context) {
	if err := h.volumes.Delete(c.Request.Context(), c.Param("id")); err != nil {
		writeErr(c, err)
		return
	}
	c.Status(http.StatusNoContent)
}

// PatchVolume refuses every patch: a volume's size and image are fixed at
// create and boot order moves through the VM's set-boot subresource, so there
// is no mutable field a merge patch could legitimately change.
func (h *Handler) PatchVolume(c *gin.Context) {
	c.JSON(http.StatusUnprocessableEntity, errBody("immutable",
		errors.New("volume spec is immutable; boot order moves via the VM's set-boot subresource")))
}

func (h *Handler) CreateFlavor(c *gin.Context) {
	var obj object.Flavor
	if !bind(c, &obj) {
		return
	}
	req := obj.ToCreateRequest()
	if req.Name == "" {
		writeErr(c, core.ErrInvalidRequest)
		return
	}
	f, err := h.flavors.Create(c.Request.Context(), req)
	if err != nil {
		writeErr(c, err)
		return
	}
	c.JSON(http.StatusCreated, object.NewFlavor(f))
}

func (h *Handler) ListFlavors(c *gin.Context) {
	fs, err := h.flavors.List(c.Request.Context())
	if err != nil {
		writeErr(c, err)
		return
	}
	out := lo.Map(fs, func(f core.Flavor, _ int) object.Flavor { return object.NewFlavor(f) })
	c.JSON(http.StatusOK, out)
}

func (h *Handler) GetFlavor(c *gin.Context) {
	f, err := h.flavors.Get(c.Request.Context(), c.Param("name"))
	if err != nil {
		writeErr(c, err)
		return
	}
	c.JSON(http.StatusOK, object.NewFlavor(f))
}

func (h *Handler) DeleteFlavor(c *gin.Context) {
	if err := h.flavors.Delete(c.Request.Context(), c.Param("name")); err != nil {
		writeErr(c, err)
		return
	}
	c.Status(http.StatusNoContent)
}

// PatchFlavor refuses every patch: flavors are immutable by design, so a
// resize is a new flavor, never an edit.
func (h *Handler) PatchFlavor(c *gin.Context) {
	c.JSON(http.StatusUnprocessableEntity, errBody("immutable",
		errors.New("flavors are immutable; create a new flavor and resize the VM onto it")))
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
	case errors.Is(err, core.ErrInstanceNotFound), errors.Is(err, core.ErrNodeNotFound), errors.Is(err, core.ErrFlavorNotFound), errors.Is(err, core.ErrVolumeNotFound):
		write(c, http.StatusNotFound, "not_found", err)
	case errors.Is(err, core.ErrUnsupportedPlatform):
		write(c, http.StatusFailedDependency, "unsupported_platform", err)
	default:
		write(c, http.StatusInternalServerError, "internal", err)
	}
}
