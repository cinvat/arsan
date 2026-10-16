package agent

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/cinvat/arsan/internal/bus"
	"github.com/cinvat/arsan/internal/core"
)

// Subject is where one agent listens: agent.<node-id>. The node id is the
// address — there is no host:port to track, which is what lets an agent move
// without anyone noticing.
func Subject(nodeID string) string { return "agent." + nodeID }

// Operation names an engine call. One envelope, one op, optional fields.
type Operation string

const (
	OpInfo     Operation = "info"
	OpValidate Operation = "validate"
	OpCreate   Operation = "create"
	OpStart    Operation = "start"
	OpStop     Operation = "stop"
	OpDelete   Operation = "delete"
	OpReboot   Operation = "reboot"
	OpMigrate  Operation = "migrate"
	OpUsage    Operation = "usage"
	OpDomains  Operation = "domains"
	// Storage ops converge one VM's disks to the record: update on SHUTOFF,
	// attach/detach live when the guest runs.
	OpUpdateDisks Operation = "update-disks"
	OpAttachDisk  Operation = "attach-disk"
	OpDetachDisk  Operation = "detach-disk"
	OpAttachCdrom Operation = "attach-cdrom"
	OpDetachCdrom Operation = "detach-cdrom"
)

// Request is one engine call.
type Request struct {
	Op           Operation   `json:"op"`
	InstanceID   string      `json:"instance_id,omitempty"`
	NodeID       string      `json:"node_id,omitempty"`
	Name         string      `json:"name,omitempty"`
	Namespace    string      `json:"namespace,omitempty"`
	Flavor       core.Flavor `json:"flavor,omitempty"`
	Disks        []core.Disk `json:"disks,omitempty"`
	Disk         core.Disk   `json:"disk,omitempty"`
	CdromImage   string      `json:"cdrom_image,omitempty"`
	DetachedImage string     `json:"detached_image,omitempty"`
	BootDevice   string      `json:"boot_device,omitempty"`
	PinnedCPUs   []int       `json:"pinned_cpus,omitempty"`
	ImageRef     string      `json:"image_ref,omitempty"`
	TargetNodeID string      `json:"target_node_id,omitempty"`
}

// Response answers it. Info, Usage and Domains ride along only on their own ops.
type Response struct {
	OK      bool                `json:"ok"`
	Error   string              `json:"error,omitempty"`
	Info    core.InstanceInfo   `json:"info,omitempty"`
	Usage   core.Resources      `json:"usage,omitempty"`
	Domains []core.DomainStatus `json:"domains,omitempty"`
	Engine  string              `json:"engine,omitempty"`
	Total   core.Resources      `json:"total,omitempty"`
}

// Worker answers engine calls for one node. It owns nothing but the engine:
// identity, capacity and liveness all live in the node record, and the
// controller is the one that reads and writes it.
type Worker struct {
	nodeID string
	driver core.Driver
}

// NewWorker builds the responder for nodeID. Serve attaches it to the bus;
// tests call Handle directly, with no transport at all.
func NewWorker(nodeID string, drv core.Driver) *Worker {
	return &Worker{nodeID: nodeID, driver: drv}
}

// Serve answers until ctx is cancelled.
func Serve(ctx context.Context, commands *bus.NATS, nodeID string, drv core.Driver) error {
	w := NewWorker(nodeID, drv)
	stop, err := commands.Answer(ctx, Subject(nodeID), w.Handle)
	if err != nil {
		return err
	}
	defer stop()
	<-ctx.Done()
	return ctx.Err()
}

// Handle answers one raw request.
func (w *Worker) Handle(ctx context.Context, raw []byte) []byte {
	var req Request
	if err := json.Unmarshal(raw, &req); err != nil {
		return encode(Response{Error: fmt.Sprintf("bad request: %v", err)})
	}
	resp, err := w.exec(ctx, req)
	if err != nil {
		resp = Response{Error: err.Error()}
	} else {
		resp.OK = true
	}
	return encode(resp)
}

func encode(resp Response) []byte {
	raw, err := json.Marshal(resp)
	if err != nil {
		// A fixed struct always marshals; this is unreachable in practice.
		return []byte(`{"ok":false,"error":"encode response"}`)
	}
	return raw
}

func (w *Worker) exec(ctx context.Context, req Request) (Response, error) {
	switch req.Op {
	case OpInfo:
		total, err := hostTotal(ctx, w.driver)
		if err != nil {
			return Response{}, err
		}
		return Response{Engine: w.driver.Name(), Total: total}, nil
	case OpValidate:
		if err := w.driver.Validate(ctx, req.Flavor); err != nil {
			return Response{}, err
		}
		return Response{}, nil
	case OpCreate:
		info, err := w.driver.Create(ctx, core.CreateRequest{
			InstanceID: req.InstanceID, NodeID: req.NodeID, Name: req.Name, Namespace: req.Namespace,
			Flavor: req.Flavor, Disks: req.Disks, CdromImage: req.CdromImage, BootDevice: req.BootDevice,
			PinnedCPUs: req.PinnedCPUs, ImageRef: req.ImageRef,
		})
		if err != nil {
			return Response{}, err
		}
		return Response{Info: info}, nil
	case OpStart:
		return Response{}, w.driver.Start(ctx, req.InstanceID)
	case OpStop:
		return Response{}, w.driver.Stop(ctx, req.InstanceID)
	case OpDelete:
		return Response{}, w.driver.Delete(ctx, req.InstanceID)
	case OpReboot:
		return Response{}, w.driver.Reboot(ctx, req.InstanceID)
	case OpMigrate:
		return Response{}, w.driver.Migrate(ctx, req.InstanceID, req.TargetNodeID)
	case OpUsage:
		usage, err := w.driver.Usage(ctx)
		if err != nil {
			return Response{}, err
		}
		return Response{Usage: usage}, nil
	case OpDomains:
		domains, err := w.driver.Domains(ctx)
		if err != nil {
			return Response{}, err
		}
		return Response{Domains: domains}, nil
	case OpUpdateDisks:
		return Response{}, w.driver.UpdateDisks(ctx, createRequestOf(req))
	case OpAttachDisk:
		return Response{}, w.driver.AttachDisk(ctx, createRequestOf(req), req.Disk)
	case OpDetachDisk:
		return Response{}, w.driver.DetachDisk(ctx, createRequestOf(req), req.Disk)
	case OpAttachCdrom:
		return Response{}, w.driver.AttachCdrom(ctx, createRequestOf(req))
	case OpDetachCdrom:
		return Response{}, w.driver.DetachCdrom(ctx, createRequestOf(req), req.DetachedImage)
	default:
		return Response{}, fmt.Errorf("unknown op %q", req.Op)
	}
}

// createRequestOf rebuilds the engine request from a storage op envelope.
// Attach and detach carry the full post-mutation record state, so the engine
// converges to it without ever reading the api.
func createRequestOf(req Request) core.CreateRequest {
	return core.CreateRequest{
		InstanceID: req.InstanceID, NodeID: req.NodeID, Name: req.Name, Namespace: req.Namespace,
		Flavor: req.Flavor, Disks: req.Disks, CdromImage: req.CdromImage, BootDevice: req.BootDevice,
		PinnedCPUs: req.PinnedCPUs, ImageRef: req.ImageRef,
	}
}

// hostTotal asks the engine what its host has. Engines with nothing to detect
// (fake) report nothing, and the node keeps whatever the bootstrap recorded.
func hostTotal(ctx context.Context, drv core.Driver) (core.Resources, error) {
	if capable, ok := drv.(interface {
		HostCapacity(context.Context) (core.Resources, error)
	}); ok {
		return capable.HostCapacity(ctx)
	}
	return core.Resources{}, nil
}
