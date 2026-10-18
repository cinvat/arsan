// Package v1alpha1 is the first versioned REST surface.
//
// Everything is mounted under the /v1alpha1 group. A breaking change later means
// adding a v1beta1 package alongside this one; v1alpha1 stays exactly as it is
// so old clients keep working.
package v1alpha1

import (
	"github.com/cinvat/arsan/api/v1alpha1/handler"
	"github.com/cinvat/arsan/internal/bus"
	"github.com/cinvat/arsan/internal/core"
	"github.com/gin-gonic/gin"
)

// APIVersion is the path segment this version is served under.
const APIVersion = "v1alpha1"

// Config carries the services and the bootstrap credential.
//
// The api is CRUD plus authentication. It never places and never calls the
// controller's libraries: anything that needs a decision is published to Bus.
type Config struct {
	Instances core.InstanceService
	Nodes     core.NodeService
	Flavors   core.FlavorService
	Volumes   core.VolumeService
	Bus       bus.Bus
	// BootstrapToken is the shared secret an agent presents to register.
	// Empty disables agent registration entirely.
	BootstrapToken string
}

// Register mounts this version onto the engine under its own group.
func Register(r *gin.Engine, cfg Config) {
	h := handler.New(cfg.Instances, cfg.Nodes, cfg.Flavors, cfg.Volumes)

	bootstrap := &handler.Bootstrap{
		Token: cfg.BootstrapToken,
		Nodes: cfg.Nodes,
		Bus:   cfg.Bus,
	}

	g := r.Group(APIVersion)

	// Discovery. GET /v1alpha1 is the APIResourceList an aggregation origin
	// (kloudapi) fetches to learn this version's kinds; it is the group
	// version itself, not a subresource.
	g.GET("", handler.Discovery)

	g.GET("/health", handler.Health)

	// Nodes. An agent registers itself here with the bootstrap token and is
	// the only writer of its own record; there is no unauthenticated route
	// that creates a node.
	g.POST("/nodes", bootstrap.RegisterNode)
	g.GET("/nodes", h.ListNodes)
	g.GET("/nodes/:id", h.GetNode)
	g.POST("/nodes/:id/usage", h.RecordUsage)

	// VirtualMachines. CRUD only: create persists PENDING and publishes a command,
	// and every state change afterwards is the controller's doing.
	g.POST("/virtualmachines", h.CreateVirtualMachine)
	g.GET("/virtualmachines", h.ListVirtualMachines)
	g.GET("/virtualmachines/:id", h.GetVirtualMachine)
	g.PATCH("/virtualmachines/:id", h.PatchVirtualMachine)
	g.DELETE("/virtualmachines/:id", h.DeleteVirtualMachine)
	g.POST("/virtualmachines/:id/start", h.StartVirtualMachine)
	g.POST("/virtualmachines/:id/stop", h.StopVirtualMachine)
	g.POST("/virtualmachines/:id/reboot", h.RebootVirtualMachine)
	g.POST("/virtualmachines/:id/volumes", h.AttachVolume)
	g.DELETE("/virtualmachines/:id/volumes/:volumeId", h.DetachVolume)
	g.POST("/virtualmachines/:id/boot", h.SetBootVolume)
	g.GET("/virtualmachines/:id/console", h.ConsoleVirtualMachine)

	// Volumes. Named, independent block devices: create from a base, blank,
	// or an iso; attach by id; delete removes the record (and the file only
	// when DeleteOnTermination said so). Patch is mounted and refused: the
	// spec is immutable, so kubectl-style clients get a clear 422 rather
	// than a 405 "no such verb".
	g.POST("/volumes", h.CreateVolume)
	g.GET("/volumes", h.ListVolumes)
	g.GET("/volumes/:id", h.GetVolume)
	g.PATCH("/volumes/:id", h.PatchVolume)
	g.DELETE("/volumes/:id", h.DeleteVolume)

	// Flavors. Named, immutable sizing referenced by flavor_ref at VM create.
	g.POST("/flavors", h.CreateFlavor)
	g.GET("/flavors", h.ListFlavors)
	g.GET("/flavors/:name", h.GetFlavor)
	g.PATCH("/flavors/:name", h.PatchFlavor)
	g.DELETE("/flavors/:name", h.DeleteFlavor)
}
