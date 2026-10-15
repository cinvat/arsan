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
	Bus       bus.Bus
	// BootstrapToken is the shared secret an agent presents to register.
	// Empty disables agent registration entirely.
	BootstrapToken string
}

// Register mounts this version onto the engine under its own group.
func Register(r *gin.Engine, cfg Config) {
	h := handler.New(cfg.Instances, cfg.Nodes)

	bootstrap := &handler.Bootstrap{
		Token: cfg.BootstrapToken,
		Nodes: cfg.Nodes,
		Bus:   cfg.Bus,
	}

	g := r.Group(APIVersion)

	g.GET("/health", handler.Health)

	// Nodes. An agent registers itself here with the bootstrap token and is
	// the only writer of its own record; there is no unauthenticated route
	// that creates a node.
	g.POST("/nodes", bootstrap.RegisterNode)
	g.GET("/nodes", h.ListNodes)
	g.POST("/nodes/:id/usage", h.RecordUsage)

	// VirtualMachines. CRUD only: create persists PENDING and publishes a command,
	// and every state change afterwards is the controller's doing.
	g.POST("/virtualmachines", h.CreateVirtualMachine)
	g.GET("/virtualmachines", h.ListVirtualMachines)
	g.GET("/virtualmachines/:id", h.GetVirtualMachine)
	g.DELETE("/virtualmachines/:id", h.DeleteVirtualMachine)
	g.POST("/virtualmachines/:id/start", h.StartVirtualMachine)
	g.POST("/virtualmachines/:id/stop", h.StopVirtualMachine)
}
