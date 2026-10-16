# Step 8: Controller REST API

## Goal
Implement the GIN-based REST API for the control plane: `/virtualmachines`,  `/nodes` (`?watch=true` streams), `/healthz`.

---

## File: `internal/api/api.go`

```go
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/cinvat/arsan/internal/bus"
	"github.com/cinvat/arsan/internal/core"
	"github.com/gin-gonic/gin"
)

type Config struct {
	Instances core.InstanceService
	Nodes     core.NodeService
	Bus       bus.Bus
	// BootstrapToken is the shared secret an agent presents to register.
	BootstrapToken string
	Log            Logger
}

type Logger interface {
	Warn(msg string, kv ...any)
	Info(msg string, kv ...any)
	Error(msg string, kv ...any)
}

type Server struct {
	cfg Config
}

func New(cfg Config) *Server {
	if cfg.RequestTimeout == 0 { cfg.RequestTimeout = 30 * time.Second }
	if cfg.Log == nil { cfg.Log = nopLogger{} }
	return &Server{cfg: cfg}
}

type nopLogger struct{}
func (nopLogger) Info(string, ...any)  {}
func (nopLogger) Warn(string, ...any)  {}
func (nopLogger) Error(string, ...any) {}

func (s *Server) Handler() http.Handler {
	r := gin.New()
	r.Use(gin.Recovery(), s.loggerMiddleware())
	r.GET("/healthz", func(c *gin.Context) { c.JSON(http.StatusOK, gin.H{"status": "ok"}) })

	// Nodes
	r.POST("/nodes", s.registerNode)
	r.GET("/nodes", s.listNodes)
	r.GET("/nodes/:id", s.getNode)
	r.POST("/nodes/:id/usage", s.usage)
	r.POST("/nodes/:id/disable", s.disableNode)
	r.POST("/nodes/:id/enable", s.enableNode)

	// Servers
	r.POST("/virtualmachines", s.createVirtualMachine)
	r.GET("/virtualmachines", s.listVirtualMachines)
	r.GET("/virtualmachines/:id", s.getVirtualMachine)
	r.DELETE("/virtualmachines/:id", s.deleteVirtualMachine)
	r.POST("/virtualmachines/:id/start", s.startVirtualMachine)
	r.POST("/virtualmachines/:id/stop", s.stopVirtualMachine)

	return r
}

func (s *Server) loggerMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		s.cfg.Log.Info("http",
			"method", c.Request.Method, "path", c.Request.URL.Path,
			"status", c.Writer.Status(), "duration", time.Since(start))
	}
}

func (s *Server) registerNode(c *gin.Context) {
	var req struct {
		Name   string `json:"name" binding:"required"`
		Arch   string `json:"arch"`
		Driver string `json:"driver"`
		Total  core.Resources `json:"total" binding:"required"`
	}
	if !bind(c, &req) { return }

	n, err := s.cfg.Nodes.Register(c.Request.Context(), core.RegisterNodeRequest{
		Name: req.Name, Arch: req.Arch, Driver: req.Driver, Total: req.Total,
	})
	if err != nil { writeErr(c, err); return }
	c.JSON(http.StatusCreated, n)
}

func (s *Server) listNodes(c *gin.Context) {
	nodes, err := s.cfg.Nodes.List(c.Request.Context())
	if err != nil { writeErr(c, err); return }
	c.JSON(http.StatusOK, nodes)
}

func (s *Server) getNode(c *gin.Context) {
	n, err := s.cfg.Nodes.Get(c.Request.Context(), c.Param("id"))
	if err != nil { writeErr(c, err); return }
	c.JSON(http.StatusOK, n)
}

// usage records what an agent reports it is using. It does not affect
// readiness: the controller decides that by probing over NATS, and a usage
// report only proves the request arrived.
func (s *Server) usage(c *gin.Context) {
	id := c.Param("id")
	var req struct { Usage core.Resources `json:"usage"` }
	if c.Request.ContentLength > 0 && !bind(c, &req) { return }
	if err := s.cfg.Nodes.RecordUsage(c.Request.Context(), id, req.Usage); err != nil {
		writeErr(c, err); return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

func (s *Server) disableNode(c *gin.Context) {
	if err := s.cfg.Nodes.Disable(c.Request.Context(), c.Param("id")); err != nil {
		writeErr(c, err); return
	}
	c.JSON(http.StatusOK, gin.H{"status": "disabled"})
}

func (s *Server) enableNode(c *gin.Context) {
	if err := s.cfg.Nodes.Enable(c.Request.Context(), c.Param("id")); err != nil {
		writeErr(c, err); return
	}
	c.JSON(http.StatusOK, gin.H{"status": "enabled"})
}

func (s *Server) createServer(c *gin.Context) {
	var req struct {
		Name     string `json:"name" binding:"required"`
		ImageRef string `json:"image_ref" binding:"required"`
		Flavor   core.Flavor `json:"flavor" binding:"required"`
	}
	if !bind(c, &req) { return }

	inst, err := s.cfg.Instances.Create(c.Request.Context(), core.CreateInstanceRequest{
		Name: req.Name, ImageRef: req.ImageRef, Flavor: req.Flavor,
	})
	if err != nil { writeErr(c, err); return }
	c.JSON(http.StatusAccepted, inst) // 202: async, poll GET /virtualmachines/:id
}

func (s *Server) listServers(c *gin.Context) {
	if state := c.Query("state"); state != "" {
		all, err := s.cfg.Instances.List(c.Request.Context())
		if err != nil { writeErr(c, err); return }
		var out []core.Instance
		for _, i := range all {
			if string(i.State) == state { out = append(out, i) }
		}
		c.JSON(http.StatusOK, out); return
	}
	insts, err := s.cfg.Instances.List(c.Request.Context())
	if err != nil { writeErr(c, err); return }
	c.JSON(http.StatusOK, insts)
}

func (s *Server) getServer(c *gin.Context) {
	inst, err := s.cfg.Instances.Get(c.Request.Context(), c.Param("id"))
	if err != nil { writeErr(c, err); return }
	c.JSON(http.StatusOK, inst)
}

func (s *Server) deleteServer(c *gin.Context) {
	if err := s.cfg.Instances.Delete(c.Request.Context(), c.Param("id")); err != nil {
		writeErr(c, err); return
	}
	c.Status(http.StatusNoContent)
}

func (s *Server) startServer(c *gin.Context) {
	if err := s.cfg.Instances.Start(c.Request.Context(), c.Param("id")); err != nil {
		writeErr(c, err); return
	}
	c.JSON(http.StatusAccepted, gin.H{"status": "starting"})
}

func (s *Server) stopServer(c *gin.Context) {
	if err := s.cfg.Instances.Stop(c.Request.Context(), c.Param("id")); err != nil {
		writeErr(c, err); return
	}
	c.JSON(http.StatusOK, gin.H{"status": "stopped"})
}

// ========== Helpers ==========

func bind(c *gin.Context, dst any) bool {
	if err := c.ShouldBindJSON(dst); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("malformed body: %v", err)})
		return false
	}
	return true
}

func writeErr(c *gin.Context, err error) {
	switch {
	case errors.Is(err, core.ErrInvalidRequest), errors.Is(err, core.ErrRequestRejected):
		writeJSON(c, http.StatusBadRequest, errBody("bad_request", err))
	case errors.Is(err, core.ErrInvalidTransition):
		writeJSON(c, http.StatusConflict, errBody("invalid_state", err))
	case errors.Is(err, core.ErrNoValidNode):
		writeJSON(c, http.StatusConflict, errBody("no_valid_host", err))
	case errors.Is(err, core.ErrCapacityConflict):
		writeJSON(c, http.StatusConflict, errBody("capacity_conflict", err))
	case errors.Is(err, core.ErrInstanceNotFound), errors.Is(err, core.ErrNodeNotFound):
		writeJSON(c, http.StatusNotFound, errBody("not_found", err))
	case errors.Is(err, core.ErrUnsupportedPlatform):
		writeJSON(c, http.StatusFailedDependency, errBody("unsupported_platform", err))
	default:
		writeJSON(c, http.StatusInternalServerError, errBody("internal", err))
	}
}

func errBody(code string, err error) map[string]any {
	return map[string]any{"code": code, "message": err.Error()}
}

func writeJSON(c *gin.Context, status int, body any) {
	c.Header("Content-Type", "application/json")
	c.JSON(status, body)
}
```

---

## Wire in `cmd/api.go`

```go
package cmd

import (
	"context"
	"fmt"
	"net/http"

	"github.com/cinvat/arsan/internal/api"
	"github.com/cinvat/arsan/internal/compute"
	"github.com/cinvat/arsan/internal/core"
	"github.com/cinvat/arsan/internal/scheduler"
	"github.com/cinvat/arsan/internal/store/memory"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var apiCmd = &cobra.Command{
	Use:   "api",
	Short: "Run the control plane REST API",
	RunE: func(cmd *cobra.Command, _ []string) error { return runAPI(cmd.Context()) },
}

func init() { rootCmd.AddCommand(apiCmd) }

func runAPI(ctx context.Context) error {
	v := viper.GetViper()

	st := memory.New()

	// The api builds no engine and no scheduler. Anything it cannot answer
	// from the store becomes a command on the bus, and the controller decides.
	commands, err := bus.ConnectNATS(v.GetString("bus.url"), 10*time.Second)
	if err != nil { return err }
	defer commands.Close()

	srv := &http.Server{
		Addr: v.GetString("addr"),
		Handler: api.New(api.Config{
			Instances: records.New(st),   // CRUD only
			Nodes:     records.New(st),
			Commands:  commands,          // decisions leave via NATS
			Log:       stdLogger{},
		}).Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	fmt.Printf("API listening on %s\n", v.GetString("addr"))
	return srv.ListenAndServe()
}

type stdLogger struct{}
func (stdLogger) Info(msg string, kv ...any)  { fmt.Printf("INFO %s %v\n", msg, kv) }
func (stdLogger) Warn(msg string, kv ...any)  { fmt.Printf("WARN %s %v\n", msg, kv) }
func (stdLogger) Error(msg string, kv ...any) { fmt.Printf("ERROR %s %v\n", msg, kv) }
```

**The api has no engine, no scheduler and no compute service.** Wiring them in
here — as an earlier draft did — would put every placement decision behind the
same process that serves HTTP, which is the coupling this split exists to avoid.
A `POST /instances` writes the row and publishes a command; the `200` means
"queued", not "running".

Check it, in one line:

```sh
go list -deps ./api/... | grep cinvat
# must show only: api, api/v1alpha1, handler, internal/bus,
#                 internal/core, internal/records, pkg/util
# no scheduler, no compute, no node, no driver
```
---

## API Endpoints Summary

| Method | Path | Description |
|--------|------|-------------|
| GET | `/healthz` | Liveness probe |
| POST | `/nodes` | Register compute node |
| GET | `/nodes` | List all nodes |
| GET | `/nodes/:id` | Get node details |
| POST | `/nodes/:id/usage` | Record reported node usage |
| POST | `/nodes/:id/disable` | Disable node (evict) |
| POST | `/nodes/:id/enable` | Re-enable node |
| POST | `/virtualmachines` | Create VM (202 Accepted) |
| GET | `/virtualmachines` | List VMs (filter by `?state=`) |
| GET | `/virtualmachines/:id` | Get VM state |
| DELETE | `/virtualmachines/:id` | Delete VM |
| POST | `/virtualmachines/:id/start` | Start stopped VM |
| POST | `/virtualmachines/:id/stop` | Stop running VM |

---

## Example Usage

```bash
# Register node
curl -sX POST localhost:8080/nodes \
  -H 'content-type: application/json' \
  -d '{"name":"compute-1","total":{"vcpu":8,"ram":16,"disk":200}}'

# Record usage (does not affect readiness)
curl -sX POST localhost:8080/nodes/n-compute-1/usage \
  -H 'content-type: application/json' \
  -d '{"usage":{"vcpu":2,"ram":4,"disk":20}}'

# Create VM
curl -sX POST localhost:8080/virtualmachines \
  -H 'content-type: application/json' \
  -d '{"name":"web-1","image_ref":"cirros","flavor":{"name":"m1.small","vcpu":1,"ram":2,"disk":10}}'

# Poll until RUNNING
curl -s localhost:8080/virtualmachines/i-abc123 | jq '{id,name,state,node_id,generation}'

# Stop/Start
curl -sX POST localhost:8080/virtualmachines/i-abc123/stop
curl -sX POST localhost:8080/virtualmachines/i-abc123/start
```

---

## Next

Step 9: Remote Driver