// Package api assembles the versioned REST surface. Each API version lives in
// its own subpackage and registers itself onto a version group, so adding
// v1beta1 later means adding a directory, not editing this file.
package api

import (
	"time"

	"github.com/cinvat/arsan/api/v1alpha1"
	"github.com/cinvat/arsan/internal/bus"
	"github.com/cinvat/arsan/internal/core"
	"github.com/gin-gonic/gin"
)

// Logger is the narrow logging surface the API needs.
type Logger interface {
	Warn(msg string, kv ...any)
	Info(msg string, kv ...any)
	Error(msg string, kv ...any)
}

type nopLogger struct{}

func (nopLogger) Info(string, ...any)  {}
func (nopLogger) Warn(string, ...any)  {}
func (nopLogger) Error(string, ...any) {}

// Config carries the services the handlers call, plus the command bus.
//
// Handlers depend on the core interfaces and on bus.Bus, never on a concrete
// service or on the controller's packages.
type Config struct {
	Instances core.InstanceService
	Nodes     core.NodeService
	Flavors   core.FlavorService
	Bus       bus.Bus
	// BootstrapToken is the shared secret an agent presents to register.
	BootstrapToken string
	Log            Logger
}

// Register builds the engine and mounts every API version onto it.
func Register(cfg Config) *gin.Engine {
	r := gin.New()
	r.Use(gin.Recovery(), requestLogger(cfg.Log))

	v1alpha1.Register(r, v1alpha1.Config{
		Instances:      cfg.Instances,
		Nodes:          cfg.Nodes,
		Flavors:        cfg.Flavors,
		Bus:            cfg.Bus,
		BootstrapToken: cfg.BootstrapToken,
	})
	return r
}

// requestLogger logs one line per request. A nil Log is replaced rather than
// checked at every call site.
func requestLogger(log Logger) gin.HandlerFunc {
	if log == nil {
		log = nopLogger{}
	}
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		log.Info("http",
			"method", c.Request.Method,
			"path", c.Request.URL.Path,
			"status", c.Writer.Status(),
			"duration", time.Since(start))
	}
}
