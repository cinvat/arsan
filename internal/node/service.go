package node

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cinvat/arsan/internal/core"
	"github.com/cinvat/arsan/internal/message"
)

const (
	DefaultHeartbeatTimeout = 30 * time.Second
	DefaultLivenessInterval = 10 * time.Second
)

type Logger interface {
	Warn(msg string, kv ...any)
	Info(msg string, kv ...any)
	Error(msg string, kv ...any)
}

type Config struct {
	Nodes core.NodeStore
	Bus   *message.Bus
	Log   Logger
	Arch  string
}

type Service struct {
	cfg Config
}

func New(cfg Config) *Service {
	if cfg.Arch == "" {
		cfg.Arch = "amd64"
	}
	return &Service{cfg: cfg}
}

func (s *Service) Register(ctx context.Context, req core.RegisterNodeRequest) (core.Node, error) {
	if strings.TrimSpace(req.Name) == "" {
		return core.Node{}, fmt.Errorf("name required: %w", core.ErrInvalidRequest)
	}
	arch := req.Arch
	if arch == "" {
		arch = s.cfg.Arch
	}
	// The engine is declared by the agent itself at bootstrap. The controller
	// has no engine of its own to offer as a default, so there is no fallback
	// here: a node that did not say what it runs is a node we cannot place on.
	drv := req.Driver
	if drv == "" {
		return core.Node{}, fmt.Errorf("driver required: %w", core.ErrInvalidRequest)
	}
	if req.Total.VCPU <= 0 || req.Total.RAM <= 0 || req.Total.Disk <= 0 {
		return core.Node{}, fmt.Errorf("total capacity required: %w", core.ErrInvalidRequest)
	}

	n := core.Node{
		ID:      slug(req.Name),
		Name:    req.Name,
		Arch:    arch,
		Driver:  drv,
		Address: req.Address,
		Total:   req.Total,
		Enabled: true,
	}
	node, err := s.cfg.Nodes.Register(ctx, n)
	if err != nil {
		return core.Node{}, fmt.Errorf("register %s: %w", req.Name, err)
	}
	s.cfg.Log.Info("node registered", "node", node.ID, "arch", node.Arch, "driver", node.Driver)
	return node, nil
}

func (s *Service) RecordUsage(ctx context.Context, nodeID string, usage core.Resources) error {
	node, err := s.cfg.Nodes.RecordUsage(ctx, nodeID, usage, time.Now().UTC())
	if err != nil {
		return err
	}
	if node.State == core.NodeReady {
		return nil
	}
	if _, err := s.cfg.Nodes.SetState(ctx, nodeID, core.NodeReady); err != nil {
		return fmt.Errorf("set node %s up: %w", nodeID, err)
	}
	s.cfg.Log.Info("node up", "node", nodeID)
	return nil
}

func (s *Service) Get(ctx context.Context, id string) (core.Node, error) {
	return s.cfg.Nodes.GetNode(ctx, id)
}

func (s *Service) List(ctx context.Context) ([]core.Node, error) {
	return s.cfg.Nodes.ListNodes(ctx)
}

func (s *Service) Enable(ctx context.Context, id string) error {
	node, err := s.cfg.Nodes.GetNode(ctx, id)
	if err != nil {
		return err
	}
	if _, err := s.cfg.Nodes.SetState(ctx, id, node.State); err != nil {
		return err
	}
	return nil
}

func (s *Service) Disable(ctx context.Context, id string) error {
	node, err := s.cfg.Nodes.GetNode(ctx, id)
	if err != nil {
		return err
	}
	if _, err := s.cfg.Nodes.SetState(ctx, id, core.NodeNotReady); err != nil {
		return fmt.Errorf("disable %s: %w", id, err)
	}
	s.cfg.Log.Info("node disabled", "node", id, "was", node.State)
	return nil
}

func (s *Service) Drained(ctx context.Context, id string) ([]string, error) {
	n, err := s.cfg.Nodes.GetNode(ctx, id)
	if err != nil {
		return nil, err
	}
	return n.Instances, nil
}

func slug(name string) string {
	s := strings.ToLower(strings.TrimSpace(name))
	s = strings.ReplaceAll(s, " ", "-")
	return "n-" + s
}
