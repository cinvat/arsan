package agent

import (
	"context"
	"sync"

	"github.com/cinvat/arsan/internal/core"
)

// AgentServer serves engine operations over gRPC.
//
// It holds only the compute engine. Identity, capacity and liveness all arrive
// by REST at bootstrap and live in the node record, so carrying them here would
// be a second copy that could disagree with the control plane's.
type AgentServer struct {
	mu      sync.Mutex
	driver  core.Driver
	stopped bool
}

func NewAgentServer(drv core.Driver) *AgentServer {
	return &AgentServer{driver: drv}
}

func (s *AgentServer) Create(ctx context.Context, req *CreateRequest) (*CreateResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return &CreateResponse{Success: false, Message: "agent stopped"}, nil
	}
	info, err := s.driver.Create(ctx, core.CreateRequest{
		InstanceID: req.InstanceID,
		NodeID:     req.NodeID,
		Name:       req.Name,
		Flavor:     ToCoreFlavor(req.Flavor),
		ImageRef:   req.ImageRef,
	})
	if err != nil {
		return &CreateResponse{Success: false, Message: err.Error()}, nil
	}
	return &CreateResponse{Success: true, Message: "created", Info: &InstanceInfo{
		ID: info.ID, NodeID: info.NodeID, Driver: info.Driver, Started: info.Started,
	}}, nil
}

func (s *AgentServer) Start(ctx context.Context, req *StartRequest) (*StartResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return &StartResponse{Success: false, Message: "agent stopped"}, nil
	}
	if err := s.driver.Start(ctx, req.InstanceID); err != nil {
		return &StartResponse{Success: false, Message: err.Error()}, nil
	}
	return &StartResponse{Success: true, Message: "started"}, nil
}

func (s *AgentServer) Stop(ctx context.Context, req *StopRequest) (*StopResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return &StopResponse{Success: false, Message: "agent stopped"}, nil
	}
	if err := s.driver.Stop(ctx, req.InstanceID); err != nil {
		return &StopResponse{Success: false, Message: err.Error()}, nil
	}
	return &StopResponse{Success: true, Message: "stopped"}, nil
}

func (s *AgentServer) Delete(ctx context.Context, req *DeleteRequest) (*DeleteResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return &DeleteResponse{Success: false, Message: "agent stopped"}, nil
	}
	if err := s.driver.Delete(ctx, req.InstanceID); err != nil {
		return &DeleteResponse{Success: false, Message: err.Error()}, nil
	}
	return &DeleteResponse{Success: true, Message: "deleted"}, nil
}

func (s *AgentServer) Reboot(ctx context.Context, req *RebootRequest) (*RebootResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return &RebootResponse{Success: false, Message: "agent stopped"}, nil
	}
	if err := s.driver.Reboot(ctx, req.InstanceID); err != nil {
		return &RebootResponse{Success: false, Message: err.Error()}, nil
	}
	return &RebootResponse{Success: true, Message: "rebooted"}, nil
}

func (s *AgentServer) Migrate(ctx context.Context, req *MigrateRequest) (*MigrateResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return &MigrateResponse{Success: false, Message: "agent stopped"}, nil
	}
	if err := s.driver.Migrate(ctx, req.InstanceID, req.TargetNodeID); err != nil {
		return &MigrateResponse{Success: false, Message: err.Error()}, nil
	}
	return &MigrateResponse{Success: true, Message: "migrated"}, nil
}

func (s *AgentServer) Usage(ctx context.Context, req *UsageRequest) (*UsageResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return &UsageResponse{Success: false, Message: "agent stopped"}, nil
	}
	usage, err := s.driver.Usage(ctx)
	if err != nil {
		return &UsageResponse{Success: false, Message: err.Error()}, nil
	}
	return &UsageResponse{Success: true, Message: "ok", Usage: FromCoreResources(usage)}, nil
}

func (s *AgentServer) Validate(ctx context.Context, req *ValidateRequest) (*ValidateResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return &ValidateResponse{CanRun: false, Message: "agent stopped"}, nil
	}
	err := s.driver.Validate(ctx, ToCoreFlavor(req.Flavor))
	if err != nil {
		return &ValidateResponse{CanRun: false, Message: err.Error()}, nil
	}
	return &ValidateResponse{CanRun: true, Message: "ok"}, nil
}

func (s *AgentServer) StopAgent() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stopped = true
}

var _ ComputeNodeAgentServer = (*AgentServer)(nil)
