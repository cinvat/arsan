# Step 7: Agent gRPC Server

## Goal
Implement the gRPC server that runs on each compute node, wrapping a `core.Driver` and exposing VM operations via gRPC.

---

## Proto Definition: `api/agent/agent.proto`

```protobuf
syntax = "proto3";

package agent;

option go_package = "github.com/cinvat/arsan/api/agent";

service ComputeNodeAgent {
  // Engine operations only.
  //
  // There is deliberately no Register or Heartbeat RPC here. The agent
  // bootstraps over REST (POST /v1alpha1/nodes), like a kubelet registering
  // its Node object; and readiness is the controller's to decide, by probing
  // the Usage RPC below. Adding either here would put identity and readiness
  // back inside the engine service, where they do not belong.
  rpc Create(CreateRequest) returns (CreateResponse);
  rpc Start(StartRequest) returns (StartResponse);
  rpc Stop(StopRequest) returns (StopResponse);
  rpc Delete(DeleteRequest) returns (DeleteResponse);
  rpc Reboot(RebootRequest) returns (RebootResponse);
  rpc Migrate(MigrateRequest) returns (MigrateResponse);
  rpc Usage(UsageRequest) returns (UsageResponse);
  rpc Validate(ValidateRequest) returns (ValidateResponse);
}

// Identity is not part of this service. The agent sends it over REST at
// bootstrap, and readiness comes from the controller probing Usage.

message CreateRequest {
  string instance_id = 1;
  string node_id = 2;
  string name = 3;
  Flavor flavor = 4;
  string image_ref = 5;
}
message CreateResponse { bool success = 1; string message = 2; InstanceInfo info = 3; }

message StartRequest { string instance_id = 1; }
message StartResponse { bool success = 1; string message = 2; }

message StopRequest { string instance_id = 1; }
message StopResponse { bool success = 1; string message = 2; }

message DeleteRequest { string instance_id = 1; }
message DeleteResponse { bool success = 1; string message = 2; }

message RebootRequest { string instance_id = 1; }
message RebootResponse { bool success = 1; string message = 2; }

message MigrateRequest { string instance_id = 1; string target_node_id = 2; }
message MigrateResponse { bool success = 1; string message = 2; }

message UsageRequest {}
message UsageResponse { Resources usage = 1; bool success = 2; string message = 3; }

message ValidateRequest { Flavor flavor = 1; }
message ValidateResponse { bool can_run = 1; string message = 2; }

message InstanceInfo {
  string id = 1; string node_id = 2; string driver = 3; bool started = 4;
}

message Flavor { string name = 1; int32 vcpu = 2; int32 ram = 3; int32 disk = 4; }
message Resources { int32 vcpu = 1; int32 ram = 2; int32 disk = 3; }
```

Generate Go code:
```bash
protoc --go_out=. --go-grpc_out=. api/agent/agent.proto
```

---

## Agent Messages: `internal/agent/agent.go`

```go
package agent

import (
	"context"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/cinvat/arsan/internal/core"
)

type ComputeNodeAgentClient interface {
	Create(ctx context.Context, req *CreateRequest, opts ...grpc.CallOption) (*CreateResponse, error)
	Start(ctx context.Context, req *StartRequest, opts ...grpc.CallOption) (*StartResponse, error)
	Stop(ctx context.Context, req *StopRequest, opts ...grpc.CallOption) (*StopResponse, error)
	Delete(ctx context.Context, req *DeleteRequest, opts ...grpc.CallOption) (*DeleteResponse, error)
	Reboot(ctx context.Context, req *RebootRequest, opts ...grpc.CallOption) (*RebootResponse, error)
	Migrate(ctx context.Context, req *MigrateRequest, opts ...grpc.CallOption) (*MigrateResponse, error)
	Usage(ctx context.Context, req *UsageRequest, opts ...grpc.CallOption) (*UsageResponse, error)
	Validate(ctx context.Context, req *ValidateRequest, opts ...grpc.CallOption) (*ValidateResponse, error)
	Close() error
}

func NewClient(addr string) (ComputeNodeAgentClient, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil { return nil, err }
	return &clientConn{conn: conn, client: NewComputeNodeAgentClient(conn)}, nil
}

type clientConn struct {
	conn   *grpc.ClientConn
	client ComputeNodeAgentClient
}

func (c *clientConn) Create(ctx context.Context, req *CreateRequest, opts ...grpc.CallOption) (*CreateResponse, error) {
	return c.client.Create(ctx, req, opts...)
}
func (c *clientConn) Start(ctx context.Context, req *StartRequest, opts ...grpc.CallOption) (*StartResponse, error) {
	return c.client.Start(ctx, req, opts...)
}
func (c *clientConn) Stop(ctx context.Context, req *StopRequest, opts ...grpc.CallOption) (*StopResponse, error) {
	return c.client.Stop(ctx, req, opts...)
}
func (c *clientConn) Delete(ctx context.Context, req *DeleteRequest, opts ...grpc.CallOption) (*DeleteResponse, error) {
	return c.client.Delete(ctx, req, opts...)
}
func (c *clientConn) Reboot(ctx context.Context, req *RebootRequest, opts ...grpc.CallOption) (*RebootResponse, error) {
	return c.client.Reboot(ctx, req, opts...)
}
func (c *clientConn) Migrate(ctx context.Context, req *MigrateRequest, opts ...grpc.CallOption) (*MigrateResponse, error) {
	return c.client.Migrate(ctx, req, opts...)
}
func (c *clientConn) Usage(ctx context.Context, req *UsageRequest, opts ...grpc.CallOption) (*UsageResponse, error) {
	return c.client.Usage(ctx, req, opts...)
}
func (c *clientConn) Validate(ctx context.Context, req *ValidateRequest, opts ...grpc.CallOption) (*ValidateResponse, error) {
	return c.client.Validate(ctx, req, opts...)
}
func (c *clientConn) Close() error { return c.conn.Close() }

type ComputeNodeAgentServer interface {
	Create(context.Context, *CreateRequest) (*CreateResponse, error)
	Start(context.Context, *StartRequest) (*StartResponse, error)
	Stop(context.Context, *StopRequest) (*StopResponse, error)
	Delete(context.Context, *DeleteRequest) (*DeleteResponse, error)
	Reboot(context.Context, *RebootRequest) (*RebootResponse, error)
	Migrate(context.Context, *MigrateRequest) (*MigrateResponse, error)
	Usage(context.Context, *UsageRequest) (*UsageResponse, error)
	Validate(context.Context, *ValidateRequest) (*ValidateResponse, error)
}

func RegisterServer(s *grpc.Server, impl ComputeNodeAgentServer) {
	registerComputeNodeAgentServer(s, impl)
}
```

---

## Agent Server: `internal/agent/server.go`

```go
package agent

import (
	"context"
	"fmt"
	"sync"

	"github.com/cinvat/arsan/internal/core"
	"google.golang.org/grpc"
)

type AgentServer struct {
	mu       sync.Mutex
	driver   core.Driver
	nodeID   string
	name     string
	address  string
	arch     string
	total    Resources
	stopped  bool
}

func NewAgentServer(nodeID, name, address, arch string, total Resources, drv core.Driver) *AgentServer {
	return &AgentServer{driver: drv, nodeID: nodeID, name: name, address: address, arch: arch, total: total}
}

func (s *AgentServer) Create(ctx context.Context, req *CreateRequest) (*CreateResponse, error) {
	s.mu.Lock(); defer s.mu.Unlock()
	if s.stopped { return &CreateResponse{Success: false, Message: "agent stopped"}, nil }
	info, err := s.driver.Create(ctx, core.CreateRequest{
		InstanceID: req.InstanceID, NodeID: req.NodeID, Name: req.Name,
		Flavor: toCoreFlavor(req.Flavor), ImageRef: req.ImageRef,
	})
	if err != nil { return &CreateResponse{Success: false, Message: err.Error()}, nil }
	return &CreateResponse{Success: true, Message: "created", Info: &InstanceInfo{
		ID: info.ID, NodeID: info.NodeID, Driver: info.Driver, Started: info.Started,
	}}, nil
}

func (s *AgentServer) Start(ctx context.Context, req *StartRequest) (*StartResponse, error) {
	s.mu.Lock(); defer s.mu.Unlock()
	if s.stopped { return &StartResponse{Success: false, Message: "agent stopped"}, nil }
	if err := s.driver.Start(ctx, req.InstanceID); err != nil {
		return &StartResponse{Success: false, Message: err.Error()}, nil
	}
	return &StartResponse{Success: true, Message: "started"}, nil
}

func (s *AgentServer) Stop(ctx context.Context, req *StopRequest) (*StopResponse, error) {
	s.mu.Lock(); defer s.mu.Unlock()
	if s.stopped { return &StopResponse{Success: false, Message: "agent stopped"}, nil }
	if err := s.driver.Stop(ctx, req.InstanceID); err != nil {
		return &StopResponse{Success: false, Message: err.Error()}, nil
	}
	return &StopResponse{Success: true, Message: "stopped"}, nil
}

func (s *AgentServer) Delete(ctx context.Context, req *DeleteRequest) (*DeleteResponse, error) {
	s.mu.Lock(); defer s.mu.Unlock()
	if s.stopped { return &DeleteResponse{Success: false, Message: "agent stopped"}, nil }
	if err := s.driver.Delete(ctx, req.InstanceID); err != nil {
		return &DeleteResponse{Success: false, Message: err.Error()}, nil
	}
	return &DeleteResponse{Success: true, Message: "deleted"}, nil
}

func (s *AgentServer) Reboot(ctx context.Context, req *RebootRequest) (*RebootResponse, error) {
	s.mu.Lock(); defer s.mu.Unlock()
	if s.stopped { return &RebootResponse{Success: false, Message: "agent stopped"}, nil }
	if err := s.driver.Reboot(ctx, req.InstanceID); err != nil {
		return &RebootResponse{Success: false, Message: err.Error()}, nil
	}
	return &RebootResponse{Success: true, Message: "rebooted"}, nil
}

func (s *AgentServer) Migrate(ctx context.Context, req *MigrateRequest) (*MigrateResponse, error) {
	s.mu.Lock(); defer s.mu.Unlock()
	if s.stopped { return &MigrateResponse{Success: false, Message: "agent stopped"}, nil }
	if err := s.driver.Migrate(ctx, req.InstanceID, req.TargetNodeID); err != nil {
		return &MigrateResponse{Success: false, Message: err.Error()}, nil
	}
	return &MigrateResponse{Success: true, Message: "migrated"}, nil
}

func (s *AgentServer) Usage(ctx context.Context, req *UsageRequest) (*UsageResponse, error) {
	s.mu.Lock(); defer s.mu.Unlock()
	if s.stopped { return &UsageResponse{Success: false, Message: "agent stopped"}, nil }
	usage, err := s.driver.Usage(ctx)
	if err != nil { return &UsageResponse{Success: false, Message: err.Error()}, nil }
	return &UsageResponse{Success: true, Message: "ok", Usage: fromCoreResources(usage)}, nil
}

func (s *AgentServer) Validate(ctx context.Context, req *ValidateRequest) (*ValidateResponse, error) {
	s.mu.Lock(); defer s.mu.Unlock()
	if s.stopped { return &ValidateResponse{CanRun: false, Message: "agent stopped"}, nil }
	err := s.driver.Validate(ctx, toCoreFlavor(req.Flavor))
	if err != nil { return &ValidateResponse{CanRun: false, Message: err.Error()}, nil }
	return &ValidateResponse{CanRun: true, Message: "ok"}, nil
}

func (s *AgentServer) StopAgent() {
	s.mu.Lock(); defer s.mu.Unlock()
	s.stopped = true
}

// Conversion helpers
func fromCoreResources(r core.Resources) Resources {
	return Resources{VCPU: int32(r.VCPU), RAM: int32(r.RAM), Disk: int32(r.Disk)}
}
func toCoreResources(r Resources) core.Resources {
	return core.Resources{VCPU: int(r.VCPU), RAM: int(r.RAM), Disk: int(r.Disk)}
}
func fromCoreFlavor(f core.Flavor) Flavor {
	return Flavor{Name: f.Name, VCPU: int32(f.VCPU), RAM: int32(f.RAM), Disk: int32(f.Disk)}
}
func toCoreFlavor(f Flavor) core.Flavor {
	return core.Flavor{Name: f.Name, VCPU: int(f.VCPU), RAM: int(f.RAM), Disk: int(f.Disk)}
}
```

---

## CLI Command: `cmd/agent.go`

```go
package cmd

import (
	"context"
	"fmt"
	"log"
	"net"

	"github.com/cinvat/arsan/internal/agent"
	"github.com/cinvat/arsan/internal/core"
	"github.com/cinvat/arsan/internal/driver"
	"github.com/cinvat/arsan/internal/driver/fake"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

var agentCmd = &cobra.Command{
	Use:   "agent",
	Short: "Run the compute node gRPC agent",
	RunE: func(cmd *cobra.Command, _ []string) error { return runAgent(cmd.Context()) },
}

func init() { rootCmd.AddCommand(agentCmd) }

func runAgent(ctx context.Context) error {
	v := viper.GetViper()

	// The node's identity is the machine, so it defaults to the hostname --
	// like a kubelet reporting the node it runs on. An operator should not have
	// to invent a name and keep it consistent across restarts.
	name := v.GetString("node.name")
	if name == "" {
		host, err := os.Hostname()
		if err != nil {
			return fmt.Errorf("node.name is required and the hostname is unavailable: %w", err)
		}
		name = host
	}

	arch := v.GetString("node.arch")
	vcpu := v.GetInt("node.vcpu")
	ram := v.GetInt("node.ram")
	disk := v.GetInt("node.disk")
	driverName := v.GetString("driver")
	listen := v.GetString("agent.addr")

	total := core.Resources{VCPU: vcpu, RAM: ram, Disk: disk}
	var drv core.Driver
	switch driverName {
	case "fake": drv = fake.New()
	case "kvm":
		d, err := driver.New("kvm")
		if err != nil { return fmt.Errorf("kvm driver: %w", err) }
		drv = d
	default: return fmt.Errorf("unknown driver %q", driverName)
	}

	agentSrv := agent.NewAgentServer(nodeID, name, address, "amd64", agent.FromCoreResources(total), drv)
	grpcServer := grpc.NewServer()
	agent.RegisterServer(grpcServer, agentSrv)

	lis, err := net.Listen("tcp", listen)
	if err != nil { return fmt.Errorf("listen %s: %w", listen, err) }

	go func() {
		<-ctx.Done()
		grpcServer.GracefulStop()
		agentSrv.StopAgent()
	}()

	log.Printf("agent starting on %s (node %s)", listen, nodeID)
	return grpcServer.Serve(lis)
}
```

---

## Run the Agent

```bash
# Terminal 1: the api, which holds the bootstrap token
./arsan api --addr=:8080 --bus.url=nats://127.0.0.1:4222

# Terminal 2: start the agent
#
# The agent registers itself over REST and reports where it can be dialled:
#   --node.advertise   its own gRPC address, sent on every bootstrap
#   --driver           the engine it will actually run VMs with
# There is no --node.id: the agent is assigned one, and re-bootstrapping is how
# it moves to a new port.
echo "$ARSAN_TOKEN" > /tmp/arsan-token && chmod 600 /tmp/arsan-token
./arsan agent \
  --api.url=http://127.0.0.1:8080 \
  --node.advertise=127.0.0.1:9090 \
  --node.vcpu=8 --node.ram=16 --node.disk=200 \
  --driver=fake --agent.addr=:9090 \
  --bootstrap-token-file=/tmp/arsan-token

# Terminal 2: Test with grpcurl
grpcurl -plaintext -d '{"instance_id":"i-1","node_id":"n1","name":"test","flavor":{"name":"m1.small","vcpu":1,"ram":2,"disk":10},"image_ref":"cirros"}' \
  127.0.0.1:9090 arsan.agent.ComputeNode/Create
```

---

## Next

Step 8: Controller REST API