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
	if err != nil {
		return nil, err
	}
	return &clientConn{conn: conn}, nil
}

type clientConn struct {
	conn *grpc.ClientConn
}

func (c *clientConn) Create(ctx context.Context, req *CreateRequest, opts ...grpc.CallOption) (*CreateResponse, error) {
	var resp CreateResponse
	err := c.conn.Invoke(ctx, "/agent.ComputeNodeAgent/Create", req, &resp, opts...)
	return &resp, err
}
func (c *clientConn) Start(ctx context.Context, req *StartRequest, opts ...grpc.CallOption) (*StartResponse, error) {
	var resp StartResponse
	err := c.conn.Invoke(ctx, "/agent.ComputeNodeAgent/Start", req, &resp, opts...)
	return &resp, err
}
func (c *clientConn) Stop(ctx context.Context, req *StopRequest, opts ...grpc.CallOption) (*StopResponse, error) {
	var resp StopResponse
	err := c.conn.Invoke(ctx, "/agent.ComputeNodeAgent/Stop", req, &resp, opts...)
	return &resp, err
}
func (c *clientConn) Delete(ctx context.Context, req *DeleteRequest, opts ...grpc.CallOption) (*DeleteResponse, error) {
	var resp DeleteResponse
	err := c.conn.Invoke(ctx, "/agent.ComputeNodeAgent/Delete", req, &resp, opts...)
	return &resp, err
}
func (c *clientConn) Reboot(ctx context.Context, req *RebootRequest, opts ...grpc.CallOption) (*RebootResponse, error) {
	var resp RebootResponse
	err := c.conn.Invoke(ctx, "/agent.ComputeNodeAgent/Reboot", req, &resp, opts...)
	return &resp, err
}
func (c *clientConn) Migrate(ctx context.Context, req *MigrateRequest, opts ...grpc.CallOption) (*MigrateResponse, error) {
	var resp MigrateResponse
	err := c.conn.Invoke(ctx, "/agent.ComputeNodeAgent/Migrate", req, &resp, opts...)
	return &resp, err
}
func (c *clientConn) Usage(ctx context.Context, req *UsageRequest, opts ...grpc.CallOption) (*UsageResponse, error) {
	var resp UsageResponse
	err := c.conn.Invoke(ctx, "/agent.ComputeNodeAgent/Usage", req, &resp, opts...)
	return &resp, err
}
func (c *clientConn) Validate(ctx context.Context, req *ValidateRequest, opts ...grpc.CallOption) (*ValidateResponse, error) {
	var resp ValidateResponse
	err := c.conn.Invoke(ctx, "/agent.ComputeNodeAgent/Validate", req, &resp, opts...)
	return &resp, err
}
func (c *clientConn) Close() error {
	return c.conn.Close()
}

type ComputeNodeAgentServer interface {
	// Registration and liveness are NOT here: an agent bootstraps over REST
	// and the controller observes the gRPC connection. This service is
	// engine operations only.
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
	s.RegisterService(&_ComputeNodeAgent_serviceDesc, impl)
}

var _ComputeNodeAgent_serviceDesc = grpc.ServiceDesc{
	ServiceName: "agent.ComputeNodeAgent",
	HandlerType: (*ComputeNodeAgentServer)(nil),
	Methods: []grpc.MethodDesc{
		{MethodName: "Create", Handler: _Create_Handler},
		{MethodName: "Start", Handler: _Start_Handler},
		{MethodName: "Stop", Handler: _Stop_Handler},
		{MethodName: "Delete", Handler: _Delete_Handler},
		{MethodName: "Reboot", Handler: _Reboot_Handler},
		{MethodName: "Migrate", Handler: _Migrate_Handler},
		{MethodName: "Usage", Handler: _Usage_Handler},
		{MethodName: "Validate", Handler: _Validate_Handler},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "agent.proto",
}

func _Create_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CreateRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ComputeNodeAgentServer).Create(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/agent.ComputeNodeAgent/Create"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ComputeNodeAgentServer).Create(ctx, req.(*CreateRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Start_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(StartRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ComputeNodeAgentServer).Start(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/agent.ComputeNodeAgent/Start"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ComputeNodeAgentServer).Start(ctx, req.(*StartRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Stop_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(StopRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ComputeNodeAgentServer).Stop(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/agent.ComputeNodeAgent/Stop"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ComputeNodeAgentServer).Stop(ctx, req.(*StopRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Delete_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DeleteRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ComputeNodeAgentServer).Delete(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/agent.ComputeNodeAgent/Delete"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ComputeNodeAgentServer).Delete(ctx, req.(*DeleteRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Reboot_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(RebootRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ComputeNodeAgentServer).Reboot(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/agent.ComputeNodeAgent/Reboot"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ComputeNodeAgentServer).Reboot(ctx, req.(*RebootRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Migrate_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(MigrateRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ComputeNodeAgentServer).Migrate(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/agent.ComputeNodeAgent/Migrate"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ComputeNodeAgentServer).Migrate(ctx, req.(*MigrateRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Usage_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(UsageRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ComputeNodeAgentServer).Usage(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/agent.ComputeNodeAgent/Usage"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ComputeNodeAgentServer).Usage(ctx, req.(*UsageRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Validate_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ValidateRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ComputeNodeAgentServer).Validate(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/agent.ComputeNodeAgent/Validate"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ComputeNodeAgentServer).Validate(ctx, req.(*ValidateRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// Messages

type CreateRequest struct {
	InstanceID string
	NodeID     string
	Name       string
	Flavor     Flavor
	ImageRef   string
}

type CreateResponse struct {
	Success bool
	Message string
	Info    *InstanceInfo
}

type StartRequest struct {
	InstanceID string
}

type StartResponse struct {
	Success bool
	Message string
}

type StopRequest struct {
	InstanceID string
}

type StopResponse struct {
	Success bool
	Message string
}

type DeleteRequest struct {
	InstanceID string
}

type DeleteResponse struct {
	Success bool
	Message string
}

type RebootRequest struct {
	InstanceID string
}

type RebootResponse struct {
	Success bool
	Message string
}

type MigrateRequest struct {
	InstanceID   string
	TargetNodeID string
}

type MigrateResponse struct {
	Success bool
	Message string
}

type UsageRequest struct{}

type UsageResponse struct {
	Usage   Resources
	Success bool
	Message string
}

type ValidateRequest struct {
	Flavor Flavor
}

type ValidateResponse struct {
	CanRun  bool
	Message string
}

type InstanceInfo struct {
	ID      string
	NodeID  string
	Driver  string
	Started bool
}

type Flavor struct {
	Name string
	VCPU int32
	RAM  int32
	Disk int32
}

type Resources struct {
	VCPU int32
	RAM  int32
	Disk int32
}

func FromCoreResources(r core.Resources) Resources {
	return Resources{VCPU: int32(r.VCPU), RAM: int32(r.RAM), Disk: int32(r.Disk)}
}

func ToCoreResources(r Resources) core.Resources {
	return core.Resources{VCPU: int(r.VCPU), RAM: int(r.RAM), Disk: int(r.Disk)}
}

func FromCoreFlavor(f core.Flavor) Flavor {
	return Flavor{Name: f.Name, VCPU: int32(f.VCPU), RAM: int32(f.RAM), Disk: int32(f.Disk)}
}

func ToCoreFlavor(f Flavor) core.Flavor {
	return core.Flavor{Name: f.Name, VCPU: int(f.VCPU), RAM: int(f.RAM), Disk: int(f.Disk)}
}

func WithTimeout(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, d)
}
