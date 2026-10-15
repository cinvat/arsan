package agent

import (
	"context"

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
	registerCodec()
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.CallContentSubtype(jsonCodec{}.Name())),
	)
	if err != nil {
		return nil, err
	}
	return &clientConn{conn: conn}, nil
}

type clientConn struct {
	conn *grpc.ClientConn
}

func (c *clientConn) invoke(ctx context.Context, method string, req, resp any, opts ...grpc.CallOption) error {
	return c.conn.Invoke(ctx, "/agent.ComputeNodeAgent/"+method, req, resp, opts...)
}

func (c *clientConn) Create(ctx context.Context, req *CreateRequest, opts ...grpc.CallOption) (*CreateResponse, error) {
	var resp CreateResponse
	return &resp, c.invoke(ctx, "Create", req, &resp, opts...)
}
func (c *clientConn) Start(ctx context.Context, req *StartRequest, opts ...grpc.CallOption) (*StartResponse, error) {
	var resp StartResponse
	return &resp, c.invoke(ctx, "Start", req, &resp, opts...)
}
func (c *clientConn) Stop(ctx context.Context, req *StopRequest, opts ...grpc.CallOption) (*StopResponse, error) {
	var resp StopResponse
	return &resp, c.invoke(ctx, "Stop", req, &resp, opts...)
}
func (c *clientConn) Delete(ctx context.Context, req *DeleteRequest, opts ...grpc.CallOption) (*DeleteResponse, error) {
	var resp DeleteResponse
	return &resp, c.invoke(ctx, "Delete", req, &resp, opts...)
}
func (c *clientConn) Reboot(ctx context.Context, req *RebootRequest, opts ...grpc.CallOption) (*RebootResponse, error) {
	var resp RebootResponse
	return &resp, c.invoke(ctx, "Reboot", req, &resp, opts...)
}
func (c *clientConn) Migrate(ctx context.Context, req *MigrateRequest, opts ...grpc.CallOption) (*MigrateResponse, error) {
	var resp MigrateResponse
	return &resp, c.invoke(ctx, "Migrate", req, &resp, opts...)
}
func (c *clientConn) Usage(ctx context.Context, req *UsageRequest, opts ...grpc.CallOption) (*UsageResponse, error) {
	var resp UsageResponse
	return &resp, c.invoke(ctx, "Usage", req, &resp, opts...)
}
func (c *clientConn) Validate(ctx context.Context, req *ValidateRequest, opts ...grpc.CallOption) (*ValidateResponse, error) {
	var resp ValidateResponse
	return &resp, c.invoke(ctx, "Validate", req, &resp, opts...)
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
	registerCodec()
	s.RegisterService(&_ComputeNodeAgent_serviceDesc, impl)
}

// handler builds the gRPC method handler for one engine operation. The eight
// operations differ only in request type and method, so they share this
// instead of eight copies of the decode/dispatch boilerplate.
func handler[Req any, Resp any](method string, newReq func() *Req, call func(ComputeNodeAgentServer, context.Context, *Req) (*Resp, error)) grpc.MethodDesc {
	return grpc.MethodDesc{
		MethodName: method,
		Handler: func(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
			in := newReq()
			if err := dec(in); err != nil {
				return nil, err
			}
			svc := srv.(ComputeNodeAgentServer)
			if interceptor == nil {
				return call(svc, ctx, in)
			}
			invoke := func(ctx context.Context, req interface{}) (interface{}, error) {
				return call(svc, ctx, req.(*Req))
			}
			return interceptor(ctx, in, &grpc.UnaryServerInfo{
				Server:     srv,
				FullMethod: "/agent.ComputeNodeAgent/" + method,
			}, invoke)
		},
	}
}

var _ComputeNodeAgent_serviceDesc = grpc.ServiceDesc{
	ServiceName: "agent.ComputeNodeAgent",
	HandlerType: (*ComputeNodeAgentServer)(nil),
	Methods: []grpc.MethodDesc{
		handler("Create", func() *CreateRequest { return new(CreateRequest) }, ComputeNodeAgentServer.Create),
		handler("Start", func() *StartRequest { return new(StartRequest) }, ComputeNodeAgentServer.Start),
		handler("Stop", func() *StopRequest { return new(StopRequest) }, ComputeNodeAgentServer.Stop),
		handler("Delete", func() *DeleteRequest { return new(DeleteRequest) }, ComputeNodeAgentServer.Delete),
		handler("Reboot", func() *RebootRequest { return new(RebootRequest) }, ComputeNodeAgentServer.Reboot),
		handler("Migrate", func() *MigrateRequest { return new(MigrateRequest) }, ComputeNodeAgentServer.Migrate),
		handler("Usage", func() *UsageRequest { return new(UsageRequest) }, ComputeNodeAgentServer.Usage),
		handler("Validate", func() *ValidateRequest { return new(ValidateRequest) }, ComputeNodeAgentServer.Validate),
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "agent.proto",
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
