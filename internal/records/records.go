// Package records is the api's CRUD layer.
//
// It exists so the api has no dependency on the controller's services. Two
// types, not one: core.InstanceService and core.NodeService both declare
// Get(ctx, id) and List(ctx) with different result types, so no single type can
// satisfy both.
//
// Nothing here decides anything. Records are validated, written, and read;
// anything that requires a judgement — where a VM runs, whether a transition is
// legal, whether a node is healthy — is published to the command bus for the
// controller. The one exception is instance creation, because a PENDING row is
// a fact about the request, not a decision: it records that the user asked.
package records

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/cinvat/arsan/internal/bus"
	"github.com/cinvat/arsan/internal/core"
	"github.com/cinvat/arsan/pkg/util"
)

// Nodes implements core.NodeService over a node store.
type Nodes struct {
	Store core.NodeStore
}

// Register upserts a node. Only the bootstrap endpoint reaches it, and that
// endpoint authenticates before calling.
func (s *Nodes) Register(ctx context.Context, req core.RegisterNodeRequest) (core.Node, error) {
	if req.Name == "" {
		return core.Node{}, fmt.Errorf("name required: %w", core.ErrInvalidRequest)
	}
	return s.Store.Register(ctx, core.Node{
		ID:      NodeID(req.Name),
		Name:    req.Name,
		Enabled: true,
	})
}

// NodeID derives a node's stable ID from its name.
//
// "Compute Node 1" and "compute-node-1" must resolve to the same node, or one
// host registers twice and its reservations are split across two records.
func NodeID(name string) string { return "n-" + util.Slug(name) }

// RecordUsage stores what an agent reports it is using. It does not change
// readiness: that is the controller's probe, and nothing here may promote a node
// the probe has not cleared.
func (s *Nodes) RecordUsage(ctx context.Context, nodeID string, usage core.Resources) error {
	_, err := s.Store.RecordUsage(ctx, nodeID, usage, time.Now().UTC())
	return err
}

func (s *Nodes) Get(ctx context.Context, id string) (core.Node, error) {
	return s.Store.GetNode(ctx, id)
}

func (s *Nodes) List(ctx context.Context) ([]core.Node, error) {
	return s.Store.ListNodes(ctx)
}

// Enable returns a node to service. Readiness itself is decided by the next
// probe, not here: re-enabling a node that is still unreachable would put it
// back in the scheduler's eligible set on the strength of an operator's intent.
func (s *Nodes) Enable(ctx context.Context, id string) error {
	_, err := s.Store.SetNodeEnabled(ctx, id, true)
	return err
}

// Disable takes a node out of service. It stops receiving new VMs immediately
// and no probe may undo it. The reason names the operator, so a later probe
// failure cannot misdescribe their decision as a broken agent.
func (s *Nodes) Disable(ctx context.Context, id string) error {
	if _, err := s.Store.SetNodeEnabled(ctx, id, false); err != nil {
		return err
	}
	_, err := s.Store.SetReadiness(ctx, id, core.NodeNotReady, "disabled by operator")
	return err
}

// Instances implements core.InstanceService over an instance store and the bus.
type Instances struct {
	Store core.InstanceStore
	Bus   bus.Bus
}

// Create validates, persists a PENDING instance, and asks the controller to
// build it.
//
// The PENDING row is not a decision. It records that a user asked for a VM;
// whether and where it runs is the controller's answer. If the publish fails
// the record stays PENDING and the reconciliation sweep picks it up, so the
// caller is told the truth either way.
func (s *Instances) Create(ctx context.Context, req core.CreateInstanceRequest) (core.Instance, error) {
	if req.Name == "" {
		return core.Instance{}, fmt.Errorf("name required: %w", core.ErrInvalidRequest)
	}
	if req.ImageRef == "" {
		return core.Instance{}, fmt.Errorf("image_ref required: %w", core.ErrInvalidRequest)
	}
	if req.Flavor.VCPU <= 0 || req.Flavor.RAM <= 0 || req.Flavor.Disk <= 0 {
		return core.Instance{}, fmt.Errorf("flavor must have positive vcpu, ram and disk: %w", core.ErrInvalidRequest)
	}

	now := time.Now().UTC()
	inst := core.Instance{
		ID:        util.NewID("i"),
		Name:      req.Name,
		Flavor:    req.Flavor,
		ImageRef:  req.ImageRef,
		State:     core.StatePending,
		CreatedAt: now,
		UpdatedAt: now,
	}
	created, err := s.Store.Create(ctx, inst)
	if err != nil {
		return core.Instance{}, err
	}

	if err := s.Bus.Publish(ctx, bus.Command{
		ID:     util.NewID("cmd"),
		Topic:  bus.TopicInstanceCreate,
		Target: created.ID,
	}); err != nil {
		// The record exists but nothing has been asked to act on it. Report
		// the failure rather than a 202 for work nobody was asked to do; the
		// sweep will retry.
		return core.Instance{}, fmt.Errorf("enqueue %s: %w", bus.TopicInstanceCreate, err)
	}
	return created, nil
}

func (s *Instances) Get(ctx context.Context, id string) (core.Instance, error) {
	return s.Store.Get(ctx, id)
}

func (s *Instances) List(ctx context.Context) ([]core.Instance, error) {
	return s.Store.List(ctx)
}

// Delete publishes a command rather than removing the row. The record has to
// outlive the request so the controller can tear the VM down and release its
// reservation; deleting it here would strand both.
func (s *Instances) Delete(ctx context.Context, id string) error {
	return s.command(ctx, bus.TopicInstanceDelete, id)
}

// Start and Stop publish commands for the same reason: the api does not decide
// whether an instance is in a state that permits the change. The controller's
// compare-and-swap rejects the command if it is not.
func (s *Instances) Start(ctx context.Context, id string) error {
	return s.command(ctx, bus.TopicInstanceStart, id)
}

func (s *Instances) Stop(ctx context.Context, id string) error {
	return s.command(ctx, bus.TopicInstanceStop, id)
}

func (s *Instances) Migrate(ctx context.Context, id, targetNodeID string) error {
	if targetNodeID == "" {
		return fmt.Errorf("target_node_id required: %w", core.ErrInvalidRequest)
	}
	// The target travels in the payload: the command carries the target it
	// concerns plus its own data, never a snapshot of the record.
	payload, err := json.Marshal(map[string]string{"target_node_id": targetNodeID})
	if err != nil {
		return err
	}
	return s.publish(ctx, bus.TopicInstanceMigrate, id, payload)
}

func (s *Instances) command(ctx context.Context, topic, target string) error {
	return s.publish(ctx, topic, target, nil)
}

func (s *Instances) publish(ctx context.Context, topic, target string, payload []byte) error {
	if _, err := s.Store.Get(ctx, target); err != nil {
		// 404 before publishing, so a client does not get a 202 for a
		// command that can only fail.
		return err
	}
	if err := s.Bus.Publish(ctx, bus.Command{
		ID:      util.NewID("cmd"),
		Topic:   topic,
		Target:  target,
		Payload: payload,
	}); err != nil {
		return fmt.Errorf("enqueue %s: %w", topic, err)
	}
	return nil
}

var (
	_ core.NodeService     = (*Nodes)(nil)
	_ core.InstanceService = (*Instances)(nil)
)

// ErrUnimplemented marks a CRUD method the api deliberately does not perform.
// It is never returned by anything the api exposes.
var ErrUnimplemented = errors.New("records: not implemented by the api")
