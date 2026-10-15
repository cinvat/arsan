// Package agent implements the compute node agent: it registers itself with
// the control plane and serves engine calls over gRPC.
package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/cinvat/arsan/internal/core"
)

// BootstrapRequest is what an agent posts to register itself.
//
// A bootstrap carries identity and nothing else. Capacity, engine and usage
// are fetched by the controller after the probe succeeds, so the agent never
// has to describe a host it may not even know yet.
type BootstrapRequest struct {
	Token string `json:"token"`
	Name  string `json:"name"`
}

// BootstrapError describes a rejected registration.
//
// The api's body is not read for its error text beyond the status, because a
// bootstrap failure is almost always "this token is not the one the api holds"
// and the useful message is that, not whatever the server chose to say.
type BootstrapError struct {
	StatusCode int
	Body       string
}

func (e *BootstrapError) Error() string {
	if e.StatusCode == http.StatusUnauthorized {
		return fmt.Sprintf("bootstrap rejected: token not accepted (%d)", e.StatusCode)
	}
	return fmt.Sprintf("bootstrap failed: status %d: %s", e.StatusCode, e.Body)
}

// Unauthenticated reports whether the token was the problem, as opposed to the
// api being unreachable or broken. Those need different responses: a bad token
// will never fix itself, while an unreachable api usually will.
func (e *BootstrapError) Unauthenticated() bool {
	return e.StatusCode == http.StatusUnauthorized
}

// Register posts a bootstrap request to the api and returns the stored node.
//
// It is a single request with no session: the response is the whole exchange,
// and everything after is the controller dialling this agent.
func Register(ctx context.Context, apiURL, apiVersion, token string, req BootstrapRequest) (core.Node, error) {
	req.Token = token

	body, err := json.Marshal(req)
	if err != nil {
		return core.Node{}, fmt.Errorf("agent: encode bootstrap: %w", err)
	}

	endpoint := fmt.Sprintf("%s/%s/nodes", trimSlash(apiURL), apiVersion)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return core.Node{}, fmt.Errorf("agent: build bootstrap request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	httpResp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		return core.Node{}, fmt.Errorf("agent: bootstrap request: %w", err)
	}
	defer httpResp.Body.Close()

	// Bound the read: a 2xx with an unbounded body would let a
	// misconfigured api stream forever.
	raw, err := io.ReadAll(io.LimitReader(httpResp.Body, 1<<20))
	if err != nil {
		return core.Node{}, fmt.Errorf("agent: read bootstrap response: %w", err)
	}

	if httpResp.StatusCode != http.StatusCreated && httpResp.StatusCode != http.StatusOK {
		return core.Node{}, &BootstrapError{StatusCode: httpResp.StatusCode, Body: string(raw)}
	}

	var node core.Node
	if err := json.Unmarshal(raw, &node); err != nil {
		return core.Node{}, fmt.Errorf("agent: decode bootstrap response: %w", err)
	}
	return node, nil
}

// RetryBootstrap registers the agent, retrying while the api looks
// unreachable.
//
// A refused token is NOT retried: it will not become valid by waiting, and an
// agent looping on a 401 looks alive while being permanently unable to join.
// An unreachable api is retried, because that is a startup ordering problem
// and it usually resolves in seconds.
//
// It returns the node on success, or an error the caller should exit on.
func RetryBootstrap(ctx context.Context, client BootstrapFunc, attempts int, backoff time.Duration) (core.Node, error) {
	var lastErr error
	for i := range attempts {
		if i > 0 {
			select {
			case <-ctx.Done():
				return core.Node{}, ctx.Err()
			case <-time.After(backoff):
			}
		}

		node, err := client(ctx)
		if err == nil {
			return node, nil
		}
		lastErr = err

		var be *BootstrapError
		if errors.As(err, &be) && be.Unauthenticated() {
			return core.Node{}, err
		}
	}
	return core.Node{}, fmt.Errorf("agent: bootstrap did not succeed after %d attempts: %w", attempts, lastErr)
}

// BootstrapFunc is one bootstrap attempt, so RetryBootstrap can be tested
// without a server.
type BootstrapFunc func(ctx context.Context) (core.Node, error)

func trimSlash(s string) string {
	for len(s) > 0 && s[len(s)-1] == '/' {
		s = s[:len(s)-1]
	}
	return s
}
