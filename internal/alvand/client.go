// Package alvand is arsan's HTTP client for alvand's logical port API.
//
// It is consumed by the controller, never the api: port binding is a placement
// decision, and the api stays CRUD. Only the verbs compute needs are modelled;
// the client is not a general-purpose alvand SDK.
package alvand

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/cinvat/arsan/internal/core"
)

// apiPrefix mirrors alvand's aggregation mount. Using it verbatim means the
// same client works directly against alvand or through kloudapi.
const apiPrefix = "/apis/alvand/v1alpha1/logicalports"

type Client struct {
	base string
	http *http.Client
}

func New(baseURL string) *Client {
	return &Client{
		base: strings.TrimRight(baseURL, "/"),
		http: &http.Client{Timeout: 10 * time.Second},
	}
}

// portObject is the subset of alvand's LogicalPort envelope arsan needs.
type portObject struct {
	Status struct {
		ID             string `json:"id"`
		MAC            string `json:"mac"`
		IP             string `json:"ip"`
		OVNLogicalPort string `json:"ovnLogicalPort"`
		Binding        struct {
			VIFType string `json:"vifType"`
			Bridge  string `json:"bridge"`
		} `json:"binding"`
	} `json:"status"`
}

func (p portObject) interfaceOf(portID string) core.LogicalSwitchInterface {
	out := core.LogicalSwitchInterface{
		PortID:         portID,
		MAC:            p.Status.MAC,
		IP:             p.Status.IP,
		OVNLogicalPort: p.Status.OVNLogicalPort,
		VIFType:        p.Status.Binding.VIFType,
		Bridge:         p.Status.Binding.Bridge,
	}
	if out.OVNLogicalPort == "" {
		out.OVNLogicalPort = p.Status.ID
	}
	if out.OVNLogicalPort == "" {
		out.OVNLogicalPort = portID
	}
	if out.VIFType == "" {
		out.VIFType = "ovs"
	}
	if out.Bridge == "" {
		out.Bridge = "br-int"
	}
	return out
}

// Attach binds the port to an instance on a chassis and returns the resolved
// VIF details. It is idempotent on alvand's side, so retries are safe.
func (c *Client) Attach(ctx context.Context, portID, instanceRef, device, bindingHost string) (core.LogicalSwitchInterface, error) {
	body, err := json.Marshal(map[string]string{
		"instanceRef": instanceRef,
		"device":      device,
		"bindingHost": bindingHost,
	})
	if err != nil {
		return core.LogicalSwitchInterface{}, err
	}
	var out portObject
	if err := c.do(ctx, http.MethodPost, c.portURL(portID)+"/attach", body, &out); err != nil {
		return core.LogicalSwitchInterface{}, fmt.Errorf("attach port %s: %w", portID, err)
	}
	return out.interfaceOf(portID), nil
}

// Detach releases the port. A port that is already gone is success: the VM is
// being deleted and the row may have been removed first.
func (c *Client) Detach(ctx context.Context, portID string) error {
	err := c.do(ctx, http.MethodPost, c.portURL(portID)+"/detach", nil, nil)
	if err != nil && isNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("detach port %s: %w", portID, err)
	}
	return nil
}

func (c *Client) portURL(portID string) string {
	return c.base + apiPrefix + "/" + url.PathEscape(portID)
}

func (c *Client) do(ctx context.Context, method, endpoint string, body []byte, out any) error {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint, reader)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return &statusError{code: resp.StatusCode, body: strings.TrimSpace(string(raw))}
	}
	if out == nil {
		return nil
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("decode alvand response: %w", err)
	}
	return nil
}

type statusError struct {
	code int
	body string
}

func (e *statusError) Error() string {
	if e.body == "" {
		return fmt.Sprintf("alvand: status %d", e.code)
	}
	return fmt.Sprintf("alvand: status %d: %s", e.code, e.body)
}

func isNotFound(err error) bool {
	se, ok := err.(*statusError)
	return ok && se.code == http.StatusNotFound
}
