package cmd

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"golang.org/x/net/http2"
)

// Resources mirrors the api's node shape: nodes have CPU, VMs have VCPU.
// Field names match the server's JSON.
type Resources struct {
	CPU  int `json:"CPU"`
	RAM  int `json:"RAM"`
	Disk int `json:"Disk"`
}

// VCPU mirrors the api's topology object. It accepts a mapping or a legacy
// bare count, in JSON and YAML alike.
type VCPU struct {
	Sockets   int  `json:"sockets" yaml:"sockets"`
	Cores     int  `json:"cores" yaml:"cores"`
	Threads   int  `json:"threads" yaml:"threads"`
	Dedicated bool `json:"dedicated" yaml:"dedicated"`
}

// Count is the schedulable number of vCPUs.
func (v VCPU) Count() int { return v.Sockets * v.Cores * v.Threads }

func (v *VCPU) UnmarshalJSON(raw []byte) error {
	var n int
	if err := json.Unmarshal(raw, &n); err == nil {
		*v = VCPU{Sockets: 1, Cores: n, Threads: 1}
		return nil
	}
	type plain VCPU
	var p plain
	if err := json.Unmarshal(raw, &p); err != nil {
		return err
	}
	*v = VCPU(p)
	return nil
}

func (v *VCPU) UnmarshalYAML(unmarshal func(any) error) error {
	var n int
	if err := unmarshal(&n); err == nil {
		*v = VCPU{Sockets: 1, Cores: n, Threads: 1}
		return nil
	}
	type plain VCPU
	var p plain
	if err := unmarshal(&p); err != nil {
		return err
	}
	*v = VCPU(p)
	return nil
}

type Flavor struct {
	Name string `json:"Name"`
	VCPU VCPU   `json:"VCPU"`
	RAM  int    `json:"RAM"`
	Disk int    `json:"Disk"`
}

type Node struct {
	ID             string    `json:"ID"`
	Name           string    `json:"Name"`
	State          string    `json:"State"`
	Enabled        bool      `json:"Enabled"`
	Driver         string    `json:"Driver"`
	IP             string    `json:"IP"`
	Total          Resources `json:"Total"`
	Available      Resources `json:"Available"`
	PinnedCPUs     []int     `json:"PinnedCPUs"`
	NotReadyReason string    `json:"NotReadyReason"`
}

type VirtualMachine struct {
	ID            string `json:"ID"`
	Name          string `json:"Name"`
	State         string `json:"State"`
	NodeID        string `json:"NodeID"`
	ImageRef      string `json:"ImageRef"`
	Flavor        Flavor `json:"Flavor"`
	PinnedCPUs    []int  `json:"PinnedCPUs"`
	PendingReason string `json:"PendingReason"`
	FailureReason string `json:"FailureReason"`
}

type Client struct {
	base string
	http *http.Client
	// stream has no Timeout: Client.Timeout spans the whole body read, so a
	// shared deadline would sever every watch after 30s of idleness.
	stream *http.Client
}

func NewClient(apiURL string) *Client {
	// Prior-knowledge h2c, matching the api's unencrypted HTTP/2: watch
	// streams ride one multiplexed connection instead of one TCP stream
	// each, with no certificates to manage.
	tr := &http2.Transport{
		AllowHTTP: true,
		DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, addr)
		},
	}
	return &Client{
		base:   strings.TrimSuffix(apiURL, "/") + "/v1alpha1",
		http:   &http.Client{Transport: tr, Timeout: 30 * time.Second},
		stream: &http.Client{Transport: tr},
	}
}

func (c *Client) get(path string, out any) error {
	resp, err := c.http.Get(c.base + path)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("%s: %s", resp.Status, strings.TrimSpace(string(body)))
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func (c *Client) post(path string, body any, out any) error {
	var raw []byte
	if body != nil {
		var err error
		raw, err = json.Marshal(body)
		if err != nil {
			return err
		}
	}
	resp, err := c.http.Post(c.base+path, "application/json", bytes.NewReader(raw))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		msg, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("%s: %s", resp.Status, strings.TrimSpace(string(msg)))
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}

func (c *Client) delete(path string) error {
	req, err := http.NewRequest(http.MethodDelete, c.base+path, nil)
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		msg, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("%s: %s", resp.Status, strings.TrimSpace(string(msg)))
	}
	return nil
}

func (c *Client) GetNodes() ([]Node, error) {
	var out []Node
	if err := c.get("/nodes", &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *Client) GetVirtualMachines() ([]VirtualMachine, error) {
	var out []VirtualMachine
	if err := c.get("/virtualmachines", &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *Client) CreateVirtualMachine(name, image, flavorRef string, flavor Flavor) (VirtualMachine, error) {
	var out VirtualMachine
	body := map[string]any{"name": name, "image_ref": image}
	if flavorRef != "" {
		body["flavor_ref"] = flavorRef
	} else {
		body["flavor"] = flavor
	}
	err := c.post("/virtualmachines", body, &out)
	return out, err
}

func (c *Client) CreateFlavor(name string, vcpu VCPU, memory, disk string) (Flavor, error) {
	var out Flavor
	err := c.post("/flavors", map[string]any{
		"name": name, "vcpu": vcpu, "memory": memory, "disk": disk,
	}, &out)
	return out, err
}

func (c *Client) GetFlavors() ([]Flavor, error) {
	var out []Flavor
	if err := c.get("/flavors", &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *Client) DeleteVirtualMachine(id string) error {
	return c.delete("/virtualmachines/" + id)
}

// snapshot mirrors one NDJSON line of a watch stream.
type snapshot struct {
	Nodes           []Node           `json:"nodes"`
	VirtualMachines []VirtualMachine `json:"virtualmachines"`
}
