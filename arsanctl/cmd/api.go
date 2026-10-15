package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Resources mirrors the api's shape. Field names match the server's JSON.
type Resources struct {
	VCPU int `json:"VCPU"`
	RAM  int `json:"RAM"`
	Disk int `json:"Disk"`
}

type Flavor struct {
	Name string `json:"Name"`
	VCPU int    `json:"VCPU"`
	RAM  int    `json:"RAM"`
	Disk int    `json:"Disk"`
}

type Node struct {
	ID             string    `json:"ID"`
	Name           string    `json:"Name"`
	State          string    `json:"State"`
	Enabled        bool      `json:"Enabled"`
	Driver         string    `json:"Driver"`
	Total          Resources `json:"Total"`
	Available      Resources `json:"Available"`
	NotReadyReason string    `json:"NotReadyReason"`
}

type VirtualMachine struct {
	ID       string `json:"ID"`
	Name     string `json:"Name"`
	State    string `json:"State"`
	NodeID   string `json:"NodeID"`
	ImageRef string `json:"ImageRef"`
	Flavor   Flavor `json:"Flavor"`
}

type Client struct {
	base string
	http *http.Client
}

func NewClient(apiURL string) *Client {
	return &Client{
		base: strings.TrimSuffix(apiURL, "/") + "/v1alpha1",
		http: &http.Client{Timeout: 30 * time.Second},
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

func (c *Client) CreateVirtualMachine(name, image string, flavor Flavor) (VirtualMachine, error) {
	var out VirtualMachine
	err := c.post("/virtualmachines", map[string]any{
		"name": name, "image_ref": image, "flavor": flavor,
	}, &out)
	return out, err
}

func (c *Client) DeleteVirtualMachine(id string) error {
	return c.delete("/virtualmachines/" + id)
}

// snapshot mirrors one NDJSON line of GET /nodes/watch.
type snapshot struct {
	Nodes []Node `json:"nodes"`
}
