package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"quorum/internal/model"
)

// ErrConflict is returned by Write when the server responds 409.
var ErrConflict = errors.New("version conflict")

var (
	ErrLeaseHeld = errors.New("lease held by another agent")
	ErrNotHolder = errors.New("caller does not hold the lease")
)

type Client struct {
	base string
	http *http.Client
}

func NewClient(base string) *Client {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.MaxIdleConns = 256
	t.MaxIdleConnsPerHost = 256
	return &Client{base: base, http: &http.Client{Timeout: 5 * time.Second, Transport: t}}
}

func (c *Client) Read(docID string) (model.Entry, error) {
	var e model.Entry
	resp, err := c.http.Get(c.base + "/read?doc=" + url.QueryEscape(docID))
	if err != nil {
		return e, err
	}
	defer func() { _, _ = io.Copy(io.Discard, resp.Body); resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return e, fmt.Errorf("read: status %d", resp.StatusCode)
	}
	err = json.NewDecoder(resp.Body).Decode(&e)
	return e, err
}

func (c *Client) Write(docID, agentID, payload string, baseVersion int) (model.Finding, error) {
	var f model.Finding
	body, _ := json.Marshal(writeRequest{
		DocID: docID, AgentID: agentID, Payload: payload, BaseVersion: baseVersion,
	})
	resp, err := c.http.Post(c.base+"/write", "application/json", bytes.NewReader(body))
	if err != nil {
		return f, err
	}
	defer func() { _, _ = io.Copy(io.Discard, resp.Body); resp.Body.Close() }()
	switch resp.StatusCode {
	case http.StatusOK:
		err = json.NewDecoder(resp.Body).Decode(&f)
		return f, err
	case http.StatusConflict:
		return f, ErrConflict
	default:
		return f, fmt.Errorf("write: status %d", resp.StatusCode)
	}
}

func (c *Client) Findings(query string) ([]model.Finding, error) {
	u := c.base + "/findings"
	if query != "" {
		u += "?q=" + url.QueryEscape(query)
	}
	resp, err := c.http.Get(u)
	if err != nil {
		return nil, err
	}
	defer func() { _, _ = io.Copy(io.Discard, resp.Body); resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("findings: status %d", resp.StatusCode)
	}
	var out []model.Finding
	err = json.NewDecoder(resp.Body).Decode(&out)
	return out, err
}

func (c *Client) Claim(docID, agentID string, ttl time.Duration) (model.Claim, error) {
	return c.leaseCall("/claim", docID, agentID, ttl, ErrLeaseHeld)
}

func (c *Client) Renew(docID, agentID string, ttl time.Duration) (model.Claim, error) {
	return c.leaseCall("/renew", docID, agentID, ttl, ErrNotHolder)
}

func (c *Client) leaseCall(path, docID, agentID string, ttl time.Duration, conflictErr error) (model.Claim, error) {
	var out model.Claim
	body, _ := json.Marshal(leaseRequest{DocID: docID, AgentID: agentID, TTLMs: int(ttl.Milliseconds())})
	resp, err := c.http.Post(c.base+path, "application/json", bytes.NewReader(body))
	if err != nil {
		return out, err
	}
	defer func() { _, _ = io.Copy(io.Discard, resp.Body); resp.Body.Close() }()
	switch resp.StatusCode {
	case http.StatusOK:
		err = json.NewDecoder(resp.Body).Decode(&out)
		return out, err
	case http.StatusConflict:
		return out, conflictErr
	default:
		return out, fmt.Errorf("%s: status %d", path, resp.StatusCode)
	}
}

func (c *Client) Release(docID, agentID string) error {
	body, _ := json.Marshal(leaseRequest{DocID: docID, AgentID: agentID})
	resp, err := c.http.Post(c.base+"/release", "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer func() { _, _ = io.Copy(io.Discard, resp.Body); resp.Body.Close() }()
	switch resp.StatusCode {
	case http.StatusNoContent:
		return nil
	case http.StatusConflict:
		return ErrNotHolder
	default:
		return fmt.Errorf("release: status %d", resp.StatusCode)
	}
}
