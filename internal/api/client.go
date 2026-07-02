package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"

	"quorum/internal/model"
)

// ErrConflict is returned by Write when the server responds 409.
var ErrConflict = errors.New("version conflict")

type Client struct {
	base string
	http *http.Client
}

func NewClient(base string) *Client {
	return &Client{base: base, http: &http.Client{}}
}

func (c *Client) Read(docID string) (model.Entry, error) {
	var e model.Entry
	resp, err := c.http.Get(c.base + "/read?doc=" + url.QueryEscape(docID))
	if err != nil {
		return e, err
	}
	defer resp.Body.Close()
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
	defer resp.Body.Close()
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
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("findings: status %d", resp.StatusCode)
	}
	var out []model.Finding
	err = json.NewDecoder(resp.Body).Decode(&out)
	return out, err
}

func (c *Client) Claim(docID, agentID string, ttlMs int) (model.Claim, error) {
	var cl model.Claim
	body, _ := json.Marshal(leaseRequest{
		DocID: docID, AgentID: agentID, TTLMs: ttlMs,
	})
	resp, err := c.http.Post(c.base+"/claim", "application/json", bytes.NewReader(body))
	if err != nil {
		return cl, err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
		err = json.NewDecoder(resp.Body).Decode(&cl)
		return cl, err
	case http.StatusConflict:
		return cl, ErrConflict
	default:
		return cl, fmt.Errorf("claim: status %d", resp.StatusCode)
	}
}

func (c *Client) Renew(docID, agentID string, ttlMs int) (model.Claim, error) {
	var cl model.Claim
	body, _ := json.Marshal(leaseRequest{
		DocID: docID, AgentID: agentID, TTLMs: ttlMs,
	})
	resp, err := c.http.Post(c.base+"/renew", "application/json", bytes.NewReader(body))
	if err != nil {
		return cl, err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
		err = json.NewDecoder(resp.Body).Decode(&cl)
		return cl, err
	case http.StatusConflict:
		return cl, ErrConflict
	default:
		return cl, fmt.Errorf("renew: status %d", resp.StatusCode)
	}
}

func (c *Client) Release(docID, agentID string) error {
	body, _ := json.Marshal(leaseRequest{
		DocID: docID, AgentID: agentID,
	})
	resp, err := c.http.Post(c.base+"/release", "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusNoContent:
		return nil
	case http.StatusConflict:
		return ErrConflict
	default:
		return fmt.Errorf("release: status %d", resp.StatusCode)
	}
}
