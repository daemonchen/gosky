package sky

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
)

const (
	// Version is the Sky version this library is meant to work with.
	Version = "0.4.0"

	// DefaultHost is the host if no host is set on the client.
	DefaultHost = "localhost:8585"
)

// Client represents the client interface to the Sky server.
type Client struct {
	HTTPClient http.Client
	Host       string
}

// Constructs a URL based on the client's host, port and a given path.
func (c *Client) URL(path string) *url.URL {
	return &url.URL{Scheme: "http", Host: c.Host, Path: path}
}

// Send sends low-level data to and from the server.
func (c *Client) Send(method string, path string, data interface{}, ret interface{}) error {
	url := c.URL(path)

	// Convert the data to JSON.
	var err error
	var body []byte
	if str, ok := data.(string); ok {
		body = []byte(str)
	} else if data != nil {
		body, err = json.Marshal(data)
		if err != nil {
			return err
		}
	}

	// Create the request object.
	req, err := http.NewRequest(method, url.String(), strings.NewReader(string(body)))
	if err != nil {
		return err
	}
	if _, ok := data.(string); ok {
		req.Header.Add("Content-Type", "text/plain")
	} else {
		req.Header.Add("Content-Type", "application/json")
	}

	// Send the request to the server.
	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// If we have a return object then deserialize to it.
	if resp.StatusCode != http.StatusOK {
		var m message
		b, _ := ioutil.ReadAll(resp.Body)
		if json.Unmarshal(b, &m); m.Message != "" {
			return errors.New(m.Message)
		} else {
			return fmt.Errorf("%d error: %s %s", resp.StatusCode, method, url.String())
		}
	}

	// Deserialize data into return object if we have one.
	if ret != nil {
		err := json.NewDecoder(resp.Body).Decode(ret)
		if err != nil && err != io.EOF {
			return err
		}
	}

	return nil
}

// Table retrieves a reference to a given table.
func (c *Client) Table(name string) (*Table, error) {
	if name == "" {
		return nil, ErrTableNameRequired
	}
	table := &Table{Client: c}
	if err := c.Send("GET", fmt.Sprintf("/tables/%s", name), nil, table); err != nil {
		return nil, err
	}
	return table, nil
}

// Tables retrieves a list of all table on the server.
func (c *Client) Tables() ([]*Table, error) {
	tables := make([]*Table, 0)
	if err := c.Send("GET", "/tables", nil, &tables); err != nil {
		return nil, err
	}
	for _, t := range tables {
		t.Client = c
	}
	return tables, nil
}

func (c *Client) CreateTable(t *Table) error {
	if t == nil {
		return ErrTableRequired
	}
	t.Client = c
	return c.Send("POST", "/tables", t, t)
}

func (c *Client) DeleteTable(name string) error {
	if name == "" {
		return ErrTableNameRequired
	}
	return c.Send("DELETE", path.Join("/tables", name), nil, nil)
}

func (c *Client) Ping() bool {
	err := c.Send("GET", "/ping", nil, nil)
	return (err == nil)
}

func (c *Client) Stream() (*EventStream, error) {
	return NewEventStream(c)
}

func warn(v ...interface{}) {
	fmt.Fprintln(os.Stderr, v...)
}

func warnf(msg string, v ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", v...)
}

// message is a generic return message from Sky that can occur on error.
type message struct {
	Message string `json:"message"`
}
