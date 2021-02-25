package dapi

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"path"

	"github.com/hashicorp/go-cleanhttp"
)

type Client struct {
	config  Config
	baseUrl url.URL
	client  *http.Client
}

type Config struct {
	ApiKey string
	Client *http.Client
}

// New creates a new Dremio client.
func New(baseUrl string, cfg Config) (*Client, error) {
	u, err := url.Parse(baseUrl)

	if err != nil {
		return nil, err
	}

	cli := cfg.Client
	if cli == nil {
		cli = cleanhttp.DefaultClient()
	}

	return &Client{
		config:  cfg,
		baseUrl: *u,
		client:  cli,
	}, nil
}

func (c *Client) request(method, requestPath string, query url.Values, body io.Reader, responseStruct interface{}) error {
	r, err := c.newRequest(method, requestPath, query, body)
	if err != nil {
		return err
	}

	resp, err := c.client.Do(r)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	bodyContents, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if os.Getenv("DREMIO_LOG") != "" {
		log.Printf("response status %d with body %v", resp.StatusCode, string(bodyContents))
	}

	if resp.StatusCode >= 400 {
		return fmt.Errorf("status: %d, body: %v", resp.StatusCode, string(bodyContents))
	}

	if responseStruct == nil {
		return nil
	}

	err = json.Unmarshal(bodyContents, responseStruct)
	if err != nil {
		return err
	}

	return nil
}

func (c *Client) newRequest(method, requestPath string, query url.Values, body io.Reader) (*http.Request, error) {
	url := c.baseUrl
	url.Path = path.Join(url.Path, requestPath)
	url.RawQuery = query.Encode()
	req, err := http.NewRequest(method, url.String(), body)
	if err != nil {
		return req, err
	}

	if c.config.ApiKey != "" {
		req.Header.Add("Authorization", fmt.Sprintf("_dremio%s", c.config.ApiKey))
	}

	if os.Getenv("DREMIO_LOG") != "" {
		if body == nil {
			log.Printf("request (%s) to %s with no body data", method, url.String())
		} else {
			log.Printf("request (%s) to %s with body data: %s", method, url.String(), body.(*bytes.Buffer).String())
		}
	}

	req.Header.Add("Content-Type", "application/json")
	return req, err
}
