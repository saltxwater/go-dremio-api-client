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
	ApiKey   string
	Username string
	Password string
	Client   *http.Client
}

// New creates a new Dremio client.
func NewClient(baseUrl string, cfg Config) (*Client, error) {
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

func (c *Client) request(method, requestPath string, body io.Reader, responseStruct interface{}) error {
	if c.config.ApiKey == "" {
		apikey, err := c.getApiKey(c.config.Username, c.config.Password)
		if err != nil {
			return err
		}
		c.config.ApiKey = apikey
	}

	r, err := c.newRequest(method, requestPath, body)
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

func (c *Client) newRequest(method, requestPath string, body io.Reader) (*http.Request, error) {
	log.Printf("BaseUrl %s", c.baseUrl.String())
	url := c.baseUrl.String() + requestPath
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return req, err
	}

	if c.config.ApiKey != "" {
		req.Header.Add("Authorization", fmt.Sprintf("_dremio%s", c.config.ApiKey))
	}

	if os.Getenv("DREMIO_LOG") != "" {
		if body == nil {
			log.Printf("request (%s) to %s with no body data", method, url)
		} else {
			log.Printf("request (%s) to %s with body data: %s", method, url, body.(*bytes.Buffer))
		}
	}

	req.Header.Add("Content-Type", "application/json")

	log.Printf("Created %s request for %s.", req.Method, url)
	return req, err
}

type authResponse struct {
	Token   string `json:"token"`
	Expires int    `json:"expires"`
}

func (c *Client) getApiKey(username string, password string) (string, error) {
	url := c.baseUrl
	url.Path = path.Join(url.Path, "/apiv2/login")

	bodyObj, err := json.Marshal(map[string]string{
		"userName": username,
		"password": password,
	})
	if err != nil {
		return "", err
	}
	body := bytes.NewBuffer(bodyObj)
	req, err := http.NewRequest("POST", url.String(), body)
	if err != nil {
		return "", err
	}
	req.Header.Add("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return "", nil
	}
	defer resp.Body.Close()

	bodyContents, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", nil
	}

	if resp.StatusCode >= 400 {
		return "", fmt.Errorf("status: %d, body: %v", resp.StatusCode, string(bodyContents))
	}
	responseStruct := new(authResponse)

	err = json.Unmarshal(bodyContents, &responseStruct)
	if err != nil {
		return "", err
	}

	return responseStruct.Token, nil
}
