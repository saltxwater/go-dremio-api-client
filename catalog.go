package dapi

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"strings"
)

type CatalogEntity struct {
	EntityType string         `json:"entityType,omitempty"`
	Id         string         `json:"id,omitempty"`
	Tag        string         `json:"tag,omitempty"`
	Path       []string       `json:"path,omitempty"`
	Name       string         `json:"name,omitempty"`
	Children   []CatalogChild `json:"children,omitempty"`
}

type CatalogEntitySummary struct {
	Id            string   `json:"id,omitempty"`
	Tag           string   `json:"tag,omitempty"`
	Path          []string `json:"path,omitempty"`
	Type          string   `json:"type,omitempty"`
	DatasetType   string   `json:"datasetType,omitempty"`
	ContainerType string   `json:"containerType,omitempty"`
}

type CatalogChild struct {
	Id            string   `json:"id,omitempty"`
	Path          []string `json:"path,omitempty"`
	Tag           string   `json:"tag,omitempty"`
	Name          string   `json:"name,omitempty"`
	Type          string   `json:"type,omitempty"`
	DatasetType   string   `json:"datasetType,omitempty"`
	ContainerType string   `json:"containerType,omitempty"`
}

type GetCatalogResponse struct {
	Data []CatalogEntitySummary `json:"data"`
}

type TagsBody struct {
	Tags    []string `json:"tags"`
	Version string   `json:"version"`
}

type WikiBody struct {
	Text    string `json:"text"`
	Version int    `json:"version"`
}

func (c *Client) GetEntityTags(id string) (TagsBody, error) {
	response := new(TagsBody)
	url := fmt.Sprintf("/api/v3/catalog/%s/collaboration/tag", id)
	err := c.request("GET", url, nil, response)
	if err != nil {
		return TagsBody{}, err
	}
	return *response, err
}

func (c *Client) SetEntityTags(id string, tags []string, version string) error {
	rawBody := TagsBody{
		Tags:    tags,
		Version: version,
	}
	body, err := json.Marshal(rawBody)
	if err != nil {
		return err
	}
	url := fmt.Sprintf("/api/v3/catalog/%s/collaboration/tag", id)
	return c.request("POST", url, bytes.NewBuffer(body), nil)
}

func (c *Client) GetEntityWiki(id string) (WikiBody, error) {
	response := new(WikiBody)
	url := fmt.Sprintf("/api/v3/catalog/%s/collaboration/wiki", id)
	err := c.request("GET", url, nil, response)
	if err != nil {
		return WikiBody{}, err
	}
	return *response, err
}
func (c *Client) SetEntityWiki(id string, text string, version int) error {
	rawBody := WikiBody{
		Text:    text,
		Version: version,
	}
	body, err := json.Marshal(rawBody)
	if err != nil {
		return err
	}
	url := fmt.Sprintf("/api/v3/catalog/%s/collaboration/wiki", id)
	return c.request("POST", url, bytes.NewBuffer(body), nil)
}

func (c *Client) GetRootCatalogSummary() ([]CatalogEntitySummary, error) {
	response := new(GetCatalogResponse)

	err := c.request("GET", "/api/v3/catalog", nil, response)
	if err != nil {
		return nil, err
	}

	return response.Data, err
}

func (c *Client) GetCatalogEntityById(id string) (*CatalogEntity, error) {
	response := new(CatalogEntity)
	err := c.getCatalogItem(id, response)
	if err != nil {
		return nil, err
	}
	response.EnrichFields()
	return response, nil
}

func (c *Client) GetCatalogEntityByPath(path []string) (*CatalogEntity, error) {
	elements := make([]string, len(path))
	for i, e := range path {
		elements[i] = url.QueryEscape(e)
	}
	response := new(CatalogEntity)
	url := fmt.Sprintf("/api/v3/catalog/by-path/%s", strings.Join(elements, "/"))
	err := c.request("GET", url, nil, response)
	if err != nil {
		return nil, err
	}
	response.EnrichFields()
	return response, nil
}

func (c *Client) getCatalogItem(id string, result interface{}) error {
	path := fmt.Sprintf("/api/v3/catalog/%s", url.QueryEscape(id))
	err := c.request("GET", path, nil, result)
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) newCatalogItem(payload interface{}, result interface{}) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return c.request("POST", "/api/v3/catalog", bytes.NewBuffer(body), result)
}

func (c *Client) updateCatalogItem(id string, payload interface{}, result interface{}) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	path := fmt.Sprintf("/api/v3/catalog/%s", url.QueryEscape(id))
	return c.request("PUT", path, bytes.NewBuffer(body), result)
}

func (c *Client) DeleteCatalogItem(id string) error {
	path := fmt.Sprintf("/api/v3/catalog/%s", url.QueryEscape(id))
	return c.request("DELETE", path, nil, nil)
}

func (ce *CatalogEntity) EnrichFields() {
	log.Printf("Enrich Entity: %#v", ce)
	if ce.Name == "" && len(ce.Path) > 0 {
		ce.Name = ce.Path[len(ce.Path)-1]
	}
	if len(ce.Path) == 0 && ce.Name != "" {
		ce.Path = []string{ce.Name}
	}

	for _, b := range ce.Children {
		b.EnrichFields()
	}
	log.Printf("Enriched Entity: %#v", ce)
}

func (ce *CatalogChild) EnrichFields() {
	log.Printf("Enrich Child: %#v", ce)
	if ce.Name == "" && len(ce.Path) > 0 {
		ce.Name = ce.Path[len(ce.Path)-1]
	}
	if len(ce.Path) == 0 && ce.Name != "" {
		ce.Path = []string{ce.Name}
	}
	log.Printf("Enriched Child: %#v", ce)
}
