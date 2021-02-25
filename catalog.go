package dapi

import (
	"bytes"
	"encoding/json"
	"fmt"
)

type CatalogEntity struct {
	EntityType string `json:"entityType,omitempty"`
	Id         string `json:"id,omitempty"`
	Tag        string `json:"tag,omitempty"`
}

type CatalogEntitySummary struct {
	Id            string   `json:"id,omitempty"`
	Tag           string   `json:"tag,omitempty"`
	Path          []string `json:"path,omitempty"`
	Type          string   `json:"type,omitempty"`
	DatasetType   string   `json:"datasetType,omitempty"`
	ContainerType string   `json:"containerType,omitempty"`
}

type GetCatalogResponse struct {
	Data []CatalogEntitySummary `json:"data"`
}

func (c *Client) GetRootCatalogSummary() ([]CatalogEntitySummary, error) {
	response := new(GetCatalogResponse)

	err := c.request("GET", "/api/v3/catalog", nil, nil, response)
	if err != nil {
		return nil, err
	}

	return response.Data, err
}

func (c *Client) getCatalogItem(id string, result interface{}) error {
	path := fmt.Sprintf("/api/v3/catalog/%s", id)
	err := c.request("GET", path, nil, nil, result)
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
	return c.request("POST", "/api/v3/catalog", nil, bytes.NewBuffer(body), result)
}

func (c *Client) updateCatalogItem(id string, payload interface{}, result interface{}) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	path := fmt.Sprintf("/api/v3/catalog/%s", id)
	return c.request("PUT", path, nil, bytes.NewBuffer(body), result)
}

func (c *Client) DeleteCatalogItem(id string) error {
	path := fmt.Sprintf("/api/v3/catalog/%s", id)
	return c.request("DELETE", path, nil, nil, nil)
}
