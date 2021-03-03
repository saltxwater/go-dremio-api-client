package dapi

import (
	"errors"
)

type Source struct {
	CatalogEntity
	Name        string      `json:"name,omitempty"`
	Description string      `json:"description,omitempty"`
	Type        string      `json:"type,omitempty"`
	Config      interface{} `json:"config,omitempty"`
	CreatedAt   string      `json:"createdAt,omitempty"`
}

func (c *Client) GetSource(id string) (*Source, error) {
	response := new(Source)
	err := c.getCatalogItem(id, response)
	if err != nil {
		return nil, err
	}
	if response.EntityType != "source" {
		return nil, errors.New("Catalog entity is not a source")
	}
	response.EnrichFields()
	return response, nil
}

type NewSourceSpec struct {
	Name        string
	Description string
	Type        string
	Config      interface{}
}

func (c *Client) NewSource(spec *NewSourceSpec) (*Source, error) {
	source := Source{
		CatalogEntity: CatalogEntity{
			EntityType: "source",
		},
		Name:        spec.Name,
		Description: spec.Description,
		Type:        spec.Type,
		Config:      spec.Config,
	}
	result := new(Source)
	err := c.newCatalogItem(source, result)
	if err != nil {
		return nil, err
	}
	result.EnrichFields()
	return result, nil
}

type UpdateSourceSpec struct {
	Description string
	Config      interface{}
}

func (c *Client) UpdateSource(id string, spec *UpdateSourceSpec) (*Source, error) {
	original, err := c.GetSource(id)
	if err != nil {
		return nil, err
	}
	source := Source{
		CatalogEntity: original.CatalogEntity,
		Name:          original.Name,
		Type:          original.Type,
		Description:   spec.Description,
		Config:        spec.Config,
	}
	result := new(Source)
	err = c.updateCatalogItem(id, source, result)
	if err != nil {
		return nil, err
	}
	result.EnrichFields()
	return result, err
}
