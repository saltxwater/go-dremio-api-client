package dapi

import (
	"errors"
)

type Space struct {
	CatalogEntity
	CreatedAt string                 `json:"createdAt,omitempty"`
	Children  []CatalogEntitySummary `json:"children,omitempty"`
}

func (c *Client) GetSpace(id string) (*Space, error) {
	response := new(Space)
	err := c.getCatalogItem(id, response)
	if err != nil {
		return nil, err
	}
	if response.EntityType != "space" {
		return nil, errors.New("Catalog entity is not a space")
	}
	response.EnrichFields()
	return response, nil
}

type NewSpaceSpec struct {
	Name string
}

func (c *Client) NewSpace(spec *NewSpaceSpec) (*Space, error) {
	space := Space{
		CatalogEntity: CatalogEntity{
			EntityType: "space",
			Name:       spec.Name,
		},
	}
	result := new(Space)
	err := c.newCatalogItem(space, result)
	if err != nil {
		return nil, err
	}
	result.EnrichFields()
	return result, nil
}
