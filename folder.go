package dapi

import (
	"errors"
)

type Folder struct {
	CatalogEntity
	Children []CatalogEntitySummary `json:"children,omitempty"`
}

func (c *Client) GetFolder(id string) (*Folder, error) {
	response := new(Folder)
	err := c.getCatalogItem(id, response)
	if err != nil {
		return nil, err
	}
	if response.EntityType != "folder" {
		return nil, errors.New("Catalog entity is not a folder")
	}
	response.EnrichFields()
	return response, nil
}

type NewFolderSpec struct {
	Path []string
}

func (c *Client) NewFolder(spec *NewFolderSpec) (*Folder, error) {
	folder := Folder{
		CatalogEntity: CatalogEntity{
			EntityType: "folder",
			Path:       spec.Path,
		},
	}
	result := new(Folder)
	err := c.newCatalogItem(folder, result)
	if err != nil {
		return nil, err
	}
	result.EnrichFields()
	return result, err
}
