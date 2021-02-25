package dapi

import (
	"errors"
)

type Folder struct {
	CatalogEntity
	Path     []string               `json:"path,omitempty"`
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
	return response, nil
}

type NewFolderSpec struct {
	Path []string
}

func (c *Client) NewFolder(spec *NewFolderSpec) (*Folder, error) {
	folder := Folder{
		CatalogEntity: CatalogEntity{
			EntityType: "folder",
		},
		Path: spec.Path,
	}
	result := new(Folder)
	return result, c.newCatalogItem(folder, result)
}
