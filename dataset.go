package dapi

import (
	"errors"
)

type DatasetField struct {
	Name string           `json:"name,omitempty"`
	Type DatasetFieldType `json:"type,omitempty"`
}

type DatasetFieldType struct {
	Name      string         `json:"name,omitempty"`
	SubSchema []DatasetField `json:"subSchema,omitempty"`
	Precision int            `json:"precision,omitempty"`
	Scale     int            `json:"scale,omitempty"`
}

type Dataset struct {
	CatalogEntity
	Path   []string       `json:"path,omitempty"`
	Type   string         `json:"type,omitempty"`
	Fields []DatasetField `json:"fields,omitempty"`
}

type VirtualDataset struct {
	Dataset
	Sql        string   `json:"sql,omitempty"`
	SqlContext []string `json:"sqlContext,omitempty"`
}

type PhysicalDataset struct {
	Dataset
	Format PhysicalDatasetFormat `json:"format,omitempty"`
}

type PhysicalDatasetFormat struct {
	Type                    string `json:"type,omitempty"`
	FieldDelimiter          string `json:"fieldDelimiter,omitempty"`
	LineDelimiter           string `json:"lineDelimiter,omitempty"`
	Quote                   string `json:"quote,omitempty"`
	Comment                 string `json:"comment,omitempty"`
	Escape                  string `json:"escape,omitempty"`
	SkipFirstLine           bool   `json:"skipFirstLine,omitempty"`
	ExtractHeader           bool   `json:"extractHeader,omitempty"`
	TrimHeader              bool   `json:"trimHeader,omitempty"`
	AutoGenerateColumnNames bool   `json:"autoGenerateColumnNames,omitempty"`
	SheetName               string `json:"sheetName,omitempty"`
	HasMergedCells          bool   `json:"hasMergedCells,omitempty"`
}

func (c *Client) GetDataset(id string) (*Dataset, error) {
	result := new(Dataset)
	err := c.getCatalogItem(id, result)
	if err != nil {
		return nil, err
	}
	if result.EntityType != "dataset" {
		return nil, errors.New("Catalog entity is not a dataset")
	}
	return result, nil
}

func (c *Client) GetVirtualDataset(id string) (*VirtualDataset, error) {
	result := new(VirtualDataset)
	err := c.getCatalogItem(id, result)
	if err != nil {
		return nil, err
	}
	if result.EntityType != "dataset" {
		return nil, errors.New("Catalog entity is not a dataset")
	}
	if result.Type != "VIRTUAL_DATASET" {
		return nil, errors.New("Dataset is not a VIRTUAL_DATASET")
	}
	return result, nil
}

func (c *Client) GetPhysicalDataset(id string) (*PhysicalDataset, error) {
	result := new(PhysicalDataset)
	err := c.getCatalogItem(id, result)
	if err != nil {
		return nil, err
	}
	if result.EntityType != "dataset" {
		return nil, errors.New("Catalog entity is not a dataset")
	}
	if result.Type != "PHYSICAL_DATASET" {
		return nil, errors.New("Dataset is not a PHYSICAL_DATASET")
	}
	return result, nil
}

type NewVirtualDatasetSpec struct {
	Path       []string
	Sql        string
	SqlContext []string
}

func (c *Client) NewVirtualDataset(spec *NewVirtualDatasetSpec) (*VirtualDataset, error) {
	dataset := VirtualDataset{
		Dataset: Dataset{
			CatalogEntity: CatalogEntity{
				EntityType: "dataset",
			},
			Path: spec.Path,
			Type: "VIRTUAL_DATASET",
		},
		Sql:        spec.Sql,
		SqlContext: spec.SqlContext,
	}
	result := new(VirtualDataset)
	return result, c.newCatalogItem(dataset, result)
}

type UpdateVirtualDatasetSpec struct {
	Sql        string
	SqlContext []string
}

func (c *Client) UpdateVirtualDataset(id string, spec *UpdateVirtualDatasetSpec) (*VirtualDataset, error) {
	original, err := c.GetVirtualDataset(id)
	if err != nil {
		return nil, err
	}
	dataset := VirtualDataset{
		Dataset:    original.Dataset,
		Sql:        spec.Sql,
		SqlContext: spec.SqlContext,
	}
	result := new(VirtualDataset)
	return result, c.updateCatalogItem(id, dataset, result)
}
