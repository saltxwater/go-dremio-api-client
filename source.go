package dapi

import (
	"errors"
)

type SourceMetadataPolicy struct {
	AuthTTLMs             int    `json:"authTTLMs,omitempty"`
	DatasetRefreshAfterMs int    `json:"datasetRefreshAfterMs,omitempty"`
	DatasetExpireAfterMs  int    `json:"datasetExpireAfterMs,omitempty"`
	NamesRefreshMs        int    `json:"namesRefreshMs,omitempty"`
	DatasetUpdateMode     string `json:"datasetUpdateMode,omitempty"`
}

type Source struct {
	CatalogEntity
	Description                 string                `json:"description,omitempty"`
	Type                        string                `json:"type,omitempty"`
	Config                      interface{}           `json:"config,omitempty"`
	CreatedAt                   string                `json:"createdAt,omitempty"`
	MetadataPolicy              *SourceMetadataPolicy `json:"metadataPolicy,omitempty"`
	AccelerationRefreshPeriodMs int                   `json:"accelerationRefreshPeriodMs,omitempty"`
	AccelerationGracePeriodMs   int                   `json:"accelerationGracePeriodMs,omitempty"`
	AccelerationNeverExpire     bool                  `json:"accelerationNeverExpire,omitempty"`
	AccelerationNeverRefresh    bool                  `json:"accelerationNeverRefresh,omitempty"`
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
	Name                        string
	Description                 string
	Type                        string
	Config                      interface{}
	MetadataPolicy              *SourceMetadataPolicy
	AccelerationRefreshPeriodMs int
	AccelerationGracePeriodMs   int
	AccelerationNeverExpire     bool
	AccelerationNeverRefresh    bool
}

func (c *Client) NewSource(spec *NewSourceSpec) (*Source, error) {
	source := Source{
		CatalogEntity: CatalogEntity{
			EntityType: "source",
			Name:       spec.Name,
		},
		Description:                 spec.Description,
		Type:                        spec.Type,
		Config:                      spec.Config,
		MetadataPolicy:              spec.MetadataPolicy,
		AccelerationRefreshPeriodMs: spec.AccelerationRefreshPeriodMs,
		AccelerationGracePeriodMs:   spec.AccelerationGracePeriodMs,
		AccelerationNeverExpire:     spec.AccelerationNeverExpire,
		AccelerationNeverRefresh:    spec.AccelerationNeverRefresh,
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
	Description                 string
	Config                      interface{}
	MetadataPolicy              *SourceMetadataPolicy
	AccelerationRefreshPeriodMs int
	AccelerationGracePeriodMs   int
	AccelerationNeverExpire     bool
	AccelerationNeverRefresh    bool
}

func (c *Client) UpdateSource(id string, spec *UpdateSourceSpec) (*Source, error) {
	original, err := c.GetSource(id)
	if err != nil {
		return nil, err
	}
	source := Source{
		CatalogEntity:               original.CatalogEntity,
		Type:                        original.Type,
		Description:                 spec.Description,
		Config:                      spec.Config,
		MetadataPolicy:              spec.MetadataPolicy,
		AccelerationRefreshPeriodMs: spec.AccelerationRefreshPeriodMs,
		AccelerationGracePeriodMs:   spec.AccelerationGracePeriodMs,
		AccelerationNeverExpire:     spec.AccelerationNeverExpire,
		AccelerationNeverRefresh:    spec.AccelerationNeverRefresh,
	}
	result := new(Source)
	err = c.updateCatalogItem(id, source, result)
	if err != nil {
		return nil, err
	}
	result.EnrichFields()
	return result, err
}
