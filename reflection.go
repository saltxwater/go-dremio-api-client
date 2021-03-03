package dapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
)

type Reflection struct {
	EntityType                    string            `json:"entityType,omitempty"`
	Id                            string            `json:"id,omitempty"`
	Tag                           string            `json:"tag,omitempty"`
	Name                          string            `json:"name,omitempty"`
	Enabled                       bool              `json:"enabled,omitempty"`
	CreatedAt                     string            `json:"createdAt,omitempty"`
	UpdatedAt                     string            `json:"updatedAt,omitempty"`
	Type                          string            `json:"type,omitempty"`
	DatasetId                     string            `json:"datasetId,omitempty"`
	CurrentSizeBytes              int               `json:"currentSizeBytes,omitempty"`
	TotalSizeBytes                int               `json:"totalSizeBytes,omitempty"`
	Status                        ReflectionStatus  `json:"status,omitempty"`
	DistributionFields            []ReflectionField `json:"distributionFields,omitempty"`
	PartitionFields               []ReflectionField `json:"partitionFields,omitempty"`
	SortFields                    []ReflectionField `json:"sortFields,omitempty"`
	PartitionDistributionStrategy string            `json:"partitionDistributionStrategy,omitempty"`
}

type ReflectionStatus struct {
	Config       string `json:"config,omitempty"`
	Refresh      string `json:"refresh,omitempty"`
	Availability string `json:"availability,omitempty"`
	FailureCount int    `json:"failureCount,omitempty"`
	LastRefresh  string `json:"lastRefresh,omitempty"`
	ExpiresAt    string `json:"expiresAt,omitempty"`
}

type RawReflection struct {
	Reflection
	DisplayFields []ReflectionField `json:"displayFields,omitempty"`
}

type AggregationReflection struct {
	Reflection
	DimensionFields []ReflectionFieldWithGranularity `json:"dimensionFields,omitempty"`
	MeasureFields   []ReflectionMeasureField         `json:"measureFields,omitempty"`
}

type ReflectionField struct {
	Name string `json:"name,omitempty"`
}

type ReflectionFieldWithGranularity struct {
	ReflectionField
	Granularity string `json:"granularity,omitempty"`
}

type ReflectionMeasureField struct {
	ReflectionField
	MeasureTypeList []string `json:"measureTypeList,omitempty"`
}

func (c *Client) getReflection(id string, result interface{}) error {
	path := fmt.Sprintf("/api/v3/reflection/%s", url.QueryEscape(id))
	err := c.request("GET", path, nil, result)
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) newReflection(payload interface{}, result interface{}) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return c.request("POST", "/api/v3/reflection", bytes.NewBuffer(body), result)
}

func (c *Client) updateReflection(id string, payload interface{}, result interface{}) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	path := fmt.Sprintf("/api/v3/reflection/%s", url.QueryEscape(id))
	return c.request("PUT", path, bytes.NewBuffer(body), result)
}

func (c *Client) DeleteReflection(id string) error {
	path := fmt.Sprintf("/api/v3/reflection/%s", url.QueryEscape(id))
	return c.request("DELETE", path, nil, nil)
}

func (c *Client) GetRawReflection(id string) (*RawReflection, error) {
	reflection := new(RawReflection)
	err := c.getReflection(id, reflection)
	if err != nil {
		return nil, err
	}
	if reflection.Type != "RAW" {
		return nil, errors.New("Reflection is not RAW")
	}
	return reflection, nil
}

func (c *Client) GetAggregationReflection(id string) (*AggregationReflection, error) {
	reflection := new(AggregationReflection)
	err := c.getReflection(id, reflection)
	if err != nil {
		return nil, err
	}
	if reflection.Type != "AGGREGATION" {
		return nil, errors.New("Reflection is not AGGREGATION")
	}
	return reflection, nil
}

type RawReflectionSpec struct {
	Name                          string
	Enabled                       bool
	DisplayFields                 []ReflectionField
	DistributionFields            []ReflectionField
	PartitionFields               []ReflectionField
	SortFields                    []ReflectionField
	PartitionDistributionStrategy string
}

func (c *Client) NewRawReflection(datasetId string, spec *RawReflectionSpec) (*RawReflection, error) {
	reflection := RawReflection{
		Reflection: Reflection{
			EntityType:                    "reflection",
			Name:                          spec.Name,
			Enabled:                       spec.Enabled,
			Type:                          "RAW",
			DatasetId:                     datasetId,
			DistributionFields:            spec.DistributionFields,
			PartitionFields:               spec.PartitionFields,
			SortFields:                    spec.SortFields,
			PartitionDistributionStrategy: spec.PartitionDistributionStrategy,
		},
		DisplayFields: spec.DisplayFields,
	}
	result := new(RawReflection)
	return result, c.newReflection(reflection, result)
}

type AggregationReflectionSpec struct {
	Name                          string
	Enabled                       bool
	DimensionFields               []ReflectionFieldWithGranularity
	MeasureFields                 []ReflectionMeasureField
	DistributionFields            []ReflectionField
	PartitionFields               []ReflectionField
	SortFields                    []ReflectionField
	PartitionDistributionStrategy string
}

func (c *Client) NewAggregationReflection(datasetId string, spec *AggregationReflectionSpec) (*AggregationReflection, error) {
	reflection := AggregationReflection{
		Reflection: Reflection{
			EntityType:                    "reflection",
			Name:                          spec.Name,
			Enabled:                       spec.Enabled,
			Type:                          "AGGREGATION",
			DatasetId:                     datasetId,
			DistributionFields:            spec.DistributionFields,
			PartitionFields:               spec.PartitionFields,
			SortFields:                    spec.SortFields,
			PartitionDistributionStrategy: spec.PartitionDistributionStrategy,
		},
		DimensionFields: spec.DimensionFields,
		MeasureFields:   spec.MeasureFields,
	}
	result := new(AggregationReflection)
	return result, c.newReflection(reflection, result)
}

func (c *Client) UpdateRawReflection(id string, spec *RawReflectionSpec) (*RawReflection, error) {
	original, err := c.GetRawReflection(id)
	if err != nil {
		return nil, err
	}
	reflection := RawReflection{
		Reflection: Reflection{
			EntityType:                    "reflection",
			Id:                            id,
			Tag:                           original.Tag,
			Name:                          spec.Name,
			Enabled:                       spec.Enabled,
			Type:                          "RAW",
			DatasetId:                     original.DatasetId,
			DistributionFields:            spec.DistributionFields,
			PartitionFields:               spec.PartitionFields,
			SortFields:                    spec.SortFields,
			PartitionDistributionStrategy: spec.PartitionDistributionStrategy,
		},
		DisplayFields: spec.DisplayFields,
	}
	result := new(RawReflection)
	return result, c.updateReflection(id, reflection, result)
}

func (c *Client) UpdateAggregationReflection(id string, spec *AggregationReflectionSpec) (*AggregationReflection, error) {
	original, err := c.GetAggregationReflection(id)
	if err != nil {
		return nil, err
	}
	reflection := AggregationReflection{
		Reflection: Reflection{
			EntityType:                    "reflection",
			Id:                            id,
			Tag:                           original.Tag,
			Name:                          spec.Name,
			Enabled:                       spec.Enabled,
			Type:                          "AGGREGATION",
			DatasetId:                     original.DatasetId,
			DistributionFields:            spec.DistributionFields,
			PartitionFields:               spec.PartitionFields,
			SortFields:                    spec.SortFields,
			PartitionDistributionStrategy: spec.PartitionDistributionStrategy,
		},
		DimensionFields: spec.DimensionFields,
		MeasureFields:   spec.MeasureFields,
	}
	result := new(AggregationReflection)
	return result, c.updateReflection(id, reflection, result)
}
