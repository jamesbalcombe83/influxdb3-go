package influxdb3

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
)

type (
	// Dedicatedclient represents a client for InfluxDB Cloud Dedicated administration operations.
	DedicatedClient struct {
		client *Client
	}

	DedicatedClientConfig struct {
		AccountID        string
		ClusterID        string
		ManagementToken  string
		ManagementAPIURL url.URL
	}

	Database struct {
		Name               string              `json:"name"`
		MaxTables          uint64              `json:"maxTables"`          // default 500
		MaxColumnsPerTable uint64              `json:"maxColumnsPerTable"` // default 250
		RetentionPeriod    uint64              `json:"retentionPeriod"`    // nanoseconds default 0 is infinite
		PartitionTemplate  []PartitionTemplate `json:"partitionTemplate"`  // Tag or TagBucket, limit is total of 7
	}

	PartitionTemplate interface {
		IsPartitionTemplate()
	}

	Tag struct {
		Type  string `json:"type"`
		Value string `json:"value"`
	}

	TagBucket struct {
		Type  string         `json:"type"`
		Value TagBucketValue `json:"value"`
	}

	TagBucketValue struct {
		TagName         string `json:"tagName"`
		NumberOfBuckets uint64 `json:"numberOfBuckets"`
	}
)

func (t Tag) IsPartitionTemplate()        {}
func (tb TagBucket) IsPartitionTemplate() {}

// NewCloudDedicatedClient creates new DedicatedClient with given InfluxDB client.
func NewCloudDedicatedClient(client *Client) *DedicatedClient {
	return &DedicatedClient{client: client}
}

func (d *DedicatedClient) CreateDatabase(ctx context.Context, config *DedicatedClientConfig, db *Database) error {
	if db == nil {
		return errors.New("database must not nil")
	}

	if d.client.config.Database == "" {
		return errors.New("database name must not be empty")
	}
	db.Name = d.client.config.Database

	if len(db.PartitionTemplate) > 7 {
		return errors.New("partition template should not have more than 7 tags or tag buckets")
	}

	if db.MaxTables == 0 {
		db.MaxTables = uint64(500)
	}

	if db.MaxColumnsPerTable == 0 {
		db.MaxColumnsPerTable = uint64(250)
	}

	path := fmt.Sprintf("/api/v0/accounts/%s/clusters/%s/databases", config.AccountID, config.ClusterID)

	return d.createDatabase(ctx, path, db, config)
}

func (d *DedicatedClient) createDatabase(ctx context.Context, path string, db any, config *DedicatedClientConfig) error {
	u, err := config.ManagementAPIURL.Parse(path)
	if err != nil {
		return fmt.Errorf("failed to parse database creation path: %w", err)
	}

	body, err := json.Marshal(db)
	if err != nil {
		return fmt.Errorf("failed to marshal database creation request body: %w", err)
	}

	headers := http.Header{}
	headers.Set("Content-Type", "application/json")
	headers.Set("Accept", "application/json")
	headers.Set("Authorization", "Bearer "+config.ManagementToken)

	param := httpParams{
		endpointURL: u,
		queryParams: nil,
		httpMethod:  "POST",
		headers:     headers,
		body:        bytes.NewReader(body),
	}

	_, err = d.client.makeAPICall(ctx, param)
	return err
}
