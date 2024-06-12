package influxdb3

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
)

type (
	// Dedicatedclient represents a client for InfluxDB Cloud Dedicated administration operations.
	DedicatedClient struct {
		client *Client
	}

	Database struct {
		Name               string            `json:"name"`
		MaxTables          uint64            `json:"maxTables,omitempty"`          // default 500
		MaxColumnsPerTable uint64            `json:"maxColumnsPerTable,omitempty"` // default 250
		RetentionPeriod    uint64            `json:"retentionPeriod,omitempty"`    // nanoseconds default 0 is infinite
		PartitionTemplate  PartitionTemplate `json:"partitionTemplate,omitempty"`
	}

	PartitionTemplate struct {
		TagOrTagBucketArray []interface{} // Tag or TagBucket, limit is total of 7
	}

	Tag struct {
		Type  string `json:"type"`
		Value string `json:"value"`
	}

	TagBucket struct {
		Type  string `json:"type"`
		Value struct {
			TagName         string `json:"tagName"`
			NumberOfBuckets uint64 `json:"numberOfBuckets"`
		}
	}
)

// NewCloudDedicatedClient creates new DedicatedClient with given InfluxDB client.
func NewCloudDedicatedClient(client *Client) *DedicatedClient {
	return &DedicatedClient{client: client}
}

func (d *DedicatedClient) CreateDatabase(ctx context.Context, db *Database, accountID, clusterID string) error {
	if db == nil {
		return errors.New("database must not nil")
	}

	if d.client.config.Database == "" {
		return errors.New("database name must not be empty")
	}
	db.Name = d.client.config.Database

	if len(db.PartitionTemplate.TagOrTagBucketArray) > 7 {
		return errors.New("partition template should not have more than 7 tags or tag buckets")
	}

	if db.MaxTables == 0 {
		db.MaxTables = 500
	}

	if db.MaxColumnsPerTable == 0 {
		db.MaxColumnsPerTable = 250
	}

	path := fmt.Sprintf("/api/v0/accounts/%s/clusters/%s/databases", accountID, clusterID)

	return d.createDatabase(ctx, path, db)
}

func (d *DedicatedClient) createDatabase(ctx context.Context, path string, db any) error {
	u, err := d.client.apiURL.Parse(path)
	if err != nil {
		return fmt.Errorf("failed to parth bucket creation path: %w", err)
	}

	body, err := json.Marshal(db)
	if err != nil {
		return fmt.Errorf("failed to marshal bucket creation request body: %w", err)
	}

	headers := http.Header{}
	headers.Set("Content-Type", "application/json")
	headers.Set("Accept", "application/json")
	// TODO confirm if need to add the Bearer token here or not

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
