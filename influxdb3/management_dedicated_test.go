package influxdb3

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDedicatedClientCreateDatabase(t *testing.T) {
	correctPath := fmt.Sprintf("/api/v0/accounts/%s/clusters/%s/databases", "test-account", "test-cluster")

	tests := []struct {
		name     string
		db       *Database
		wantBody map[string]any
		wantErr  bool
	}{
		{
			name: "create database with name and defaults",
			db: &Database{
				Name:              "test-database",
				PartitionTemplate: []PartitionTemplate{},
			},
			wantBody: map[string]any{
				"name":               "default-database",
				"maxTables":          float64(500),
				"maxColumnsPerTable": float64(250),
				"retentionPeriod":    float64(0),
				"partitionTemplate":  []any{},
			},
			wantErr: false,
		},
		{
			name: "create database with name and custom values",
			db: &Database{
				Name:               "test-database",
				MaxTables:          1000,
				MaxColumnsPerTable: 500,
				RetentionPeriod:    1000,
				PartitionTemplate: []PartitionTemplate{
					Tag{
						Type:  "tag",
						Value: "tag-value",
					},
					TagBucket{
						Type: "tag",
						Value: TagBucketValue{
							TagName:         "tagName",
							NumberOfBuckets: 3,
						},
					},
				},
			},
			wantBody: map[string]any{
				"name":               "default-database",
				"maxTables":          float64(1000),
				"maxColumnsPerTable": float64(500),
				"retentionPeriod":    float64(1000),
				"partitionTemplate": []any{
					map[string]any{
						"type":  "tag",
						"value": "tag-value",
					},
					map[string]any{
						"type": "tag",
						"value": map[string]any{
							"tagName":         "tagName",
							"numberOfBuckets": float64(3),
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name:     "nil database",
			db:       nil,
			wantBody: map[string]any{},
			wantErr:  true,
		},
		{
			name: "db partition template has more than 7 tags",
			db: &Database{
				Name: "test-database",
				PartitionTemplate: []PartitionTemplate{
					Tag{},
					Tag{},
					Tag{},
					Tag{},
					Tag{},
					Tag{},
					Tag{},
					Tag{},
				},
			},
			wantBody: map[string]any{},
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				// initialization of query client
				if r.Method == "PRI" {
					return
				}

				assert.EqualValues(t, correctPath, r.URL.String())
				bodyBytes, err := io.ReadAll(r.Body)
				require.NoError(t, err)
				var body map[string]any
				err = json.Unmarshal(bodyBytes, &body)
				require.NoError(t, err)
				assert.Equal(t, tt.wantBody, body)
				w.WriteHeader(201)
			}))

			c, err := New(ClientConfig{
				Host:         ts.URL,
				Token:        "my-token",
				Organization: "default-organization",
				Database:     "default-database",
			})
			require.NoError(t, err)

			dc := NewCloudDedicatedClient(c)
			err = dc.CreateDatabase(context.Background(), tt.db, "test-account", "test-cluster")
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}

	t.Run("Internal error cases", func(t *testing.T) {
		c, err := New(ClientConfig{
			Host:  "dummy",
			Token: "dummy",
		})
		require.NoError(t, err)

		dc := NewCloudDedicatedClient(c)
		err = dc.createDatabase(context.Background(), "wrong path:", nil)
		assert.Error(t, err)

		wrongBody := map[string]any{
			"funcField": func() {},
		}
		err = dc.createDatabase(context.Background(), correctPath, wrongBody)
		assert.Error(t, err)
	})

}
