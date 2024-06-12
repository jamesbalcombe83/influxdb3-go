package influxdb3

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
