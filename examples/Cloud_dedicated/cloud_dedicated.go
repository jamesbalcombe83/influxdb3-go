package main

import (
	"context"
	"fmt"
	"os"

	"github.com/InfluxCommunity/influxdb3-go/influxdb3"
)

func main() {
	// Retrieve credentials from environment variables.
	url := os.Getenv("INFLUX_URL")
	token := os.Getenv("INFLUX_TOKEN")
	database := os.Getenv("INFLUX_DATABASE")
	managementToken := os.Getenv("INFLUX_MANAGEMENT_TOKEN")
	accountID := os.Getenv("INFLUX_ACCOUNT_ID")
	clusterID := os.Getenv("INFLUX_CLUSTER_ID")

	// Instantiate a client using your credentials.
	client, err := influxdb3.New(influxdb3.ClientConfig{
		Host:     url,
		Token:    token,
		Database: database,
	})
	if err != nil {
		panic(err)
	}

	cloudDedicatedConfig := &influxdb3.DedicatedClientConfig{
		AccountID:       accountID,
		ClusterID:       clusterID,
		ManagementToken: managementToken,
	}

	// Close the client when finished and raise any errors.
	defer func(client *influxdb3.Client) {
		err := client.Close()
		if err != nil {
			panic(err)
		}
	}(client)

	dc := influxdb3.NewCloudDedicatedClient(client)
	if err := dc.CreateDatabase(context.Background(), cloudDedicatedConfig, &influxdb3.Database{}); err != nil {
		panic(fmt.Errorf("failed to create database: %w", err))
	}
	fmt.Println("Database created successfully")
}
