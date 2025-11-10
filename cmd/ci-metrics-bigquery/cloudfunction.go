//go:build cloudfunction
// +build cloudfunction

package main

import (
	"context"
	"fmt"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"

	"github.com/sirupsen/logrus"

	"github.com/droslean/ci-metrics-bigquery/pkg/metrics"
)

const (
	ProjectName = "openshift-gce-devel"
	DatasetName = "ci_operator_metrics"
)

// LoadMetricsFromGCS is the Cloud Function entry point
func LoadMetricsFromGCS(ctx context.Context, e storage.Event) error {
	logger := logrus.WithField("bucket", e.Bucket).WithField("name", e.Name)

	if !metrics.IsMetricsFile(e.Name) {
		logger.Error("Received non-metrics file")
		return fmt.Errorf("unexpected file received: %s (expected ci-operator-metrics.json)", e.Name)
	}

	logger.Infof("Processing metrics file: gs://%s/%s", e.Bucket, e.Name)

	bqClient, err := bigquery.NewClient(ctx, ProjectName)
	if err != nil {
		logger.WithError(err).Error("Failed to create BigQuery client")
		return fmt.Errorf("failed to create BigQuery client: %w", err)
	}
	defer bqClient.Close()

	if err := metrics.NewBigQueryLoader(ctx, bqClient, ProjectName, DatasetName).LoadFromGCS(e.Bucket, e.Name); err != nil {
		logger.WithError(err).Error("Failed to load metrics from GCS")
		return fmt.Errorf("failed to load metrics: %w", err)
	}

	logger.Info("Successfully loaded metrics into BigQuery")
	return nil
}
