package main

import (
	"context"
	"flag"
	"fmt"
	"net/url"
	"strings"

	"cloud.google.com/go/bigquery"

	"github.com/sirupsen/logrus"

	"github.com/droslean/ci-metrics-bigquery/pkg/metrics"
)

type options struct {
	projectID string
	datasetID string
	gcsPath   string
	bucket    string
	object    string
	exportDir string
}

func gatherOptions() *options {
	opts := &options{}
	flag.StringVar(&opts.projectID, "google-project-id", "", "GCP project ID")
	flag.StringVar(&opts.datasetID, "bigquery-dataset", "", "BigQuery dataset ID")
	flag.StringVar(&opts.gcsPath, "gcs-path", "", "Full GCS path to a specific metrics.json file")
	flag.StringVar(&opts.exportDir, "export", "", "Export data to directory as JSON files for manual BigQuery import (instead of writing to BigQuery)")
	flag.Parse()
	return opts
}

func validate(opts *options) error {
	if opts.gcsPath == "" {
		return fmt.Errorf("--gcs-path is required")
	}

	if opts.exportDir == "" {
		if opts.projectID == "" {
			return fmt.Errorf("--google-project-id is required")
		}
		if opts.datasetID == "" {
			return fmt.Errorf("--bigquery-dataset is required")
		}
	}
	return nil
}

func (o *options) complete() error {
	var err error
	o.bucket, o.object, err = parseGCSPath(o.gcsPath)
	if err != nil {
		return fmt.Errorf("invalid GCS path: %w", err)
	}
	return nil
}

func main() {
	opts := gatherOptions()

	if err := validate(opts); err != nil {
		logrus.Fatal(err)
	}

	if err := opts.complete(); err != nil {
		logrus.Fatal(err)
	}

	ctx := context.Background()

	if opts.exportDir != "" {
		if err := metrics.ExportMetricsFromGCS(ctx, opts.bucket, opts.object, opts.exportDir); err != nil {
			logrus.WithError(err).Fatal("Failed to export metrics from GCS")
		}
		return
	}

	bqClient, err := bigquery.NewClient(ctx, opts.projectID)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to create BigQuery client")
	}
	defer bqClient.Close()

	loader := metrics.NewBigQueryLoader(ctx, bqClient, opts.projectID, opts.datasetID)
	logrus.Infof("Loading metrics from %s into BigQuery dataset %s.%s", opts.gcsPath, opts.projectID, opts.datasetID)
	if err := loader.LoadFromGCS(opts.bucket, opts.object); err != nil {
		logrus.WithError(err).Fatal("Failed to load metrics from GCS")
	}
	logrus.Info("Successfully loaded metrics into BigQuery")
}

func parseGCSPath(gcsPath string) (bucket, object string, err error) {
	u, err := url.Parse(gcsPath)
	if err != nil {
		return "", "", fmt.Errorf("invalid GCS path: %w", err)
	}
	if u.Scheme != "gs" {
		return "", "", fmt.Errorf("path must use gs:// scheme, got %s://", u.Scheme)
	}
	if u.Host == "" {
		return "", "", fmt.Errorf("bucket name is required")
	}
	bucket = u.Host
	object = strings.TrimPrefix(u.Path, "/")
	if object == "" {
		return "", "", fmt.Errorf("object path is required")
	}
	return bucket, object, nil
}
