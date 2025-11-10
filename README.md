# CI Metrics BigQuery Loader

Loads CI operator metrics from Google Cloud Storage into BigQuery for analytics.

## Overview

This tool processes `ci-operator-metrics.json` files from GCS and loads them into BigQuery tables. It can run as:
- **Google Cloud Function**: Automatically triggered when metrics files are uploaded to GCS
- **CLI Tool**: Manual execution for testing or one-off imports

## Features

- Automatically splits metrics into separate BigQuery tables (images, nodes, leases, builds, pods, events, insights)
- Supports union types for polymorphic metrics (leases, images)
- Creates tables automatically with schema inference
- Export mode for manual BigQuery import

## Usage

### CLI Tool

Load metrics into BigQuery:

```bash
go run ./cmd/ci-metrics-bigquery \
  --google-project-id=openshift-gce-devel \
  --bigquery-dataset=ci_operator_metrics \
  --gcs-path=gs://bucket/path/to/ci-operator-metrics.json
```

Export metrics as JSON files for manual import:

```bash
go run ./cmd/ci-metrics-bigquery \
  --gcs-path=gs://bucket/path/to/ci-operator-metrics.json \
  --export=./exported_metrics
```

## BigQuery Tables

The tool creates the following tables in the specified dataset:
- `images` - Image stream and tag import events
- `nodes` - Node events
- `leases` - Lease acquisition and release events
- `openshift_builds` - Build events
- `pods` - Pod lifecycle events
- `events` - General events
- `test_platform_insights` - Test platform insights

Tables are created automatically on first use. The dataset must exist beforehand.

## Build Tags

- Normal build: Includes `main.go` (CLI tool)
- Build with `-tags cloudfunction`: Includes `cloudfunction.go` (Cloud Function)

The Cloud Function deployment automatically uses the `cloudfunction` build tag.
