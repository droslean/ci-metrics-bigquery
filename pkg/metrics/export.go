package metrics

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/sirupsen/logrus"

	"cloud.google.com/go/storage"

	citoolsmetrics "github.com/openshift/ci-tools/pkg/metrics"
)

// ExportMetricsFromGCS reads metrics from GCS and exports them as JSON files for manual BigQuery import
func ExportMetricsFromGCS(ctx context.Context, bucket, object, exportDir string) error {
	gcsClient, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to create GCS client: %w", err)
	}
	defer gcsClient.Close()

	obj := gcsClient.Bucket(bucket).Object(object)
	reader, err := obj.NewReader(ctx)
	if err != nil {
		return fmt.Errorf("failed to open GCS object: %w", err)
	}
	defer reader.Close()

	var data MetricsData
	if err := json.NewDecoder(reader).Decode(&data); err != nil {
		return fmt.Errorf("failed to decode JSON from GCS: %w", err)
	}

	if err := os.MkdirAll(exportDir, 0755); err != nil {
		return fmt.Errorf("failed to create export directory: %w", err)
	}

	logger := logrus.WithField("component", "exportMetrics")
	logger.Infof("Exporting metrics to %s", exportDir)

	if len(data.Images) > 0 {
		if err := exportTable(exportDir, "images.json", data.Images); err != nil {
			return fmt.Errorf("failed to export images: %w", err)
		}
		logger.Infof("Exported %d images to images.json", len(data.Images))
	}

	if len(data.Nodes) > 0 {
		if err := exportTable(exportDir, "nodes.json", data.Nodes); err != nil {
			return fmt.Errorf("failed to export nodes: %w", err)
		}
		logger.Infof("Exported %d nodes to nodes.json", len(data.Nodes))
	}

	if len(data.Leases) > 0 {
		if err := exportTable(exportDir, "leases.json", data.Leases); err != nil {
			return fmt.Errorf("failed to export leases: %w", err)
		}
		logger.Infof("Exported %d leases to leases.json", len(data.Leases))
	}

	if len(data.OpenshiftBuilds) > 0 {
		if err := exportTable(exportDir, "openshift_builds.json", data.OpenshiftBuilds); err != nil {
			return fmt.Errorf("failed to export builds: %w", err)
		}
		logger.Infof("Exported %d builds to openshift_builds.json", len(data.OpenshiftBuilds))
	}

	if len(data.Pods) > 0 {
		if err := exportTable(exportDir, "pods.json", data.Pods); err != nil {
			return fmt.Errorf("failed to export pods: %w", err)
		}
		logger.Infof("Exported %d pods to pods.json", len(data.Pods))
	}

	if len(data.TestPlatformInsights) > 0 {
		if err := exportTable(exportDir, "test_platform_insights.json", data.TestPlatformInsights); err != nil {
			return fmt.Errorf("failed to export insights: %w", err)
		}
		logger.Infof("Exported %d insights to test_platform_insights.json", len(data.TestPlatformInsights))
	}

	if len(data.Events) > 0 {
		if err := exportTable(exportDir, "events.json", data.Events); err != nil {
			return fmt.Errorf("failed to export events: %w", err)
		}
		logger.Infof("Exported %d events to events.json", len(data.Events))
	}

	return nil
}

func exportTable(exportDir, filename string, data interface{}) error {
	filePath := filepath.Join(exportDir, filename)
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create %s: %w", filename, err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	switch v := data.(type) {
	case []*ImageEventUnion:
		for _, item := range v {
			if err := encoder.Encode(item); err != nil {
				return fmt.Errorf("failed to encode item: %w", err)
			}
		}
	case []*citoolsmetrics.NodeEvent:
		for _, item := range v {
			if err := encoder.Encode(item); err != nil {
				return fmt.Errorf("failed to encode item: %w", err)
			}
		}
	case []*LeaseEventUnion:
		for _, item := range v {
			if err := encoder.Encode(item); err != nil {
				return fmt.Errorf("failed to encode item: %w", err)
			}
		}
	case []*citoolsmetrics.BuildEvent:
		for _, item := range v {
			if err := encoder.Encode(item); err != nil {
				return fmt.Errorf("failed to encode item: %w", err)
			}
		}
	case []*citoolsmetrics.PodLifecycleMetricsEvent:
		for _, item := range v {
			if err := encoder.Encode(item); err != nil {
				return fmt.Errorf("failed to encode item: %w", err)
			}
		}
	case []*citoolsmetrics.InsightsEvent:
		for _, item := range v {
			if err := encoder.Encode(item); err != nil {
				return fmt.Errorf("failed to encode item: %w", err)
			}
		}
	case []*citoolsmetrics.Event:
		for _, item := range v {
			if err := encoder.Encode(item); err != nil {
				return fmt.Errorf("failed to encode item: %w", err)
			}
		}
	default:
		return fmt.Errorf("unsupported data type for export")
	}

	return nil
}
