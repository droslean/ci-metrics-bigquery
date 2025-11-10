package metrics

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"

	"github.com/sirupsen/logrus"
	"google.golang.org/api/googleapi"

	citoolsmetrics "github.com/openshift/ci-tools/pkg/metrics"
)

const (
	MetricsFileName = "ci-operator-metrics.json"
)

// LeaseEventUnion holds all fields from both LeaseAcquisitionMetricEvent and LeaseReleaseMetricEvent
type LeaseEventUnion struct {
	LeaseName                    string    `json:"name,omitempty"`
	Slice                        string    `json:"slice,omitempty"`
	Region                       string    `json:"region,omitempty"`
	RawLeaseName                 string    `json:"raw_lease_name,omitempty"`
	AcquisitionDurationSeconds   float64   `json:"acquisition_duration_seconds,omitempty"`
	ReleaseDurationSeconds       float64   `json:"release_duration_seconds,omitempty"`
	LeasesRemainingAtAcquisition int       `json:"leases_remaining_at_acquisition,omitempty"`
	LeasesAvailableAtRelease     int       `json:"leases_available_at_release,omitempty"`
	LeasesTotal                  int       `json:"leases_total,omitempty"`
	Released                     bool      `json:"released,omitempty"`
	Error                        string    `json:"error,omitempty"`
	Timestamp                    time.Time `json:"timestamp"`
}

// ImageEventUnion holds all fields from both ImageStreamEvent and TagImportEvent
type ImageEventUnion struct {
	Namespace          string         `json:"namespace,omitempty"`
	ImageStreamName    string         `json:"image_stream_name,omitempty"`
	FullName           string         `json:"full_name,omitempty"`
	TagName            string         `json:"tag_name,omitempty"`
	FullTagName        string         `json:"full_tag_name,omitempty"`
	SourceImage        string         `json:"source_image,omitempty"`
	SourceImageKind    string         `json:"source_image_kind,omitempty"`
	StartTime          time.Time      `json:"start_time,omitempty"`
	CompletionTime     time.Time      `json:"completion_time,omitempty"`
	DurationSeconds    float64        `json:"duration_seconds,omitempty"`
	RetryCount         int            `json:"retry_count,omitempty"`
	Success            bool           `json:"success,omitempty"`
	Error              string         `json:"error,omitempty"`
	ImageStreamDetails map[string]any `json:"image_stream_details,omitempty"`
	AdditionalContext  map[string]any `json:"additional_context,omitempty"`
	Timestamp          time.Time      `json:"timestamp"`
}

// MetricsData represents the complete metrics JSON structure
type MetricsData struct {
	Events               []*citoolsmetrics.Event                    `json:"events"`
	Images               []*ImageEventUnion                         `json:"images"`
	Leases               []*LeaseEventUnion                         `json:"leases"`
	Nodes                []*citoolsmetrics.NodeEvent                `json:"nodes"`
	OpenshiftBuilds      []*citoolsmetrics.BuildEvent               `json:"openshift_builds"`
	Pods                 []*citoolsmetrics.PodLifecycleMetricsEvent `json:"pods"`
	TestPlatformInsights []*citoolsmetrics.InsightsEvent            `json:"test_platform_insights"`
}

// BigQueryLoader handles loading metrics data into BigQuery
type BigQueryLoader struct {
	ctx       context.Context
	bqClient  *bigquery.Client
	projectID string
	datasetID string
	logger    *logrus.Entry
}

// NewBigQueryLoader creates a new BigQuery loader
func NewBigQueryLoader(ctx context.Context, bqClient *bigquery.Client, projectID, datasetID string) *BigQueryLoader {
	return &BigQueryLoader{
		ctx:       ctx,
		bqClient:  bqClient,
		projectID: projectID,
		datasetID: datasetID,
		logger:    logrus.WithField("component", "bigqueryLoader"),
	}
}

// LoadMetricsData loads the metrics file into BigQuery
func (b *BigQueryLoader) LoadMetricsData(data *MetricsData) error {
	dataset := b.bqClient.Dataset(b.datasetID)

	if err := b.loadImages(dataset, data.Images); err != nil {
		return fmt.Errorf("failed to load images: %w", err)
	}
	if err := b.loadNodes(dataset, data.Nodes); err != nil {
		return fmt.Errorf("failed to load nodes: %w", err)
	}
	if err := b.loadInsights(dataset, data.TestPlatformInsights); err != nil {
		return fmt.Errorf("failed to load insights: %w", err)
	}
	if err := b.loadLeases(dataset, data.Leases); err != nil {
		return fmt.Errorf("failed to load leases: %w", err)
	}
	if err := b.loadBuilds(dataset, data.OpenshiftBuilds); err != nil {
		return fmt.Errorf("failed to load builds: %w", err)
	}
	if err := b.loadPods(dataset, data.Pods); err != nil {
		return fmt.Errorf("failed to load pods: %w", err)
	}
	if err := b.loadEvents(dataset, data.Events); err != nil {
		return fmt.Errorf("failed to load events: %w", err)
	}

	return nil
}

// LoadFromGCS loads metrics from a GCS file
func (b *BigQueryLoader) LoadFromGCS(bucket, object string) error {
	gcsClient, err := storage.NewClient(b.ctx)
	if err != nil {
		return fmt.Errorf("failed to create GCS client: %w", err)
	}
	defer gcsClient.Close()

	obj := gcsClient.Bucket(bucket).Object(object)
	reader, err := obj.NewReader(b.ctx)
	if err != nil {
		return fmt.Errorf("failed to open GCS object: %w", err)
	}
	defer reader.Close()

	var data MetricsData
	if err := json.NewDecoder(reader).Decode(&data); err != nil {
		return fmt.Errorf("failed to decode JSON from GCS: %w", err)
	}

	return b.LoadMetricsData(&data)
}

func (b *BigQueryLoader) loadImages(dataset *bigquery.Dataset, images []*ImageEventUnion) error {
	if len(images) == 0 {
		return nil
	}

	table := dataset.Table("images")
	inserter := table.Inserter()

	schema, err := bigquery.InferSchema(ImageEventUnion{})
	if err != nil {
		return fmt.Errorf("failed to infer schema: %w", err)
	}

	if err := table.Create(b.ctx, &bigquery.TableMetadata{Schema: schema}); err != nil {
		if !isAlreadyExistsError(err) {
			return fmt.Errorf("failed to create table: %w", err)
		}
		b.logger.Debug("Table already exists")
	}

	if err := inserter.Put(b.ctx, images); err != nil {
		return fmt.Errorf("failed to insert images: %w", err)
	}

	b.logger.Infof("Loaded %d images into BigQuery", len(images))
	return nil
}

func (b *BigQueryLoader) loadNodes(dataset *bigquery.Dataset, nodes []*citoolsmetrics.NodeEvent) error {
	if len(nodes) == 0 {
		return nil
	}

	table := dataset.Table("nodes")
	inserter := table.Inserter()

	schema, err := bigquery.InferSchema(citoolsmetrics.NodeEvent{})
	if err != nil {
		return fmt.Errorf("failed to infer schema: %w", err)
	}

	if err := table.Create(b.ctx, &bigquery.TableMetadata{Schema: schema}); err != nil {
		if !isAlreadyExistsError(err) {
			return fmt.Errorf("failed to create table: %w", err)
		}
		b.logger.Debug("Table already exists")
	}

	if err := inserter.Put(b.ctx, nodes); err != nil {
		return fmt.Errorf("failed to insert nodes: %w", err)
	}

	b.logger.Infof("Loaded %d nodes into BigQuery", len(nodes))
	return nil
}

func (b *BigQueryLoader) loadInsights(dataset *bigquery.Dataset, insights []*citoolsmetrics.InsightsEvent) error {
	if len(insights) == 0 {
		return nil
	}

	table := dataset.Table("test_platform_insights")
	inserter := table.Inserter()

	schema, err := bigquery.InferSchema(citoolsmetrics.InsightsEvent{})
	if err != nil {
		return fmt.Errorf("failed to infer schema: %w", err)
	}

	if err := table.Create(b.ctx, &bigquery.TableMetadata{Schema: schema}); err != nil {
		if !isAlreadyExistsError(err) {
			return fmt.Errorf("failed to create table: %w", err)
		}
		b.logger.Debug("Table already exists")
	}

	if err := inserter.Put(b.ctx, insights); err != nil {
		return fmt.Errorf("failed to insert insights: %w", err)
	}

	b.logger.Infof("Loaded %d insights into BigQuery", len(insights))
	return nil
}

func (b *BigQueryLoader) loadLeases(dataset *bigquery.Dataset, leases []*LeaseEventUnion) error {
	if len(leases) == 0 {
		return nil
	}

	table := dataset.Table("leases")
	inserter := table.Inserter()

	schema, err := bigquery.InferSchema(LeaseEventUnion{})
	if err != nil {
		return fmt.Errorf("failed to infer schema: %w", err)
	}

	if err := table.Create(b.ctx, &bigquery.TableMetadata{Schema: schema}); err != nil {
		if !isAlreadyExistsError(err) {
			return fmt.Errorf("failed to create table: %w", err)
		}
		b.logger.Debug("Table already exists")
	}

	if err := inserter.Put(b.ctx, leases); err != nil {
		return fmt.Errorf("failed to insert leases: %w", err)
	}

	b.logger.Infof("Loaded %d leases into BigQuery", len(leases))
	return nil
}

func (b *BigQueryLoader) loadBuilds(dataset *bigquery.Dataset, builds []*citoolsmetrics.BuildEvent) error {
	if len(builds) == 0 {
		return nil
	}

	table := dataset.Table("openshift_builds")
	inserter := table.Inserter()

	schema, err := bigquery.InferSchema(citoolsmetrics.BuildEvent{})
	if err != nil {
		return fmt.Errorf("failed to infer schema: %w", err)
	}

	if err := table.Create(b.ctx, &bigquery.TableMetadata{Schema: schema}); err != nil {
		if !isAlreadyExistsError(err) {
			return fmt.Errorf("failed to create table: %w", err)
		}
		b.logger.Debug("Table already exists")
	}

	if err := inserter.Put(b.ctx, builds); err != nil {
		return fmt.Errorf("failed to insert builds: %w", err)
	}

	b.logger.Infof("Loaded %d builds into BigQuery", len(builds))
	return nil
}

func (b *BigQueryLoader) loadPods(dataset *bigquery.Dataset, pods []*citoolsmetrics.PodLifecycleMetricsEvent) error {
	if len(pods) == 0 {
		return nil
	}

	table := dataset.Table("pods")
	inserter := table.Inserter()

	schema, err := bigquery.InferSchema(citoolsmetrics.PodLifecycleMetricsEvent{})
	if err != nil {
		return fmt.Errorf("failed to infer schema: %w", err)
	}

	if err := table.Create(b.ctx, &bigquery.TableMetadata{Schema: schema}); err != nil {
		if !isAlreadyExistsError(err) {
			return fmt.Errorf("failed to create table: %w", err)
		}
		b.logger.Debug("Table already exists")
	}

	if err := inserter.Put(b.ctx, pods); err != nil {
		return fmt.Errorf("failed to insert pods: %w", err)
	}

	b.logger.Infof("Loaded %d pods into BigQuery", len(pods))
	return nil
}

func (b *BigQueryLoader) loadEvents(dataset *bigquery.Dataset, events []*citoolsmetrics.Event) error {
	if len(events) == 0 {
		return nil
	}

	table := dataset.Table("events")
	inserter := table.Inserter()

	schema, err := bigquery.InferSchema(citoolsmetrics.Event{})
	if err != nil {
		return fmt.Errorf("failed to infer schema: %w", err)
	}

	if err := table.Create(b.ctx, &bigquery.TableMetadata{Schema: schema}); err != nil {
		if !isAlreadyExistsError(err) {
			return fmt.Errorf("failed to create table: %w", err)
		}
		b.logger.Debug("Table already exists")
	}

	if err := inserter.Put(b.ctx, events); err != nil {
		return fmt.Errorf("failed to insert events: %w", err)
	}

	b.logger.Infof("Loaded %d events into BigQuery", len(events))
	return nil
}

func IsMetricsFile(name string) bool {
	return strings.HasSuffix(name, MetricsFileName)
}

// isAlreadyExistsError checks if the error indicates the resource already exists
// BigQuery returns HTTP 409 (Conflict) when a dataset or table already exists
func isAlreadyExistsError(err error) bool {
	if err == nil {
		return false
	}
	var apiErr *googleapi.Error
	if ok := errors.As(err, &apiErr); ok {
		return apiErr.Code == http.StatusConflict
	}
	return false
}
