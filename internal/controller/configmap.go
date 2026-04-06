package controller

import (
	"crypto/sha256"
	"fmt"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"

	replicationv1alpha1 "github.com/pg2iceberg/pg2iceberg-operator/api/v1alpha1"
)

// resolvedSecrets holds secret values resolved from SecretKeySelectors.
type resolvedSecrets struct {
	pgPassword   string
	s3AccessKey  string
	s3SecretKey  string
	catalogToken string // optional, for bearer auth
	statePostgre string // optional
}

// pg2icebergConfig mirrors the pg2iceberg config.yaml structure.
type pg2icebergConfig struct {
	Tables      []pg2icebergTable  `json:"tables"`
	Source      pg2icebergSource   `json:"source"`
	Sink        pg2icebergSink     `json:"sink"`
	State       *pg2icebergState   `json:"state,omitempty"`
	MetricsAddr string             `json:"metrics_addr,omitempty"`
}

type pg2icebergTable struct {
	Name            string   `json:"name"`
	SkipSnapshot    bool     `json:"skip_snapshot,omitempty"`
	PrimaryKey      []string `json:"primary_key,omitempty"`
	WatermarkColumn string   `json:"watermark_column,omitempty"`
	Iceberg         *pg2icebergTableIceberg `json:"iceberg,omitempty"`
}

type pg2icebergTableIceberg struct {
	Partition []string `json:"partition,omitempty"`
}

type pg2icebergSource struct {
	Mode     string              `json:"mode"`
	Postgres pg2icebergPostgres  `json:"postgres"`
	Logical  *pg2icebergLogical  `json:"logical,omitempty"`
	Query    *pg2icebergQuery    `json:"query,omitempty"`
}

type pg2icebergPostgres struct {
	Host     string `json:"host"`
	Port     int32  `json:"port"`
	Database string `json:"database"`
	User     string `json:"user"`
	Password string `json:"password"`
}

type pg2icebergLogical struct {
	PublicationName        string `json:"publication_name"`
	SlotName               string `json:"slot_name"`
	SnapshotConcurrency    int32  `json:"snapshot_concurrency,omitempty"`
	SnapshotChunkPages     int32  `json:"snapshot_chunk_pages,omitempty"`
	SnapshotTargetFileSize int64  `json:"snapshot_target_file_size,omitempty"`
	StandbyInterval        string `json:"standby_interval,omitempty"`
}

type pg2icebergQuery struct {
	PollInterval string `json:"poll_interval,omitempty"`
}

type pg2icebergSink struct {
	CatalogURI                 string `json:"catalog_uri"`
	CatalogAuth                string `json:"catalog_auth,omitempty"`
	CatalogToken               string `json:"catalog_token,omitempty"`
	CredentialMode             string `json:"credential_mode,omitempty"`
	Warehouse                  string `json:"warehouse,omitempty"`
	Namespace                  string `json:"namespace"`
	S3Endpoint                 string `json:"s3_endpoint,omitempty"`
	S3AccessKey                string `json:"s3_access_key,omitempty"`
	S3SecretKey                string `json:"s3_secret_key,omitempty"`
	S3Region                   string `json:"s3_region,omitempty"`
	FlushInterval              string `json:"flush_interval,omitempty"`
	FlushRows                  int32  `json:"flush_rows,omitempty"`
	FlushBytes                 int64  `json:"flush_bytes,omitempty"`
	TargetFileSize             int64  `json:"target_file_size,omitempty"`
	EventsPartition            string `json:"events_partition,omitempty"`
	MaterializerInterval       string `json:"materializer_interval,omitempty"`
	MaterializerTargetFileSize int64  `json:"materializer_target_file_size,omitempty"`
	MaterializerConcurrency    int32  `json:"materializer_concurrency,omitempty"`
}

type pg2icebergState struct {
	PostgresURL string `json:"postgres_url,omitempty"`
}

// buildConfigYAML generates a pg2iceberg config.yaml from the CR spec and resolved secrets.
func buildConfigYAML(spec *replicationv1alpha1.Pg2IcebergSpec, secrets resolvedSecrets) (string, error) {
	cfg := pg2icebergConfig{}

	// Tables
	for _, t := range spec.Tables {
		ct := pg2icebergTable{
			Name:            t.Name,
			SkipSnapshot:    t.SkipSnapshot,
			PrimaryKey:      t.PrimaryKey,
			WatermarkColumn: t.WatermarkColumn,
		}
		if len(t.Partition) > 0 {
			ct.Iceberg = &pg2icebergTableIceberg{Partition: t.Partition}
		}
		cfg.Tables = append(cfg.Tables, ct)
	}

	// Source
	mode := spec.Source.Mode
	if mode == "" {
		mode = "logical"
	}
	port := int32(5432)
	if spec.Source.Postgres.Port != nil {
		port = *spec.Source.Postgres.Port
	}
	cfg.Source = pg2icebergSource{
		Mode: mode,
		Postgres: pg2icebergPostgres{
			Host:     spec.Source.Postgres.Host,
			Port:     port,
			Database: spec.Source.Postgres.Database,
			User:     spec.Source.Postgres.User,
			Password: secrets.pgPassword,
		},
	}
	if spec.Source.Logical != nil {
		l := &pg2icebergLogical{
			PublicationName: spec.Source.Logical.PublicationName,
			SlotName:        spec.Source.Logical.SlotName,
			StandbyInterval: spec.Source.Logical.StandbyInterval,
		}
		if spec.Source.Logical.SnapshotConcurrency != nil {
			l.SnapshotConcurrency = *spec.Source.Logical.SnapshotConcurrency
		}
		if spec.Source.Logical.SnapshotChunkPages != nil {
			l.SnapshotChunkPages = *spec.Source.Logical.SnapshotChunkPages
		}
		if spec.Source.Logical.SnapshotTargetFileSize != nil {
			l.SnapshotTargetFileSize = *spec.Source.Logical.SnapshotTargetFileSize
		}
		cfg.Source.Logical = l
	}
	if spec.Source.Query != nil {
		cfg.Source.Query = &pg2icebergQuery{
			PollInterval: spec.Source.Query.PollInterval,
		}
	}

	// Sink
	cfg.Sink = pg2icebergSink{
		CatalogURI:                 spec.Sink.CatalogURI,
		CatalogAuth:                spec.Sink.CatalogAuth,
		CatalogToken:               secrets.catalogToken,
		CredentialMode:             spec.Sink.CredentialMode,
		Warehouse:                  spec.Sink.Warehouse,
		Namespace:                  spec.Sink.Namespace,
		FlushInterval:              spec.Sink.FlushInterval,
		EventsPartition:            spec.Sink.EventsPartition,
		MaterializerInterval:       spec.Sink.MaterializerInterval,
	}
	if spec.Sink.S3 != nil {
		cfg.Sink.S3Endpoint = spec.Sink.S3.Endpoint
		cfg.Sink.S3AccessKey = secrets.s3AccessKey
		cfg.Sink.S3SecretKey = secrets.s3SecretKey
		cfg.Sink.S3Region = spec.Sink.S3.Region
	}
	if spec.Sink.FlushRows != nil {
		cfg.Sink.FlushRows = *spec.Sink.FlushRows
	}
	if spec.Sink.FlushBytes != nil {
		cfg.Sink.FlushBytes = *spec.Sink.FlushBytes
	}
	if spec.Sink.TargetFileSize != nil {
		cfg.Sink.TargetFileSize = *spec.Sink.TargetFileSize
	}
	if spec.Sink.MaterializerTargetFileSize != nil {
		cfg.Sink.MaterializerTargetFileSize = *spec.Sink.MaterializerTargetFileSize
	}
	if spec.Sink.MaterializerConcurrency != nil {
		cfg.Sink.MaterializerConcurrency = *spec.Sink.MaterializerConcurrency
	}

	// Metrics address
	metricsPort := int32(9090)
	if spec.MetricsPort != nil {
		metricsPort = *spec.MetricsPort
	}
	cfg.MetricsAddr = ":" + strconv.Itoa(int(metricsPort))

	// State
	if secrets.statePostgre != "" {
		cfg.State = &pg2icebergState{PostgresURL: secrets.statePostgre}
	}

	data, err := yaml.Marshal(cfg)
	if err != nil {
		return "", fmt.Errorf("marshal config: %w", err)
	}
	return string(data), nil
}

// configHash returns the SHA256 hex digest of a config string.
func configHash(config string) string {
	h := sha256.Sum256([]byte(config))
	return fmt.Sprintf("%x", h[:8]) // 16-char prefix is sufficient
}

// buildConfigMap creates the ConfigMap containing config.yaml.
func buildConfigMap(cr *replicationv1alpha1.Pg2Iceberg, configYAML string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cr.Name + "-config",
			Namespace: cr.Namespace,
			Labels:    commonLabels(cr),
		},
		Data: map[string]string{
			"config.yaml": configYAML,
		},
	}
}

// commonLabels returns standard labels applied to all owned resources.
func commonLabels(cr *replicationv1alpha1.Pg2Iceberg) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":       "pg2iceberg",
		"app.kubernetes.io/instance":   cr.Name,
		"app.kubernetes.io/managed-by": "pg2iceberg-operator",
	}
}
