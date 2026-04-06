/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Pg2IcebergSpec defines the desired state of a pg2iceberg replication pipeline.
type Pg2IcebergSpec struct {
	// suspend stops the pipeline by scaling the Deployment to 0 replicas.
	// +optional
	Suspend *bool `json:"suspend,omitempty"`

	// image overrides the pg2iceberg container image.
	// +optional
	// +kubebuilder:default="ghcr.io/pg2iceberg/pg2iceberg:latest"
	Image string `json:"image,omitempty"`

	// tables lists the PostgreSQL tables to replicate.
	// +kubebuilder:validation:MinItems=1
	Tables []TableSpec `json:"tables"`

	// source configures the PostgreSQL source.
	Source SourceSpec `json:"source"`

	// sink configures the Iceberg/S3 destination.
	Sink SinkSpec `json:"sink"`

	// state configures checkpoint storage. If unset, uses the source PostgreSQL.
	// +optional
	State *StateSpec `json:"state,omitempty"`

	// scaleToZero configures idle-based auto scale-down and wake-up.
	// +optional
	ScaleToZero *ScaleToZeroSpec `json:"scaleToZero,omitempty"`

	// metricsPort is the port for Prometheus metrics scraping.
	// +optional
	// +kubebuilder:default=9090
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	MetricsPort *int32 `json:"metricsPort,omitempty"`

	// resources for the pg2iceberg container.
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// goMemLimit overrides GOMEMLIMIT (e.g. "1600MiB").
	// If unset, auto-calculated as 80% of resources.limits.memory.
	// +optional
	GoMemLimit string `json:"goMemLimit,omitempty"`

	// extraEnv adds additional environment variables to the container.
	// +optional
	ExtraEnv []corev1.EnvVar `json:"extraEnv,omitempty"`

	// podTemplate allows overriding pod-level scheduling fields.
	// +optional
	PodTemplate *PodTemplateOverrides `json:"podTemplate,omitempty"`
}

// TableSpec describes a PostgreSQL table to replicate.
type TableSpec struct {
	// name is the fully-qualified table name (e.g. "public.orders").
	// +kubebuilder:validation:Pattern=`^[a-zA-Z_][a-zA-Z0-9_.]*$`
	Name string `json:"name"`

	// skipSnapshot skips the initial table snapshot (logical mode only).
	// +optional
	SkipSnapshot bool `json:"skipSnapshot,omitempty"`

	// primaryKey columns (required for query mode).
	// +optional
	PrimaryKey []string `json:"primaryKey,omitempty"`

	// watermarkColumn for incremental polling (required for query mode).
	// +optional
	WatermarkColumn string `json:"watermarkColumn,omitempty"`

	// partition defines Iceberg partition transforms.
	// Examples: "day(created_at)", "month(event_time)", "region" (identity).
	// +optional
	Partition []string `json:"partition,omitempty"`
}

// SourceSpec configures the PostgreSQL source.
type SourceSpec struct {
	// mode selects the replication mode.
	// +optional
	// +kubebuilder:default="logical"
	// +kubebuilder:validation:Enum=logical;query
	Mode string `json:"mode,omitempty"`

	// postgres connection configuration.
	Postgres PostgresSpec `json:"postgres"`

	// logical replication settings (required when mode=logical).
	// +optional
	Logical *LogicalSpec `json:"logical,omitempty"`

	// query mode polling settings (used when mode=query).
	// +optional
	Query *QuerySpec `json:"query,omitempty"`
}

// PostgresSpec defines the PostgreSQL connection.
type PostgresSpec struct {
	// host is the PostgreSQL server hostname or IP.
	Host string `json:"host"`

	// port is the PostgreSQL server port.
	// +optional
	// +kubebuilder:default=5432
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	Port *int32 `json:"port,omitempty"`

	// database name to connect to.
	Database string `json:"database"`

	// user for the PostgreSQL connection.
	User string `json:"user"`

	// passwordRef references a Secret key containing the PostgreSQL password.
	PasswordRef corev1.SecretKeySelector `json:"passwordRef"`
}

// LogicalSpec configures logical replication.
type LogicalSpec struct {
	// publicationName is the PostgreSQL publication to subscribe to.
	PublicationName string `json:"publicationName"`

	// slotName is the replication slot name.
	SlotName string `json:"slotName"`

	// snapshotConcurrency controls parallel table snapshot workers.
	// Defaults to GOMAXPROCS.
	// +optional
	// +kubebuilder:validation:Minimum=1
	SnapshotConcurrency *int32 `json:"snapshotConcurrency,omitempty"`

	// snapshotChunkPages is the number of PostgreSQL pages per CTID chunk
	// during the initial snapshot. Defaults to 2048 (~16MB at 8KB/page).
	// +optional
	// +kubebuilder:validation:Minimum=1
	SnapshotChunkPages *int32 `json:"snapshotChunkPages,omitempty"`

	// snapshotTargetFileSize is the target Parquet file size in bytes for
	// snapshot writes. Defaults to 128MB.
	// +optional
	SnapshotTargetFileSize *int64 `json:"snapshotTargetFileSize,omitempty"`

	// standbyInterval is how often to confirm LSN back to PostgreSQL (e.g. "10s").
	// +optional
	StandbyInterval string `json:"standbyInterval,omitempty"`
}

// QuerySpec configures query-mode polling.
type QuerySpec struct {
	// pollInterval is how often to poll for changes (e.g. "5s").
	// +optional
	// +kubebuilder:default="5s"
	PollInterval string `json:"pollInterval,omitempty"`
}

// SinkSpec configures the Iceberg sink.
type SinkSpec struct {
	// catalogURI is the Iceberg REST catalog endpoint.
	CatalogURI string `json:"catalogURI"`

	// catalogAuth selects the catalog authentication method ("", "sigv4", or "bearer").
	// +optional
	// +kubebuilder:validation:Enum="";sigv4;bearer
	CatalogAuth string `json:"catalogAuth,omitempty"`

	// catalogTokenRef references a Secret key containing the Bearer token for catalog auth.
	// Required when catalogAuth is "bearer".
	// +optional
	CatalogTokenRef *corev1.SecretKeySelector `json:"catalogTokenRef,omitempty"`

	// credentialMode selects how S3 storage credentials are obtained.
	// "static" (default) uses explicit S3 keys. "vended" uses temporary credentials
	// provided by the catalog in LoadTable responses.
	// +optional
	// +kubebuilder:default="static"
	// +kubebuilder:validation:Enum=static;vended
	CredentialMode string `json:"credentialMode,omitempty"`

	// warehouse is the Iceberg warehouse location (e.g. "s3://warehouse/").
	// Optional when credentialMode is "vended".
	// +optional
	Warehouse string `json:"warehouse,omitempty"`

	// namespace is the Iceberg catalog namespace.
	Namespace string `json:"namespace"`

	// s3 configures S3-compatible object storage.
	// Optional when credentialMode is "vended".
	// +optional
	S3 *S3Spec `json:"s3,omitempty"`

	// flushInterval controls the periodic flush timer (e.g. "10s").
	// +optional
	FlushInterval string `json:"flushInterval,omitempty"`

	// flushRows is the row count threshold that triggers a flush.
	// +optional
	// +kubebuilder:default=1000
	// +kubebuilder:validation:Minimum=1
	FlushRows *int32 `json:"flushRows,omitempty"`

	// flushBytes is the byte threshold that triggers a flush.
	// +optional
	FlushBytes *int64 `json:"flushBytes,omitempty"`

	// targetFileSize is the target Parquet data file size in bytes.
	// +optional
	TargetFileSize *int64 `json:"targetFileSize,omitempty"`

	// eventsPartition is the partition expression for events tables.
	// +optional
	// +kubebuilder:default="day(_ts)"
	EventsPartition string `json:"eventsPartition,omitempty"`

	// materializerInterval controls how often the materializer runs (e.g. "10s").
	// +optional
	MaterializerInterval string `json:"materializerInterval,omitempty"`

	// materializerTargetFileSize is the target file size for materialized tables.
	// +optional
	MaterializerTargetFileSize *int64 `json:"materializerTargetFileSize,omitempty"`

	// materializerConcurrency controls parallel I/O for materialization.
	// +optional
	// +kubebuilder:validation:Minimum=1
	MaterializerConcurrency *int32 `json:"materializerConcurrency,omitempty"`
}

// S3Spec configures S3-compatible object storage credentials.
type S3Spec struct {
	// endpoint is the S3 endpoint URL.
	Endpoint string `json:"endpoint"`

	// region is the S3 region.
	// +optional
	// +kubebuilder:default="us-east-1"
	Region string `json:"region,omitempty"`

	// accessKeyRef references a Secret key containing the S3 access key.
	AccessKeyRef corev1.SecretKeySelector `json:"accessKeyRef"`

	// secretKeyRef references a Secret key containing the S3 secret key.
	SecretKeyRef corev1.SecretKeySelector `json:"secretKeyRef"`
}

// StateSpec configures checkpoint persistence.
type StateSpec struct {
	// postgresURLRef references a Secret key containing a PostgreSQL DSN for checkpoint storage.
	// If unset, the source PostgreSQL is used.
	// +optional
	PostgresURLRef *corev1.SecretKeySelector `json:"postgresURLRef,omitempty"`
}

// ScaleToZeroSpec configures idle-based auto scale-down and wake-up.
type ScaleToZeroSpec struct {
	// enabled turns on idle-based scaling.
	Enabled bool `json:"enabled"`

	// idleTimeout is how long the pipeline must be idle before scaling to zero (e.g. "15m").
	// +optional
	// +kubebuilder:default="15m"
	IdleTimeout string `json:"idleTimeout,omitempty"`

	// wakeCheckInterval is how often to check PostgreSQL for new changes while scaled to zero (e.g. "30s").
	// +optional
	// +kubebuilder:default="30s"
	WakeCheckInterval string `json:"wakeCheckInterval,omitempty"`

	// walLagThresholdBytes is the minimum replication lag that triggers a wake-up.
	// Default 0 means any lag wakes up the pipeline.
	// +optional
	WALLagThresholdBytes *int64 `json:"walLagThresholdBytes,omitempty"`
}

// PodTemplateOverrides allows overriding pod scheduling fields.
type PodTemplateOverrides struct {
	// labels to add to the pod.
	// +optional
	Labels map[string]string `json:"labels,omitempty"`

	// annotations to add to the pod.
	// +optional
	Annotations map[string]string `json:"annotations,omitempty"`

	// nodeSelector for pod scheduling.
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`

	// tolerations for pod scheduling.
	// +optional
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`

	// affinity for pod scheduling.
	// +optional
	Affinity *corev1.Affinity `json:"affinity,omitempty"`
}

// Pg2IcebergStatus defines the observed state of a pg2iceberg pipeline.
type Pg2IcebergStatus struct {
	// conditions represent the current state of the resource.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// observedGeneration is the last generation reconciled.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// phase summarizes the pipeline lifecycle: Pending, Snapshotting, Running, Idle, Suspended, Error.
	// +optional
	Phase string `json:"phase,omitempty"`

	// pipelineStatus is the raw status from pg2iceberg /healthz (starting/snapshotting/running/error).
	// +optional
	PipelineStatus string `json:"pipelineStatus,omitempty"`

	// rowsProcessed is the total rows replicated.
	// +optional
	RowsProcessed int64 `json:"rowsProcessed,omitempty"`

	// replicationLSN is the last confirmed replication LSN.
	// +optional
	ReplicationLSN uint64 `json:"replicationLSN,omitempty"`

	// lastFlushAt is the timestamp of the last flush.
	// +optional
	LastFlushAt string `json:"lastFlushAt,omitempty"`

	// uptime is the pipeline process uptime.
	// +optional
	Uptime string `json:"uptime,omitempty"`

	// configHash is the SHA256 of the generated config.yaml.
	// +optional
	ConfigHash string `json:"configHash,omitempty"`

	// idleSince is the RFC3339 timestamp when idle was first detected.
	// +optional
	IdleSince string `json:"idleSince,omitempty"`

	// scaledToZero indicates the pipeline was scaled down due to inactivity.
	// +optional
	ScaledToZero bool `json:"scaledToZero,omitempty"`

	// lastWakeCheck is the RFC3339 timestamp of the last PostgreSQL poll for changes.
	// +optional
	LastWakeCheck string `json:"lastWakeCheck,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Pipeline Status",type=string,JSONPath=`.status.pipelineStatus`
// +kubebuilder:printcolumn:name="Rows",type=integer,JSONPath=`.status.rowsProcessed`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// Pg2Iceberg is the Schema for the pg2icebergs API.
type Pg2Iceberg struct {
	metav1.TypeMeta `json:",inline"`

	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// +required
	Spec Pg2IcebergSpec `json:"spec"`

	// +optional
	Status Pg2IcebergStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// Pg2IcebergList contains a list of Pg2Iceberg.
type Pg2IcebergList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []Pg2Iceberg `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Pg2Iceberg{}, &Pg2IcebergList{})
}
