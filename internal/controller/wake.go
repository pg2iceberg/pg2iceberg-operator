package controller

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	replicationv1alpha1 "github.com/pg2iceberg/pg2iceberg-operator/api/v1alpha1"
)

// checkWake connects to PostgreSQL and checks whether the pipeline should wake up.
// For logical mode, it checks replication slot lag. For query mode, it checks watermark.
func checkWake(ctx context.Context, spec *replicationv1alpha1.Pg2IcebergSpec, secrets resolvedSecrets) (bool, error) {
	port := int32(5432)
	if spec.Source.Postgres.Port != nil {
		port = *spec.Source.Postgres.Port
	}

	dsn := fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s sslmode=disable",
		spec.Source.Postgres.Host, port,
		spec.Source.Postgres.Database, spec.Source.Postgres.User,
		secrets.pgPassword)

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return false, fmt.Errorf("open pg connection: %w", err)
	}
	defer db.Close()

	mode := spec.Source.Mode
	if mode == "" {
		mode = "logical"
	}

	switch mode {
	case "logical":
		return checkWakeLogical(ctx, db, spec)
	case "query":
		return checkWakeQuery(ctx, db, spec)
	default:
		return false, fmt.Errorf("unknown source mode: %s", mode)
	}
}

// checkWakeLogical checks replication slot lag to determine if there are pending WAL changes.
func checkWakeLogical(ctx context.Context, db *sql.DB, spec *replicationv1alpha1.Pg2IcebergSpec) (bool, error) {
	if spec.Source.Logical == nil {
		return false, fmt.Errorf("logical spec is nil")
	}

	var lagBytes int64
	err := db.QueryRowContext(ctx, `
		SELECT COALESCE(pg_current_wal_lsn() - confirmed_flush_lsn, 0)
		FROM pg_replication_slots
		WHERE slot_name = $1
	`, spec.Source.Logical.SlotName).Scan(&lagBytes)
	if err != nil {
		return false, fmt.Errorf("query replication slot lag: %w", err)
	}

	threshold := int64(0)
	if spec.ScaleToZero != nil && spec.ScaleToZero.WALLagThresholdBytes != nil {
		threshold = *spec.ScaleToZero.WALLagThresholdBytes
	}

	return lagBytes > threshold, nil
}

// checkWakeQuery checks if new rows exist past the last watermark for any table.
func checkWakeQuery(ctx context.Context, db *sql.DB, spec *replicationv1alpha1.Pg2IcebergSpec) (bool, error) {
	// Read the last watermark from the pg2iceberg checkpoint table.
	var watermark string
	err := db.QueryRowContext(ctx, `
		SELECT data->>'watermark'
		FROM _pg2iceberg.checkpoints
		ORDER BY updated_at DESC
		LIMIT 1
	`).Scan(&watermark)
	if err != nil {
		// If no checkpoint exists, there's definitely data to process.
		return true, nil
	}
	if watermark == "" {
		return true, nil
	}

	// Check each table for rows newer than the watermark.
	for _, t := range spec.Tables {
		if t.WatermarkColumn == "" {
			continue
		}
		var exists bool
		query := fmt.Sprintf(
			"SELECT EXISTS(SELECT 1 FROM %s WHERE %s > $1 LIMIT 1)",
			t.Name, t.WatermarkColumn,
		)
		if err := db.QueryRowContext(ctx, query, watermark).Scan(&exists); err != nil {
			return false, fmt.Errorf("check watermark for %s: %w", t.Name, err)
		}
		if exists {
			return true, nil
		}
	}

	return false, nil
}
