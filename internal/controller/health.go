package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// healthResponse mirrors the pg2iceberg /healthz JSON response.
type healthResponse struct {
	Status        string `json:"status"`
	BufferedRows  int    `json:"buffered_rows"`
	BufferedBytes int64  `json:"buffered_bytes"`
	RowsProcessed int64  `json:"rows_processed"`
	BytesProcessed int64 `json:"bytes_processed"`
	LSN           uint64 `json:"lsn,omitempty"`
	LastFlushAt   string `json:"last_flush_at,omitempty"`
	Uptime        string `json:"uptime,omitempty"`
}

// pollHealth queries the pg2iceberg /healthz endpoint via the metrics Service.
func pollHealth(ctx context.Context, serviceName, namespace string, port int32) (*healthResponse, error) {
	url := fmt.Sprintf("http://%s.%s.svc.cluster.local:%d/healthz", serviceName, namespace, port)

	client := &http.Client{Timeout: 5 * time.Second}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("health check: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read health response: %w", err)
	}

	var health healthResponse
	if err := json.Unmarshal(body, &health); err != nil {
		return nil, fmt.Errorf("parse health response: %w", err)
	}

	return &health, nil
}
