package parquet

import (
	"bytes"
	"fmt"

	"github.com/parquet-go/parquet-go"
	"github.com/streamforge/pipeline/pkg/models"
)

// EventsToParquet writes a slice of UserActivityEvent into a Snappy compressed Parquet byte buffer
func EventsToParquet(events []models.UserActivityEvent) ([]byte, error) {
	if len(events) == 0 {
		return nil, fmt.Errorf("cannot write empty events slice to parquet")
	}

	buf := new(bytes.Buffer)

	// Configure Parquet writer with Snappy compression and optimized page sizing
	writer := parquet.NewGenericWriter[models.UserActivityEvent](buf,
		parquet.Compression(&parquet.Snappy),
		parquet.PageBufferSize(64*1024), // 64KB page buffer
	)

	_, err := writer.Write(events)
	if err != nil {
		return nil, fmt.Errorf("failed writing rows to parquet writer: %w", err)
	}

	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("failed closing parquet writer: %w", err)
	}

	return buf.Bytes(), nil
}

