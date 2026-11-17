package models

import (
	"encoding/json"
	"time"
)

// UserActivityEvent represents the standardized event payload in the pipeline
type UserActivityEvent struct {
	EventID     string            `json:"event_id" parquet:"event_id,snappy"`
	UserID      string            `json:"user_id" parquet:"user_id,snappy"`
	EventType   string            `json:"event_type" parquet:"event_type,snappy"`
	TimestampMs int64             `json:"timestamp_ms" parquet:"timestamp_ms"`
	EventTime   string            `json:"event_time" parquet:"event_time,snappy"`
	IPAddress   string            `json:"ip_address" parquet:"ip_address,snappy"`
	Payload     string            `json:"payload" parquet:"payload,snappy"`
	DeviceType  string            `json:"device_type" parquet:"device_type,snappy"`
	SessionID   string            `json:"session_id,omitempty" parquet:"session_id,snappy"`
	Tags        map[string]string `json:"tags,omitempty" parquet:"-"`
}

// DeadLetterEnvelope wraps a failed message with diagnostic audit metadata
type DeadLetterEnvelope struct {
	EventID         string `json:"event_id"`
	OriginalTopic   string `json:"original_topic"`
	Partition       int    `json:"partition"`
	Offset          int64  `json:"offset"`
	ErrorMessage    string `json:"error_message"`
	StackTrace      string `json:"stack_trace,omitempty"`
	RetryCount      int    `json:"retry_count"`
	FailedAt        string `json:"failed_at"`
	RawPayload      string `json:"raw_payload"`
	ProcessorHost   string `json:"processor_host"`
	TraceID         string `json:"trace_id,omitempty"`
}

// Validate checks for mandatory fields
func (e *UserActivityEvent) Validate() error {
	if e.EventID == "" {
		return &ValidationError{Field: "event_id", Message: "event_id is required"}
	}
	if e.UserID == "" {
		return &ValidationError{Field: "user_id", Message: "user_id is required"}
	}
	if e.EventType == "" {
		return &ValidationError{Field: "event_type", Message: "event_type is required"}
	}
	if e.TimestampMs == 0 {
		e.TimestampMs = time.Now().UnixMilli()
	}
	if e.EventTime == "" {
		e.EventTime = time.UnixMilli(e.TimestampMs).UTC().Format(time.RFC3339)
	}
	if e.DeviceType == "" {
		e.DeviceType = "UNKNOWN"
	}
	return nil
}

type ValidationError struct {
	Field   string
	Message string
}

func (v *ValidationError) Error() string {
	return v.Field + ": " + v.Message
}

// ToJSON serializes the event to JSON bytes
func (e *UserActivityEvent) ToJSON() ([]byte, error) {
	return json.Marshal(e)
}

