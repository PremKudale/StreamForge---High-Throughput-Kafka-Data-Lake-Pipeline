-- ============================================================================
-- StreamForge PostgreSQL Schema: Relational Serving & Idempotency Layer
-- ============================================================================

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Deduplication & Idempotent Event Tracking
CREATE TABLE IF NOT EXISTS processed_events (
    event_id VARCHAR(64) PRIMARY KEY,
    user_id VARCHAR(64) NOT NULL,
    event_type VARCHAR(32) NOT NULL,
    timestamp_ms BIGINT NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    ip_address VARCHAR(45),
    payload JSONB,
    device_type VARCHAR(32) DEFAULT 'UNKNOWN',
    session_id VARCHAR(64),
    kafka_partition INT NOT NULL,
    kafka_offset BIGINT NOT NULL,
    processed_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    processor_instance VARCHAR(128)
);

CREATE INDEX IF NOT EXISTS idx_processed_events_user_time 
    ON processed_events (user_id, event_time DESC);

CREATE INDEX IF NOT EXISTS idx_processed_events_type_time 
    ON processed_events (event_type, event_time DESC);

CREATE INDEX IF NOT EXISTS idx_processed_events_payload_gin 
    ON processed_events USING gin (payload);

-- Aggregated Real-time Analytics Table (Continuously Upserted)
CREATE TABLE IF NOT EXISTS user_activity_summary (
    user_id VARCHAR(64) NOT NULL,
    window_date DATE NOT NULL,
    total_events INT DEFAULT 0,
    page_views INT DEFAULT 0,
    purchases INT DEFAULT 0,
    searches INT DEFAULT 0,
    total_spend NUMERIC(12, 2) DEFAULT 0.00,
    last_active_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id, window_date)
);

CREATE INDEX IF NOT EXISTS idx_user_activity_summary_date 
    ON user_activity_summary (window_date DESC, total_events DESC);

-- Dead Letter Queue (DLQ) Audit Storage
CREATE TABLE IF NOT EXISTS dlq_events (
    id BIGSERIAL PRIMARY KEY,
    event_id VARCHAR(64),
    raw_payload TEXT,
    error_message TEXT NOT NULL,
    stack_trace TEXT,
    retry_count INT DEFAULT 0,
    source_topic VARCHAR(128) NOT NULL,
    source_partition INT NOT NULL,
    source_offset BIGINT NOT NULL,
    failed_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    reprocessed BOOLEAN DEFAULT FALSE,
    reprocessed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_dlq_events_unprocessed 
    ON dlq_events (reprocessed, failed_at DESC);

