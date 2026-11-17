-- ============================================================================
-- StreamForge Idempotency & Analytics Audit Helpers
-- ============================================================================

-- Function to record and aggregate an incoming event idempotently
CREATE OR REPLACE FUNCTION process_user_activity_event(
    p_event_id VARCHAR(64),
    p_user_id VARCHAR(64),
    p_event_type VARCHAR(32),
    p_timestamp_ms BIGINT,
    p_ip_address VARCHAR(45),
    p_payload JSONB,
    p_device_type VARCHAR(32),
    p_session_id VARCHAR(64),
    p_partition INT,
    p_offset BIGINT,
    p_instance VARCHAR(128)
) RETURNS BOOLEAN AS $$
DECLARE
    v_event_time TIMESTAMPTZ := to_timestamp(p_timestamp_ms / 1000.0);
    v_window_date DATE := (to_timestamp(p_timestamp_ms / 1000.0))::DATE;
    v_inserted BOOLEAN := FALSE;
    v_amount NUMERIC(12, 2) := COALESCE((p_payload->>'amount')::NUMERIC, 0.00);
BEGIN
    -- Idempotent insert into processed_events table
    INSERT INTO processed_events (
        event_id, user_id, event_type, timestamp_ms, event_time,
        ip_address, payload, device_type, session_id,
        kafka_partition, kafka_offset, processor_instance
    ) VALUES (
        p_event_id, p_user_id, p_event_type, p_timestamp_ms, v_event_time,
        p_ip_address, p_payload, COALESCE(p_device_type, 'UNKNOWN'), p_session_id,
        p_partition, p_offset, p_instance
    ) ON CONFLICT (event_id) DO NOTHING;

    GET DIAGNOSTICS v_inserted = ROW_COUNT;

    -- Only increment aggregate metrics if this event was fresh (not a duplicate replay)
    IF v_inserted THEN
        INSERT INTO user_activity_summary (
            user_id, window_date, total_events, page_views, purchases, searches, total_spend, last_active_at, updated_at
        ) VALUES (
            p_user_id,
            v_window_date,
            1,
            CASE WHEN p_event_type = 'PAGE_VIEW' THEN 1 ELSE 0 END,
            CASE WHEN p_event_type = 'PURCHASE' THEN 1 ELSE 0 END,
            CASE WHEN p_event_type = 'SEARCH' THEN 1 ELSE 0 END,
            CASE WHEN p_event_type = 'PURCHASE' THEN v_amount ELSE 0.00 END,
            v_event_time,
            CURRENT_TIMESTAMP
        )
        ON CONFLICT (user_id, window_date) DO UPDATE SET
            total_events = user_activity_summary.total_events + 1,
            page_views = user_activity_summary.page_views + (CASE WHEN EXCLUDED.purchases = 1 THEN 0 WHEN EXCLUDED.page_views = 1 THEN 1 ELSE 0 END),
            purchases = user_activity_summary.purchases + EXCLUDED.purchases,
            searches = user_activity_summary.searches + EXCLUDED.searches,
            total_spend = user_activity_summary.total_spend + EXCLUDED.total_spend,
            last_active_at = GREATEST(user_activity_summary.last_active_at, EXCLUDED.last_active_at),
            updated_at = CURRENT_TIMESTAMP;
            
        RETURN TRUE; -- successfully processed new event
    END IF;

    RETURN FALSE; -- duplicate ignored idempotently
END;
$$ LANGUAGE plpgsql;

-- Monitoring view: Ingestion throughput over the last 15 minutes
CREATE OR REPLACE VIEW view_ingestion_rate_per_minute AS
SELECT 
    date_trunc('minute', processed_at) AS minute_window,
    event_type,
    COUNT(*) AS total_processed,
    COUNT(DISTINCT user_id) AS unique_users
FROM processed_events
WHERE processed_at >= CURRENT_TIMESTAMP - INTERVAL '1 hour'
GROUP BY 1, 2
ORDER BY 1 DESC, 2;

-- Monitoring view: DLQ Summary
CREATE OR REPLACE VIEW view_dlq_error_summary AS
SELECT 
    source_topic,
    error_message,
    COUNT(*) AS error_count,
    MIN(failed_at) AS first_seen,
    MAX(failed_at) AS last_seen
FROM dlq_events
WHERE reprocessed = FALSE
GROUP BY source_topic, error_message
ORDER BY error_count DESC;

