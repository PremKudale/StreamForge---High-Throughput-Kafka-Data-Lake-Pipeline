package com.streamforge.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamforge.model.UserActivityEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;

@Service
public class PostgresUpsertService {

    private static final Logger log = LoggerFactory.getLogger(PostgresUpsertService.class);

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    public PostgresUpsertService(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public int[] batchUpsertEvents(List<UserActivityEvent> events, int partition, long startingOffset, String processorInstance) {
        if (events == null || events.isEmpty()) {
            return new int[0];
        }

        String insertSql = """
            INSERT INTO processed_events (
                event_id, user_id, event_type, timestamp_ms, event_time,
                ip_address, payload, device_type, session_id,
                kafka_partition, kafka_offset, processor_instance
            ) VALUES (
                ?, ?, ?, ?, ?,
                ?, ?::jsonb, ?, ?,
                ?, ?, ?
            ) ON CONFLICT (event_id) DO NOTHING
        """;

        int[] updateCounts = jdbcTemplate.batchUpdate(insertSql, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                UserActivityEvent e = events.get(i);
                ps.setString(1, e.getEventId());
                ps.setString(2, e.getUserId());
                ps.setString(3, e.getEventType());
                ps.setLong(4, e.getTimestampMs());
                ps.setTimestamp(5, Timestamp.from(Instant.ofEpochMilli(e.getTimestampMs())));
                ps.setString(6, e.getIpAddress());
                ps.setString(7, e.getPayload());
                ps.setString(8, e.getDeviceType());
                ps.setString(9, e.getSessionId());
                ps.setInt(10, partition);
                ps.setLong(11, startingOffset + i);
                ps.setString(12, processorInstance);
            }

            @Override
            public int getBatchSize() {
                return events.size();
            }
        });

        // Update aggregated analytical metrics for successfully inserted non-duplicate rows
        updateAggregates(events, updateCounts);

        return updateCounts;
    }

    private void updateAggregates(List<UserActivityEvent> events, int[] insertCounts) {
        String aggregateSql = """
            INSERT INTO user_activity_summary (
                user_id, window_date, total_events, page_views, purchases, searches, total_spend, last_active_at, updated_at
            ) VALUES (
                ?, ?, 1, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP
            ) ON CONFLICT (user_id, window_date) DO UPDATE SET
                total_events = user_activity_summary.total_events + 1,
                page_views = user_activity_summary.page_views + EXCLUDED.page_views,
                purchases = user_activity_summary.purchases + EXCLUDED.purchases,
                searches = user_activity_summary.searches + EXCLUDED.searches,
                total_spend = user_activity_summary.total_spend + EXCLUDED.total_spend,
                last_active_at = GREATEST(user_activity_summary.last_active_at, EXCLUDED.last_active_at),
                updated_at = CURRENT_TIMESTAMP
        """;

        for (int i = 0; i < events.size(); i++) {
            // Only update aggregate if row was actually inserted (insertCounts[i] > 0), avoiding duplicate inflation
            if (insertCounts[i] <= 0) {
                continue;
            }

            UserActivityEvent event = events.get(i);
            try {
                LocalDate date = Instant.ofEpochMilli(event.getTimestampMs()).atZone(ZoneOffset.UTC).toLocalDate();
                int isPageView = "PAGE_VIEW".equalsIgnoreCase(event.getEventType()) ? 1 : 0;
                int isPurchase = "PURCHASE".equalsIgnoreCase(event.getEventType()) ? 1 : 0;
                int isSearch = "SEARCH".equalsIgnoreCase(event.getEventType()) ? 1 : 0;

                BigDecimal spend = BigDecimal.ZERO;
                if (isPurchase == 1 && event.getPayload() != null) {
                    Map<?, ?> payloadMap = objectMapper.readValue(event.getPayload(), Map.class);
                    Object amt = payloadMap.get("amount");
                    if (amt != null) {
                        spend = new BigDecimal(amt.toString());
                    }
                }

                jdbcTemplate.update(aggregateSql,
                        event.getUserId(),
                        Date.valueOf(date),
                        isPageView,
                        isPurchase,
                        isSearch,
                        spend,
                        Timestamp.from(Instant.ofEpochMilli(event.getTimestampMs()))
                );
            } catch (Exception e) {
                log.warn("Failed to increment user summary for {}: {}", event.getUserId(), e.getMessage());
            }
        }
    }
}

