package com.streamforge.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

@Service
public class DeadLetterQueueService {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterQueueService.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    @Value("${streamforge.topics.dlq:streamforge.events.dlq}")
    private String dlqTopic;

    public DeadLetterQueueService(KafkaTemplate<String, String> kafkaTemplate,
                                  JdbcTemplate jdbcTemplate,
                                  ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
    }

    public void routeToDeadLetterQueue(String originalTopic, int partition, long offset,
                                       String key, String rawPayload, Throwable ex) {
        String errorMsg = ex != null ? ex.getMessage() : "Unknown processing error";
        String stackTrace = getStackTraceAsString(ex);

        log.warn("Routing message to DLQ [topic={}, partition={}, offset={}, key={}]: {}",
                originalTopic, partition, offset, key, errorMsg);

        try {
            Map<String, Object> envelope = new HashMap<>();
            envelope.put("event_id", key);
            envelope.put("original_topic", originalTopic);
            envelope.put("partition", partition);
            envelope.put("offset", offset);
            envelope.put("error_message", errorMsg);
            envelope.put("stack_trace", stackTrace);
            envelope.put("failed_at", Instant.now().toString());
            envelope.put("raw_payload", rawPayload);

            String envelopeJson = objectMapper.writeValueAsString(envelope);

            // 1. Send to Kafka DLQ Topic
            ProducerRecord<String, String> record = new ProducerRecord<>(dlqTopic, key, envelopeJson);
            record.headers().add(new RecordHeader("dlq.original_topic", originalTopic.getBytes(StandardCharsets.UTF_8)));
            record.headers().add(new RecordHeader("dlq.error_reason", errorMsg.getBytes(StandardCharsets.UTF_8)));

            kafkaTemplate.send(record);

            // 2. Persist audit trail in PostgreSQL
            persistDlqAudit(key, rawPayload, errorMsg, stackTrace, originalTopic, partition, offset);

        } catch (Exception e) {
            log.error("Critical: Failed to route message to DLQ topic or audit table", e);
        }
    }

    private void persistDlqAudit(String key, String rawPayload, String errorMsg, String stackTrace,
                                 String topic, int partition, long offset) {
        try {
            String sql = """
                INSERT INTO dlq_events (
                    event_id, raw_payload, error_message, stack_trace,
                    source_topic, source_partition, source_offset
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """;
            jdbcTemplate.update(sql, key, rawPayload, errorMsg, stackTrace, topic, partition, offset);
        } catch (Exception e) {
            log.warn("Could not write DLQ record to postgres table: {}", e.getMessage());
        }
    }

    private String getStackTraceAsString(Throwable throwable) {
        if (throwable == null) return "";
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        throwable.printStackTrace(pw);
        return sw.toString();
    }
}

