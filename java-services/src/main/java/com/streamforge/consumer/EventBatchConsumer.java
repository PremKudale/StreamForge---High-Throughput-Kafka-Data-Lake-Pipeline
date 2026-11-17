package com.streamforge.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamforge.model.UserActivityEvent;
import com.streamforge.service.DeadLetterQueueService;
import com.streamforge.service.PostgresUpsertService;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

@Component
public class EventBatchConsumer {

    private static final Logger log = LoggerFactory.getLogger(EventBatchConsumer.class);

    private final PostgresUpsertService upsertService;
    private final DeadLetterQueueService dlqService;
    private final ObjectMapper objectMapper;
    private final Tracer tracer;
    private final Counter processedCounter;
    private final Counter duplicateCounter;
    private final Counter errorCounter;
    private final Timer batchTimer;
    private final String hostname;

    public EventBatchConsumer(PostgresUpsertService upsertService,
                              DeadLetterQueueService dlqService,
                              ObjectMapper objectMapper,
                              Tracer tracer,
                              MeterRegistry meterRegistry) {
        this.upsertService = upsertService;
        this.dlqService = dlqService;
        this.objectMapper = objectMapper;
        this.tracer = tracer;

        this.processedCounter = meterRegistry.counter("streamforge.consumer.events.processed");
        this.duplicateCounter = meterRegistry.counter("streamforge.consumer.events.duplicates");
        this.errorCounter = meterRegistry.counter("streamforge.consumer.events.errors");
        this.batchTimer = meterRegistry.timer("streamforge.batch.processing.time");

        String host;
        try {
            host = InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            host = "java-consumer-pod";
        }
        this.hostname = host;
    }

    @KafkaListener(
            topics = "${streamforge.topics.main:streamforge.user.activity}",
            containerFactory = "kafkaListenerContainerFactory",
            groupId = "${spring.kafka.consumer.group-id:streamforge-etl-group}"
    )
    public void consumeBatch(List<ConsumerRecord<String, String>> records, Acknowledgment acknowledgment) {
        if (records == null || records.isEmpty()) {
            acknowledgment.acknowledge();
            return;
        }

        Span span = tracer.spanBuilder("kafka.consume_batch")
                .setAttribute("messaging.batch_size", records.size())
                .setAttribute("messaging.source_topic", records.get(0).topic())
                .startSpan();

        try {
            batchTimer.record(() -> {
                List<UserActivityEvent> validEvents = new ArrayList<>(records.size());
                int partition = records.get(0).partition();
                long startingOffset = records.get(0).offset();

                for (ConsumerRecord<String, String> record : records) {
                    try {
                        UserActivityEvent event = objectMapper.readValue(record.value(), UserActivityEvent.class);
                        if (event.getEventId() == null || event.getEventId().isBlank()) {
                            event.setEventId(record.key() != null ? record.key() : "gen-" + record.offset());
                        }
                        validEvents.add(event);
                    } catch (Exception ex) {
                        errorCounter.increment();
                        dlqService.routeToDeadLetterQueue(
                                record.topic(),
                                record.partition(),
                                record.offset(),
                                record.key(),
                                record.value(),
                                ex
                        );
                    }
                }

                if (!validEvents.isEmpty()) {
                    int[] results = upsertService.batchUpsertEvents(validEvents, partition, startingOffset, hostname);
                    for (int res : results) {
                        if (res > 0) {
                            processedCounter.increment();
                        } else {
                            duplicateCounter.increment();
                        }
                    }
                }

                // Explicit manual acknowledgment after DB commit
                acknowledgment.acknowledge();
            });

            span.setStatus(StatusCode.OK);
        } catch (Exception e) {
            log.error("Unhandled exception during batch consumption", e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            span.recordException(e);
            throw e; // Triggers Spring Kafka retry and DeadLetterPublishingRecoverer
        } finally {
            span.end();
        }
    }
}

