package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/snappy"
	"github.com/streamforge/pipeline/pkg/models"
	"github.com/streamforge/pipeline/pkg/telemetry"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// ProducerConfig encapsulates tuning settings for high throughput & reliability
type ProducerConfig struct {
	Brokers       []string
	Topic         string
	DLQTopic      string
	BatchSize     int
	BatchTimeout  time.Duration
	Async         bool
	MaxAttempts   int
}

// DefaultProducerConfig returns production-tuned defaults
func DefaultProducerConfig() ProducerConfig {
	brokers := []string{"localhost:9092"}
	if envBrokers := os.Getenv("KAFKA_BROKERS"); envBrokers != "" {
		brokers = []string{envBrokers}
	}
	topic := os.Getenv("KAFKA_EVENT_TOPIC")
	if topic == "" {
		topic = "streamforge.user.activity"
	}
	dlqTopic := os.Getenv("KAFKA_DLQ_TOPIC")
	if dlqTopic == "" {
		dlqTopic = "streamforge.events.dlq"
	}

	batchSize := 1000
	if bs := os.Getenv("KAFKA_PRODUCER_BATCH_SIZE"); bs != "" {
		if parsed, err := strconv.Atoi(bs); err == nil {
			batchSize = parsed
		}
	}

	lingerMs := 10 * time.Millisecond
	if l := os.Getenv("KAFKA_PRODUCER_LINGER_MS"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil {
			lingerMs = time.Duration(parsed) * time.Millisecond
		}
	}

	return ProducerConfig{
		Brokers:      brokers,
		Topic:        topic,
		DLQTopic:     dlqTopic,
		BatchSize:    batchSize,
		BatchTimeout: lingerMs,
		Async:        false,
		MaxAttempts:  5,
	}
}

// BatchProducer wraps kafka-go writer with batching, compression, and OTel tracing
type BatchProducer struct {
	writer    *kafka.Writer
	dlqWriter *kafka.Writer
	config    ProducerConfig
	logger    *zap.Logger
	tracer    trace.Tracer
}

// NewBatchProducer initializes the Kafka writer
func NewBatchProducer(cfg ProducerConfig, logger *zap.Logger) *BatchProducer {
	dialer := &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
	}

	writer := &kafka.Writer{
		Addr:         kafka.TCP(cfg.Brokers...),
		Topic:        cfg.Topic,
		Balancer:     &kafka.Hash{}, // consistent partitioning by key (user_id)
		BatchSize:    cfg.BatchSize,
		BatchTimeout: cfg.BatchTimeout,
		Compression:  kafka.Snappy,
		RequiredAcks: kafka.RequireAll, // acks=all for zero data loss
		MaxAttempts:  cfg.MaxAttempts,
		Async:        cfg.Async,
		Transport: &kafka.Transport{
			Dial: dialer.DialFunc,
		},
	}

	dlqWriter := &kafka.Writer{
		Addr:         kafka.TCP(cfg.Brokers...),
		Topic:        cfg.DLQTopic,
		Balancer:     &kafka.LeastBytes{},
		BatchSize:    100,
		BatchTimeout: 10 * time.Millisecond,
		Compression:  kafka.Snappy,
		RequiredAcks: kafka.RequireOne,
		MaxAttempts:  3,
	}

	return &BatchProducer{
		writer:    writer,
		dlqWriter: dlqWriter,
		config:    cfg,
		logger:    logger,
		tracer:    otel.GetTracerProvider().Tracer("streamforge-producer"),
	}
}

// EnsureTopics creates the configured topics with proper partitions and replication
func EnsureTopics(brokers []string, topics ...string) error {
	if len(brokers) == 0 {
		return fmt.Errorf("no brokers supplied")
	}
	conn, err := kafka.Dial("tcp", brokers[0])
	if err != nil {
		return err
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return err
	}
	var controllerConn *kafka.Conn
	controllerConn, err = kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		return err
	}
	defer controllerConn.Close()

	topicConfigs := make([]kafka.TopicConfig, len(topics))
	for i, t := range topics {
		topicConfigs[i] = kafka.TopicConfig{
			Topic:             t,
			NumPartitions:     6, // high partition count for parallel scale
			ReplicationFactor: 1, // single broker local dev; adjust in production k8s
		}
	}

	return controllerConn.CreateTopics(topicConfigs...)
}

// PublishEvent produces a single validated event with distributed tracing
func (p *BatchProducer) PublishEvent(ctx context.Context, event models.UserActivityEvent) error {
	ctx, span := p.tracer.Start(ctx, "kafka.produce",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.destination", p.config.Topic),
			attribute.String("messaging.message_id", event.EventID),
			attribute.String("messaging.user_id", event.UserID),
			attribute.String("messaging.event_type", event.EventType),
		),
	)
	defer span.End()

	payload, err := event.ToJSON()
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to serialize event: %w", err)
	}

	headers := []kafka.Header{
		{Key: "event_id", Value: []byte(event.EventID)},
		{Key: "event_type", Value: []byte(event.EventType)},
		{Key: "source", Value: []byte("streamforge-go-producer")},
	}
	telemetry.InjectTraceContext(ctx, &headers)

	msg := kafka.Message{
		Key:     []byte(event.UserID), // Ensures ordering per user
		Value:   payload,
		Headers: headers,
		Time:    time.UnixMilli(event.TimestampMs),
	}

	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		span.RecordError(err)
		p.logger.Error("Failed to write Kafka message",
			zap.String("event_id", event.EventID),
			zap.Error(err),
		)
		// Route to Dead Letter Queue
		_ = p.RouteToDLQ(ctx, event.EventID, string(payload), err.Error(), 1)
		return err
	}

	return nil
}

// PublishBatch produces a slice of events efficiently in one batch
func (p *BatchProducer) PublishBatch(ctx context.Context, events []models.UserActivityEvent) error {
	ctx, span := p.tracer.Start(ctx, "kafka.produce_batch",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.destination", p.config.Topic),
			attribute.Int("messaging.batch_count", len(events)),
		),
	)
	defer span.End()

	messages := make([]kafka.Message, 0, len(events))
	for _, event := range events {
		payload, err := event.ToJSON()
		if err != nil {
			p.logger.Warn("Skipping malformed event during batch serialize", zap.Error(err))
			continue
		}

		headers := []kafka.Header{
			{Key: "event_id", Value: []byte(event.EventID)},
			{Key: "event_type", Value: []byte(event.EventType)},
			{Key: "source", Value: []byte("streamforge-go-producer")},
		}
		telemetry.InjectTraceContext(ctx, &headers)

		messages = append(messages, kafka.Message{
			Key:     []byte(event.UserID),
			Value:   payload,
			Headers: headers,
			Time:    time.UnixMilli(event.TimestampMs),
		})
	}

	if len(messages) == 0 {
		return nil
	}

	start := time.Now()
	err := p.writer.WriteMessages(ctx, messages...)
	duration := time.Since(start)

	if err != nil {
		span.RecordError(err)
		p.logger.Error("Failed to publish event batch to Kafka",
			zap.Int("count", len(messages)),
			zap.Duration("duration", duration),
			zap.Error(err),
		)
		return err
	}

	p.logger.Debug("Batch published successfully",
		zap.Int("count", len(messages)),
		zap.Duration("duration", duration),
	)
	return nil
}

// RouteToDLQ emits a failed message to the Dead Letter Queue
func (p *BatchProducer) RouteToDLQ(ctx context.Context, eventID, rawPayload, reason string, retryCount int) error {
	hostname, _ := os.Hostname()
	span := trace.SpanFromContext(ctx)
	traceID := ""
	if span.SpanContext().IsValid() {
		traceID = span.SpanContext().TraceID().String()
	}

	envelope := models.DeadLetterEnvelope{
		EventID:       eventID,
		OriginalTopic: p.config.Topic,
		ErrorMessage:  reason,
		RetryCount:    retryCount,
		FailedAt:      time.Now().UTC().Format(time.RFC3339),
		RawPayload:    rawPayload,
		ProcessorHost: hostname,
		TraceID:       traceID,
	}

	data, err := json.Marshal(envelope)
	if err != nil {
		return err
	}

	msg := kafka.Message{
		Key:   []byte(eventID),
		Value: data,
		Headers: []kafka.Header{
			{Key: "dlq.original_topic", Value: []byte(p.config.Topic)},
			{Key: "dlq.error_reason", Value: []byte(reason)},
		},
	}

	p.logger.Warn("Routing event to Dead Letter Queue",
		zap.String("event_id", eventID),
		zap.String("reason", reason),
	)

	return p.dlqWriter.WriteMessages(ctx, msg)
}

// Close gracefully flushes pending buffers and closes connections
func (p *BatchProducer) Close() error {
	var err1, err2 error
	if p.writer != nil {
		err1 = p.writer.Close()
	}
	if p.dlqWriter != nil {
		err2 = p.dlqWriter.Close()
	}
	if err1 != nil {
		return err1
	}
	return err2
}

