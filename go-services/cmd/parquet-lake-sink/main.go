package main

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/segmentio/kafka-go"
	"github.com/streamforge/pipeline/pkg/models"
	sfparquet "github.com/streamforge/pipeline/pkg/parquet"
	"github.com/streamforge/pipeline/pkg/s3lake"
	"github.com/streamforge/pipeline/pkg/telemetry"
	"go.uber.org/zap"
)

var (
	lakeEventsTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "streamforge_lake_sink_events_total",
			Help: "Total count of events converted to Parquet and written to MinIO S3 lake",
		},
	)
	parquetBytesWritten = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "streamforge_lake_sink_parquet_bytes_total",
			Help: "Total bytes of compressed Parquet files uploaded to S3 lake",
		},
	)
	s3UploadDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "streamforge_lake_sink_upload_duration_seconds",
			Help:    "Duration of S3 uploads in seconds",
			Buckets: prometheus.DefBuckets,
		},
	)
)

func init() {
	prometheus.MustRegister(lakeEventsTotal)
	prometheus.MustRegister(parquetBytesWritten)
	prometheus.MustRegister(s3UploadDuration)
}

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, shutdownTracer, err := telemetry.InitTracer(ctx, "streamforge-parquet-sink")
	if err != nil {
		logger.Warn("Failed to init telemetry tracer", zap.Error(err))
	} else {
		defer shutdownTracer(context.Background())
	}

	// Initialize S3/MinIO Lake Client
	minioClient, err := s3lake.NewMinioClient(ctx, logger)
	if err != nil {
		logger.Fatal("Failed to initialize MinIO client", zap.Error(err))
	}

	// Kafka Consumer Configuration
	brokers := []string{"localhost:9092"}
	if envBrokers := os.Getenv("KAFKA_BROKERS"); envBrokers != "" {
		brokers = []string{envBrokers}
	}
	topic := os.Getenv("KAFKA_EVENT_TOPIC")
	if topic == "" {
		topic = "streamforge.user.activity"
	}
	groupID := os.Getenv("KAFKA_CONSUMER_GROUP_LAKE")
	if groupID == "" {
		groupID = "streamforge-parquet-lake-sink"
	}

	batchFlushSize := 2000
	if bs := os.Getenv("LAKE_BATCH_FLUSH_SIZE"); bs != "" {
		if parsed, err := strconv.Atoi(bs); err == nil {
			batchFlushSize = parsed
		}
	}
	flushInterval := 5 * time.Second
	if fi := os.Getenv("LAKE_FLUSH_INTERVAL_SEC"); fi != "" {
		if parsed, err := strconv.Atoi(fi); err == nil {
			flushInterval = time.Duration(parsed) * time.Second
		}
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		GroupID:        groupID,
		Topic:          topic,
		MinBytes:       10e3, // 10KB
		MaxBytes:       10e6, // 10MB
		CommitInterval: 0,    // synchronous manual commits after successful S3 flush
		MaxWait:        1 * time.Second,
	})
	defer reader.Close()

	// Metrics & Health endpoint
	httpPort := os.Getenv("HTTP_PORT")
	if httpPort == "" {
		httpPort = "8082"
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("READY"))
	})

	server := &http.Server{Addr: ":" + httpPort, Handler: mux}
	go func() {
		logger.Info("Starting Parquet Sink Health & Metrics Server", zap.String("port", httpPort))
		_ = server.ListenAndServe()
	}()

	logger.Info("Starting StreamForge Parquet Lake Sink Consumer",
		zap.Strings("brokers", brokers),
		zap.String("topic", topic),
		zap.String("group_id", groupID),
		zap.Int("flush_size", batchFlushSize),
		zap.Duration("flush_interval", flushInterval),
	)

	// Processing Loop
	go func() {
		buffer := make([]models.UserActivityEvent, 0, batchFlushSize)
		lastMessages := make([]kafka.Message, 0, batchFlushSize)
		ticker := time.NewTicker(flushInterval)
		defer ticker.Stop()

		flush := func() {
			if len(buffer) == 0 {
				return
			}

			startFlush := time.Now()
			parquetData, err := sfparquet.EventsToParquet(buffer)
			if err != nil {
				logger.Error("Failed to convert events batch to Parquet", zap.Error(err))
				return
			}

			partitionKey := s3lake.GeneratePartitionPath("parquet", time.Now())
			timer := prometheus.NewTimer(s3UploadDuration)
			s3URI, err := minioClient.UploadBytes(ctx, partitionKey, parquetData, "application/vnd.apache.parquet")
			timer.ObserveDuration()

			if err != nil {
				logger.Error("Failed to upload Parquet to S3 Lake", zap.Error(err))
				return
			}

			// Commit Kafka messages up to the latest in batch
			if len(lastMessages) > 0 {
				commitMsg := lastMessages[len(lastMessages)-1]
				if err := reader.CommitMessages(ctx, commitMsg); err != nil {
					logger.Error("Failed to commit Kafka offset after S3 upload", zap.Error(err))
				}
			}

			lakeEventsTotal.Add(float64(len(buffer)))
			parquetBytesWritten.Add(float64(len(parquetData)))

			logger.Info("Flushed micro-batch to S3 Data Lake",
				zap.Int("events_count", len(buffer)),
				zap.Int("parquet_bytes", len(parquetData)),
				zap.String("s3_uri", s3URI),
				zap.Duration("duration", time.Since(startFlush)),
			)

			buffer = buffer[:0]
			lastMessages = lastMessages[:0]
		}

		for {
			select {
			case <-ctx.Done():
				flush()
				return
			case <-ticker.C:
				if len(buffer) > 0 {
					flush()
				}
			default:
				fetchCtx, fetchCancel := context.WithTimeout(ctx, 1*time.Second)
				msg, err := reader.FetchMessage(fetchCtx)
				fetchCancel()

				if err != nil {
					if err == context.Canceled || err == context.DeadlineExceeded {
						continue
					}
					logger.Warn("Error fetching message from Kafka", zap.Error(err))
					time.Sleep(500 * time.Millisecond)
					continue
				}

				var event models.UserActivityEvent
				if err := json.Unmarshal(msg.Value, &event); err != nil {
					logger.Warn("Malformed event in topic, skipping", zap.Error(err))
					_ = reader.CommitMessages(ctx, msg)
					continue
				}

				buffer = append(buffer, event)
				lastMessages = append(lastMessages, msg)

				if len(buffer) >= batchFlushSize {
					flush()
				}
			}
		}
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	logger.Info("Shutting down Parquet Lake Sink...")
	cancel()
	_ = server.Shutdown(context.Background())
	logger.Info("Parquet Lake Sink stopped")
}

