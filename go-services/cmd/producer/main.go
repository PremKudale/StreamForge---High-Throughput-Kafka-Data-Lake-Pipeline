package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/streamforge/pipeline/pkg/kafka"
	"github.com/streamforge/pipeline/pkg/models"
	"github.com/streamforge/pipeline/pkg/telemetry"
	"go.uber.org/zap"
)

var (
	eventsProducedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "streamforge_producer_events_total",
			Help: "Total count of events produced by StreamForge producer",
		},
		[]string{"event_type", "status"},
	)
	batchLatency = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "streamforge_producer_batch_latency_seconds",
			Help:    "Latency of Kafka produce batch requests in seconds",
			Buckets: prometheus.DefBuckets,
		},
	)
)

func init() {
	prometheus.MustRegister(eventsProducedTotal)
	prometheus.MustRegister(batchLatency)
}

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize OpenTelemetry Tracer
	_, shutdownTracer, err := telemetry.InitTracer(ctx, "streamforge-go-producer")
	if err != nil {
		logger.Warn("Failed to initialize OpenTelemetry tracer", zap.Error(err))
	} else {
		defer shutdownTracer(context.Background())
	}

	cfg := kafka.DefaultProducerConfig()
	producer := kafka.NewBatchProducer(cfg, logger)
	defer producer.Close()

	// Ensure Kafka topics exist
	go func() {
		for i := 0; i < 10; i++ {
			if err := kafka.EnsureTopics(cfg.Brokers, cfg.Topic, cfg.DLQTopic); err == nil {
				logger.Info("Kafka topics verified and ready", zap.Strings("topics", []string{cfg.Topic, cfg.DLQTopic}))
				break
			}
			time.Sleep(3 * time.Second)
		}
	}()

	// Start HTTP Server for REST ingestion & Prometheus metrics
	httpPort := os.Getenv("HTTP_PORT")
	if httpPort == "" {
		httpPort = "8080"
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

	// Ingestion endpoint for individual events
	mux.HandleFunc("/api/v1/events", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var event models.UserActivityEvent
		if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
			http.Error(w, "Bad request: invalid JSON: "+err.Error(), http.StatusBadRequest)
			return
		}

		if event.EventID == "" {
			event.EventID = uuid.New().String()
		}
		if err := event.Validate(); err != nil {
			http.Error(w, "Validation failed: "+err.Error(), http.StatusBadRequest)
			return
		}

		if err := producer.PublishEvent(r.Context(), event); err != nil {
			eventsProducedTotal.WithLabelValues(event.EventType, "error").Inc()
			http.Error(w, "Failed to publish event: "+err.Error(), http.StatusInternalServerError)
			return
		}

		eventsProducedTotal.WithLabelValues(event.EventType, "success").Inc()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"status":   "accepted",
			"event_id": event.EventID,
		})
	})

	// High-throughput batch ingestion endpoint
	mux.HandleFunc("/api/v1/events/batch", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var batch []models.UserActivityEvent
		if err := json.NewDecoder(r.Body).Decode(&batch); err != nil {
			http.Error(w, "Invalid batch JSON: "+err.Error(), http.StatusBadRequest)
			return
		}

		for i := range batch {
			if batch[i].EventID == "" {
				batch[i].EventID = uuid.New().String()
			}
			_ = batch[i].Validate()
		}

		timer := prometheus.NewTimer(batchLatency)
		err := producer.PublishBatch(r.Context(), batch)
		timer.ObserveDuration()

		if err != nil {
			eventsProducedTotal.WithLabelValues("batch", "error").Add(float64(len(batch)))
			http.Error(w, "Failed to publish batch: "+err.Error(), http.StatusInternalServerError)
			return
		}

		eventsProducedTotal.WithLabelValues("batch", "success").Add(float64(len(batch)))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"status":   "accepted",
			"count":    len(batch),
		})
	})

	server := &http.Server{
		Addr:    ":" + httpPort,
		Handler: mux,
	}

	go func() {
		logger.Info("Starting HTTP Ingestion API & Metrics Server", zap.String("port", httpPort))
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal("HTTP server failed", zap.Error(err))
		}
	}()

	// Optional high-throughput mock load generator
	loadGenEnabled := os.Getenv("ENABLE_LOAD_GENERATOR") == "true"
	if loadGenEnabled {
		ratePerSec := 5000
		if rps := os.Getenv("LOAD_GEN_RATE_PER_SEC"); rps != "" {
			if parsed, err := strconv.Atoi(rps); err == nil {
				ratePerSec = parsed
			}
		}
		logger.Info("Starting synthetic load generator worker pool",
			zap.Int("target_rate_per_sec", ratePerSec),
		)
		startLoadGenerator(ctx, producer, ratePerSec, logger)
	}

	// Graceful shutdown handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	logger.Info("Shutting down StreamForge producer gracefully...")
	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = server.Shutdown(shutdownCtx)
	logger.Info("StreamForge producer stopped")
}

// startLoadGenerator generates high-throughput events across multiple concurrent workers
func startLoadGenerator(ctx context.Context, producer *kafka.BatchProducer, targetRate int, logger *zap.Logger) {
	numWorkers := 4
	batchSize := 250
	interval := time.Second * time.Duration(batchSize) / time.Duration(targetRate)
	if interval < 5*time.Millisecond {
		interval = 5 * time.Millisecond
	}

	eventTypes := []string{"PAGE_VIEW", "PURCHASE", "SEARCH", "ADD_TO_CART", "LOGIN", "LOGOUT"}
	devices := []string{"MOBILE", "DESKTOP", "TABLET"}

	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			ticker := time.NewTicker(interval)
			defer ticker.Stop()

			batch := make([]models.UserActivityEvent, 0, batchSize)

			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					batch = batch[:0]
					now := time.Now()
					for i := 0; i < batchSize; i++ {
						eType := eventTypes[rand.Intn(len(eventTypes))]
						uID := fmt.Sprintf("user-%04d", rand.Intn(1000))
						payload := "{}"
						if eType == "PURCHASE" {
							payload = fmt.Sprintf(`{"order_id":"ord-%d","amount":%.2f,"currency":"USD"}`, rand.Intn(999999), float64(rand.Intn(10000))/100.0)
						} else if eType == "SEARCH" {
							payload = `{"query":"kafka stream processing","results":42}`
						}

						batch = append(batch, models.UserActivityEvent{
							EventID:     uuid.New().String(),
							UserID:      uID,
							EventType:   eType,
							TimestampMs: now.UnixMilli(),
							EventTime:   now.UTC().Format(time.RFC3339),
							IPAddress:   fmt.Sprintf("192.168.%d.%d", rand.Intn(255), rand.Intn(255)),
							Payload:     payload,
							DeviceType:  devices[rand.Intn(len(devices))],
						})
					}

					timer := prometheus.NewTimer(batchLatency)
					err := producer.PublishBatch(ctx, batch)
					timer.ObserveDuration()

					if err != nil {
						eventsProducedTotal.WithLabelValues("generator", "error").Add(float64(len(batch)))
					} else {
						eventsProducedTotal.WithLabelValues("generator", "success").Add(float64(len(batch)))
					}
				}
			}
		}(w)
	}
}

