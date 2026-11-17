package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/streamforge/pipeline/pkg/models"
	"go.uber.org/zap"
)

func main() {
	mode := flag.String("mode", "inspect", "Mode: inspect or replay")
	maxMessages := flag.Int("limit", 100, "Maximum messages to process")
	flag.Parse()

	logger, _ := zap.NewDevelopment()
	defer logger.Sync()

	brokers := []string{"localhost:9092"}
	if env := os.Getenv("KAFKA_BROKERS"); env != "" {
		brokers = []string{env}
	}
	dlqTopic := os.Getenv("KAFKA_DLQ_TOPIC")
	if dlqTopic == "" {
		dlqTopic = "streamforge.events.dlq"
	}
	mainTopic := os.Getenv("KAFKA_EVENT_TOPIC")
	if mainTopic == "" {
		mainTopic = "streamforge.user.activity"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		Topic:       dlqTopic,
		GroupID:     fmt.Sprintf("dlq-tool-%d", time.Now().Unix()),
		StartOffset: kafka.FirstOffset,
		MaxWait:     2 * time.Second,
	})
	defer reader.Close()

	var writer *kafka.Writer
	if *mode == "replay" {
		writer = &kafka.Writer{
			Addr:     kafka.TCP(brokers...),
			Topic:    mainTopic,
			Balancer: &kafka.Hash{},
		}
		defer writer.Close()
	}

	fmt.Printf("=== StreamForge DLQ Management Tool ===\n")
	fmt.Printf("Brokers: %v | DLQ Topic: %s | Mode: %s\n\n", brokers, dlqTopic, *mode)

	count := 0
	errorStats := make(map[string]int)

	for count < *maxMessages {
		fetchCtx, fetchCancel := context.WithTimeout(ctx, 3*time.Second)
		msg, err := reader.FetchMessage(fetchCtx)
		fetchCancel()

		if err != nil {
			break // No more messages currently in DLQ
		}

		var envelope models.DeadLetterEnvelope
		if err := json.Unmarshal(msg.Value, &envelope); err != nil {
			fmt.Printf("[%d] Unparseable DLQ Message: %s\n", count+1, string(msg.Value))
			continue
		}

		count++
		errorStats[envelope.ErrorMessage]++

		fmt.Printf("[%d] Failed Event ID: %s | Error: %s | FailedAt: %s\n",
			count, envelope.EventID, envelope.ErrorMessage, envelope.FailedAt)

		if *mode == "replay" {
			// Replay raw payload back to main topic
			replayMsg := kafka.Message{
				Key:   []byte(envelope.EventID),
				Value: []byte(envelope.RawPayload),
				Headers: []kafka.Header{
					{Key: "reprocessed", Value: []byte("true")},
					{Key: "original_dlq_offset", Value: []byte(fmt.Sprintf("%d", msg.Offset))},
				},
			}
			if err := writer.WriteMessages(ctx, replayMsg); err != nil {
				fmt.Printf("  -> ERROR replaying to %s: %v\n", mainTopic, err)
			} else {
				fmt.Printf("  -> Replayed successfully to topic: %s\n", mainTopic)
				_ = reader.CommitMessages(ctx, msg)
			}
		}
	}

	fmt.Printf("\n--- DLQ Summary ---\n")
	fmt.Printf("Total messages processed: %d\n", count)
	fmt.Println("Error Breakdown:")
	for reason, n := range errorStats {
		fmt.Printf("  - (%d) %s\n", n, reason)
	}
}

