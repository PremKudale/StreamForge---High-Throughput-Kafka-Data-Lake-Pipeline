package telemetry

import (
	"context"
	"os"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// InitTracer configures OpenTelemetry tracer with OTLP gRPC exporter
func InitTracer(ctx context.Context, serviceName string) (*sdktrace.TracerProvider, func(context.Context) error, error) {
	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if endpoint == "" {
		endpoint = "localhost:4317"
	}

	exporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithInsecure(),
		otlptracegrpc.WithEndpoint(endpoint),
		otlptracegrpc.WithDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
	)
	if err != nil {
		// Fallback to no-op if OTLP is unreachable in local offline testing
		tp := sdktrace.NewTracerProvider()
		otel.SetTracerProvider(tp)
		return tp, tp.Shutdown, nil
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String(serviceName),
			semconv.ServiceVersionKey.String("1.0.0"),
		),
	)
	if err != nil {
		return nil, nil, err
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return tp, tp.Shutdown, nil
}

// KafkaHeaderCarrier implements propagation.TextMapCarrier for segmentio/kafka-go headers
type KafkaHeaderCarrier struct {
	Headers *[]kafka.Header
}

func (c KafkaHeaderCarrier) Get(key string) string {
	if c.Headers == nil {
		return ""
	}
	for _, h := range *c.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c KafkaHeaderCarrier) Set(key string, val string) {
	if c.Headers == nil {
		return
	}
	for i, h := range *c.Headers {
		if h.Key == key {
			(*c.Headers)[i].Value = []byte(val)
			return
		}
	}
	*c.Headers = append(*c.Headers, kafka.Header{
		Key:   key,
		Value: []byte(val),
	})
}

func (c KafkaHeaderCarrier) Keys() []string {
	if c.Headers == nil {
		return nil
	}
	keys := make([]string, len(*c.Headers))
	for i, h := range *c.Headers {
		keys[i] = h.Key
	}
	return keys
}

// InjectTraceContext injects OpenTelemetry trace context into Kafka message headers
func InjectTraceContext(ctx context.Context, headers *[]kafka.Header) {
	otel.GetTextMapPropagator().Inject(ctx, KafkaHeaderCarrier{Headers: headers})
}

// ExtractTraceContext extracts OpenTelemetry trace context from Kafka message headers
func ExtractTraceContext(ctx context.Context, headers []kafka.Header) (context.Context, trace.SpanContext) {
	carrier := KafkaHeaderCarrier{Headers: &headers}
	extractedCtx := otel.GetTextMapPropagator().Extract(ctx, carrier)
	spanCtx := trace.SpanContextFromContext(extractedCtx)
	return extractedCtx, spanCtx
}

