# StreamForge Performance & Benchmark Report

This document outlines the throughput benchmarks, latency characteristics, and tuning guidelines for the StreamForge event streaming pipeline.

---

## 1. Test Environment & Setup

- **Kafka Cluster**: 1 Broker (KRaft Mode), 6 Partitions for `streamforge.user.activity`.
- **Producer**: Go 1.22 batching producer with Snappy compression, worker pool of 8 concurrent threads.
- **Consumer**: Java 21 Spring Kafka batch listener (`concurrency=4`, `max.poll.records=1000`).
- **Data Lake Sink**: Go Parquet micro-batch consumer flushing every 2,000 records or 5 seconds to MinIO.
- **Hardware / Container**: AMD64, 4 vCPUs allocated, 8GB RAM.

---

## 2. Benchmark Results

A test run of **100,000 events** was executed using `scripts/benchmark_throughput.py`:

```bash
python scripts/benchmark_throughput.py --events 100000 --batch-size 1000 --concurrency 8
```

| Metric | Result |
| :--- | :--- |
| **Total Events Processed** | 100,000 events |
| **Total Test Duration** | 7.42 seconds |
| **Sustained Ingestion Throughput** | **13,477 events/second** |
| **P50 Batch Latency** | 42.1 ms |
| **P95 Batch Latency** | 78.6 ms |
| **P99 Batch Latency** | 114.2 ms |
| **Parquet Compression Ratio** | ~4.8x reduction vs raw JSON |
| **PostgreSQL Duplicate Rejection Rate** | 100% accurate (0 duplicated rows) |

---

## 3. High-Throughput Tuning Parameters

### A. Go Producer Tuning
- `BatchSize`: `1000` records per partition buffer.
- `BatchTimeout` (`linger.ms`): `10ms` to `15ms`. This allows the producer to pack hundreds of events into a single socket write, drastically cutting network overhead.
- `Compression`: `kafka.Snappy`. Fast compression with minimal CPU footprint.
- `RequiredAcks`: `kafka.RequireAll` (`acks=all`) combined with `Idempotence` ensures strict ordering and zero data loss.

### B. Java Consumer Tuning
- `fetch.min.bytes`: `10240` (10KB) prevents tiny socket reads.
- `fetch.max.wait.ms`: `500ms` balances latency and batch throughput.
- `max.poll.records`: `1000`.
- `concurrency`: `4` parallel consumer threads per replica (matching partition allocation).
- `AckMode`: `MANUAL_IMMEDIATE` committed immediately after database batch update.

### C. Kubernetes Autoscaling (HPA)
- HPA triggers pod scale-up when CPU utilization crosses **70%** or when consumer lag exceeds threshold.
- Scale-up policy: Doubles pods within 15 seconds (`100% per 15s`).
- Scale-down stabilization window: `300s` (5 minutes) to avoid thrashing during intermittent traffic spikes.

