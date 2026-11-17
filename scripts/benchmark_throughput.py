#!/usr/bin/env python3
"""
StreamForge High-Throughput Ingestion Benchmark Tool
Evaluates latency (P50, P95, P99), throughput (events/sec), and batching efficiency.
"""

import argparse
import concurrent.futures
import json
import random
import time
import urllib.request
import uuid

EVENT_TYPES = ["PAGE_VIEW", "PURCHASE", "SEARCH", "ADD_TO_CART", "LOGIN"]
DEVICES = ["MOBILE", "DESKTOP", "TABLET"]

def generate_event(user_id_pool_size=1000) -> dict:
    event_type = random.choice(EVENT_TYPES)
    payload = {}
    if event_type == "PURCHASE":
        payload = {"order_id": f"ord-{random.randint(1000, 999999)}", "amount": round(random.uniform(5.0, 500.0), 2)}
    elif event_type == "SEARCH":
        payload = {"query": "distributed high throughput stream", "hits": random.randint(1, 100)}

    now_ms = int(time.time() * 1000)
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": f"usr-{random.randint(1, user_id_pool_size):05d}",
        "event_type": event_type,
        "timestamp_ms": now_ms,
        "ip_address": f"10.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}",
        "payload": json.dumps(payload),
        "device_type": random.choice(DEVICES)
    }

def send_batch(endpoint: str, batch: list) -> float:
    data = json.dumps(batch).encode("utf-8")
    req = urllib.request.Request(
        endpoint,
        data=data,
        headers={"Content-Type": "application/json"}
    )
    start = time.perf_counter()
    with urllib.request.urlopen(req, timeout=10) as resp:
        _ = resp.read()
    duration = time.perf_counter() - start
    return duration

def run_benchmark(endpoint: str, total_events: int, batch_size: int, concurrency: int):
    print("=" * 65)
    print("  StreamForge High-Throughput Kafka Ingestion Benchmark")
    print("=" * 65)
    print(f"Target Endpoint:  {endpoint}")
    print(f"Total Events:     {total_events:,}")
    print(f"Batch Size:       {batch_size:,}")
    print(f"Total Batches:    {total_events // batch_size:,}")
    print(f"Concurrency:      {concurrency} workers")
    print("-" * 65)

    num_batches = total_events // batch_size
    batches = [[generate_event() for _ in range(batch_size)] for _ in range(num_batches)]

    latencies = []
    start_total = time.perf_counter()

    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [executor.submit(send_batch, endpoint, b) for b in batches]
        for f in concurrent.futures.as_completed(futures):
            try:
                lat = f.result()
                latencies.append(lat)
            except Exception as e:
                print(f"[ERROR] Batch send failed: {e}")

    total_time = time.perf_counter() - start_total
    events_sent = len(latencies) * batch_size
    throughput = events_sent / total_time if total_time > 0 else 0

    latencies.sort()
    p50 = latencies[int(len(latencies) * 0.50)] * 1000 if latencies else 0
    p95 = latencies[int(len(latencies) * 0.95)] * 1000 if latencies else 0
    p99 = latencies[int(len(latencies) * 0.99)] * 1000 if latencies else 0

    print("\n" + "=" * 65)
    print("  BENCHMARK RESULTS SUMMARY")
    print("=" * 65)
    print(f"Total Events Ingested:    {events_sent:,} events")
    print(f"Total Duration:           {total_time:.2f} seconds")
    print(f"Sustained Throughput:     {throughput:,.2f} events/sec")
    print(f"Batch P50 Latency:        {p50:.2f} ms")
    print(f"Batch P95 Latency:        {p95:.2f} ms")
    print(f"Batch P99 Latency:        {p99:.2f} ms")
    print("=" * 65)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="StreamForge Pipeline Benchmark")
    parser.add_argument("--endpoint", default="http://localhost:8080/api/v1/events/batch", help="Batch endpoint URL")
    parser.add_argument("--events", type=int, default=50000, help="Total events to send")
    parser.add_argument("--batch-size", type=int, default=1000, help="Events per HTTP batch")
    parser.add_argument("--concurrency", type=int, default=8, help="Concurrent HTTP worker threads")
    args = parser.parse_args()

    run_benchmark(args.endpoint, args.events, args.batch_size, args.concurrency)

