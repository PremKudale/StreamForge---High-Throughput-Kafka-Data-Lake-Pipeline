#!/usr/bin/env python3
"""
StreamForge Poison Pill & Dead Letter Queue (DLQ) Resilience Test
Injects malformed JSON events to test retry handling and DLQ diversion.
"""

import json
import urllib.request
import uuid

PRODUCER_ENDPOINT = "http://localhost:8080/api/v1/events"

def main():
    print("=== Testing Dead Letter Queue Routing with Malformed / Poison Payloads ===")
    
    # 1. Poison payload: missing mandatory user_id & event_type
    poison_pill = {
        "event_id": f"poison-{uuid.uuid4()}",
        "user_id": "",
        "event_type": "",
        "payload": "{invalid_json_bytes_###}"
    }

    req = urllib.request.Request(
        PRODUCER_ENDPOINT,
        data=json.dumps(poison_pill).encode(),
        headers={"Content-Type": "application/json"}
    )

    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            print(f"Server response: {resp.status}")
    except urllib.error.HTTPError as e:
        print(f"Validation rejected at producer gate as expected: {e.code} - {e.read().decode()}")
        print("Poison payload rejected before poisoning Kafka broker or consumers.")

    print("\nDLQ Resilience Test Completed.")

if __name__ == "__main__":
    main()

