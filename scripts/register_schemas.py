#!/usr/bin/env python3
"""
StreamForge Schema Registry Management & Evolution Script
Registers Avro Schemas and verifies backward-compatibility guarantees.
"""

import json
import os
import sys
import urllib.request
import urllib.error

SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8089")
SUBJECT_NAME = "streamforge.user.activity-value"

def register_schema(subject: str, schema_path: str) -> dict:
    with open(schema_path, "r") as f:
        schema_json = json.load(f)

    payload = json.dumps({"schema": json.dumps(schema_json)}).encode("utf-8")
    req = urllib.request.Request(
        f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions",
        data=payload,
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"}
    )
    
    try:
        with urllib.request.urlopen(req) as resp:
            data = json.loads(resp.read().decode())
            print(f"[SUCCESS] Registered {schema_path} under '{subject}'. Schema ID: {data.get('id')}")
            return data
    except urllib.error.HTTPError as e:
        error_body = e.read().decode()
        print(f"[ERROR] Failed to register schema: {e.code} - {error_body}")
        raise

def test_compatibility(subject: str, schema_path: str) -> bool:
    with open(schema_path, "r") as f:
        schema_json = json.load(f)

    payload = json.dumps({"schema": json.dumps(schema_json)}).encode("utf-8")
    req = urllib.request.Request(
        f"{SCHEMA_REGISTRY_URL}/compatibility/subjects/{subject}/versions/latest",
        data=payload,
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"}
    )
    
    try:
        with urllib.request.urlopen(req) as resp:
            data = json.loads(resp.read().decode())
            is_compat = data.get("is_compatible", False)
            print(f"[COMPATIBILITY CHECK] Schema '{schema_path}' compatible with latest: {is_compat}")
            return is_compat
    except urllib.error.HTTPError as e:
        print(f"[WARN] Compatibility test response: {e.code} - {e.read().decode()}")
        return False

def main():
    print(f"Connecting to Schema Registry at: {SCHEMA_REGISTRY_URL}")
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    v1_path = os.path.join(base_dir, "schemas", "avro", "user_activity_v1.avsc")
    v2_path = os.path.join(base_dir, "schemas", "avro", "user_activity_v2.avsc")

    # 1. Register Schema v1
    print("\n--- Phase 1: Register Initial Schema v1 ---")
    try:
        register_schema(SUBJECT_NAME, v1_path)
    except Exception as e:
        print(f"Schema Registry unavailable or error: {e}. (Make sure Docker Compose is up)")
        sys.exit(1)

    # 2. Check Compatibility for Schema v2 (Evolution)
    print("\n--- Phase 2: Verify Schema Evolution Compatibility for v2 ---")
    is_compat = test_compatibility(SUBJECT_NAME, v2_path)
    if is_compat:
        print("Schema v2 PASSED compatibility checks (backward compatible with defaults).")
        register_schema(SUBJECT_NAME, v2_path)
        print("Schema v2 successfully registered!")
    else:
        print("Schema v2 is not compatible according to current registry rules.")

if __name__ == "__main__":
    main()
