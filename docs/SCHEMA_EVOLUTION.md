# Schema Evolution & Avro Compatibility

In production distributed streaming architectures, producer applications and consumer applications deploy independently. Schema evolution guarantees that changes to data contracts do not cause pipeline downtime or deserialization failures.

---

## 1. Schema Versions

### Schema v1 (`schemas/avro/user_activity_v1.avsc`)
Baseline data model defining core attributes:
- `event_id`: Unique identifier (UUID string)
- `user_id`: Actor identifier
- `event_type`: Categorical activity enum (`PAGE_VIEW`, `PURCHASE`, `SEARCH`, `ADD_TO_CART`, `LOGIN`, `LOGOUT`, `HEARTBEAT`)
- `timestamp_ms`: Epoch milliseconds
- `ip_address`: Client IP
- `payload`: Flexible JSON attributes string

### Schema v2 (`schemas/avro/user_activity_v2.avsc`)
Evolved model incorporating device information and session tracking:
- Added `device_type`: string with default `"UNKNOWN"`
- Added `session_id`: union `["null", "string"]` with default `null`
- Added `tags`: map of strings with default `{}`

---

## 2. Compatibility Guarantees: BACKWARD Compatibility

StreamForge uses Confluent Schema Registry's **BACKWARD** compatibility rule:
> **Consumers using the new schema can read data produced with the previous schema.**

### Key Rules Followed:
1. **Default Values for New Fields**:
   Every field introduced in `v2` specifies an explicit default value:
   ```json
   {
     "name": "device_type",
     "type": "string",
     "default": "UNKNOWN"
   }
   ```
   When a `v2` consumer reads an older `v1` message (which lacks `device_type`), the deserializer fills in `"UNKNOWN"` instead of throwing a missing-field exception.

2. **No Field Removals Without Deprecation**:
   Core mandatory fields (`event_id`, `user_id`, `event_type`) remain unchanged across versions.

3. **Schema Registry Automated Checks**:
   Before rolling out producer updates, CI/CD invokes:
   ```bash
   python scripts/register_schemas.py
   ```
   This validates the evolution contract against the live Schema Registry REST API `/compatibility/subjects/{subject}/versions/latest`.

---

## 3. Database Schema Co-Evolution
The PostgreSQL table and Go/Java models mirror this contract:
- PostgreSQL column `device_type` is defined with `DEFAULT 'UNKNOWN'`.
- Java Jackson model uses `@JsonIgnoreProperties(ignoreUnknown = true)` and field initializers:
  ```java
  @JsonProperty("device_type")
  private String deviceType = "UNKNOWN";
  ```
- Go model uses struct tags and fallback logic in `event.Validate()`.
