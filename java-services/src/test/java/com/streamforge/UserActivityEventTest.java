package com.streamforge;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamforge.model.UserActivityEvent;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class UserActivityEventTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    public void testEventSerializationAndSchemaEvolution() throws Exception {
        // Test v1 JSON payload without new fields
        String v1Json = """
            {
                "event_id": "evt-123",
                "user_id": "usr-456",
                "event_type": "PURCHASE",
                "timestamp_ms": 1731900000000,
                "ip_address": "10.0.0.1",
                "payload": "{\\"amount\\": 49.99}"
            }
        """;

        UserActivityEvent event = objectMapper.readValue(v1Json, UserActivityEvent.class);

        assertEquals("evt-123", event.getEventId());
        assertEquals("usr-456", event.getUserId());
        assertEquals("PURCHASE", event.getEventType());
        // Verify backward compatibility: default deviceType is UNKNOWN and sessionId is null
        assertEquals("UNKNOWN", event.getDeviceType());
        assertNull(event.getSessionId());

        // Test v2 JSON payload with evolved fields
        String v2Json = """
            {
                "event_id": "evt-789",
                "user_id": "usr-999",
                "event_type": "PAGE_VIEW",
                "timestamp_ms": 1731900005000,
                "ip_address": "10.0.0.2",
                "payload": "{}",
                "device_type": "MOBILE",
                "session_id": "sess-abc"
            }
        """;

        UserActivityEvent eventV2 = objectMapper.readValue(v2Json, UserActivityEvent.class);
        assertEquals("MOBILE", eventV2.getDeviceType());
        assertEquals("sess-abc", eventV2.getSessionId());
    }
}

