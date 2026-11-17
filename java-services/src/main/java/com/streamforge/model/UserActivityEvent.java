package com.streamforge.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;

@JsonIgnoreProperties(ignoreUnknown = true)
public class UserActivityEvent {

    @JsonProperty("event_id")
    private String eventId;

    @JsonProperty("user_id")
    private String userId;

    @JsonProperty("event_type")
    private String eventType;

    @JsonProperty("timestamp_ms")
    private Long timestampMs;

    @JsonProperty("ip_address")
    private String ipAddress;

    @JsonProperty("payload")
    private String payload;

    // Schema evolution fields (v2 compatibility with defaults)
    @JsonProperty("device_type")
    private String deviceType = "UNKNOWN";

    @JsonProperty("session_id")
    private String sessionId;

    public UserActivityEvent() {
    }

    public UserActivityEvent(String eventId, String userId, String eventType, Long timestampMs, String ipAddress, String payload) {
        this.eventId = eventId;
        this.userId = userId;
        this.eventType = eventType;
        this.timestampMs = timestampMs;
        this.ipAddress = ipAddress;
        this.payload = payload;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public Long getTimestampMs() {
        if (timestampMs == null) {
            return Instant.now().toEpochMilli();
        }
        return timestampMs;
    }

    public void setTimestampMs(Long timestampMs) {
        this.timestampMs = timestampMs;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }

    public String getPayload() {
        return payload != null ? payload : "{}";
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    public String getDeviceType() {
        return deviceType != null ? deviceType : "UNKNOWN";
    }

    public void setDeviceType(String deviceType) {
        this.deviceType = deviceType;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    @Override
    public String toString() {
        return "UserActivityEvent{" +
                "eventId='" + eventId + '\'' +
                ", userId='" + userId + '\'' +
                ", eventType='" + eventType + '\'' +
                ", timestampMs=" + timestampMs +
                ", deviceType='" + deviceType + '\'' +
                '}';
    }
}

