package com.trading.orchestrator.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Lob;
import jakarta.persistence.Table;

/**
 * Transactional Outbox entity ensuring atomic state transitions and messaging.
 * Events are written within the same database transaction as decision records,
 * guaranteeing zero data loss or duplicate execution across failures.
 */
@Entity
@Table(name = "outbox_events")
public class OutboxEventEntity {

    @Id
    @Column(name = "event_id", length = 64, nullable = false)
    private String eventId;

    @Column(name = "topic", length = 64, nullable = false)
    private String topic;

    @Column(name = "partition_key", length = 64)
    private String partitionKey;

    @Lob
    @Column(name = "payload", nullable = false)
    private String payload;

    @Column(name = "status", length = 32, nullable = false)
    private String status; // "IN_FLIGHT", "PENDING", "SENT", "FAILED"

    @Column(name = "created_at", nullable = false)
    private long createdAt;

    @Column(name = "updated_at", nullable = false)
    private long updatedAt;

    @Column(name = "retry_count", nullable = false)
    private int retryCount;

    @Column(name = "sent_at")
    private Long sentAt;

    public OutboxEventEntity() {}

    public OutboxEventEntity(String eventId, String topic, String partitionKey, String payload) {
        this(eventId, topic, partitionKey, payload, "IN_FLIGHT");
    }

    public OutboxEventEntity(String eventId, String topic, String partitionKey, String payload, String status) {
        this.eventId = eventId;
        this.topic = topic;
        this.partitionKey = partitionKey;
        this.payload = payload;
        this.status = status;
        long now = System.currentTimeMillis();
        this.createdAt = now;
        this.updatedAt = now;
        this.retryCount = 0;
    }

    public String getEventId() { return eventId; }
    public String getTopic() { return topic; }
    public String getPartitionKey() { return partitionKey; }
    public String getPayload() { return payload; }
    public String getStatus() { return status; }
    public void setStatus(String status) {
        this.status = status;
        this.updatedAt = System.currentTimeMillis();
    }
    public long getCreatedAt() { return createdAt; }
    public long getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(long updatedAt) { this.updatedAt = updatedAt; }
    public int getRetryCount() { return retryCount; }
    public void setRetryCount(int retryCount) { this.retryCount = retryCount; }
    public Long getSentAt() { return sentAt; }
    public void setSentAt(Long sentAt) { this.sentAt = sentAt; }
}
