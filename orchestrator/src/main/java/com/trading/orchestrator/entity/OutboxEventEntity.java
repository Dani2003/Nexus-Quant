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
    private String status; // "PENDING", "SENT", "FAILED"

    @Column(name = "created_at", nullable = false)
    private long createdAt;

    @Column(name = "sent_at")
    private Long sentAt;

    public OutboxEventEntity() {}

    public OutboxEventEntity(String eventId, String topic, String partitionKey, String payload) {
        this.eventId = eventId;
        this.topic = topic;
        this.partitionKey = partitionKey;
        this.payload = payload;
        this.status = "PENDING";
        this.createdAt = System.currentTimeMillis();
    }

    public String getEventId() { return eventId; }
    public String getTopic() { return topic; }
    public String getPartitionKey() { return partitionKey; }
    public String getPayload() { return payload; }
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
    public long getCreatedAt() { return createdAt; }
    public Long getSentAt() { return sentAt; }
    public void setSentAt(Long sentAt) { this.sentAt = sentAt; }
}
