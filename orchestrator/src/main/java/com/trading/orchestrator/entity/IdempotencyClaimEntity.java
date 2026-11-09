package com.trading.orchestrator.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

/**
 * Durable entity for tick idempotency claims preventing concurrent worker races
 * and duplicate processing across process restarts.
 */
@Entity
@Table(name = "idempotency_claims")
public class IdempotencyClaimEntity {

    @Id
    @Column(name = "fingerprint", length = 128, nullable = false)
    private String fingerprint;

    @Column(name = "status", length = 32, nullable = false)
    private String status; // "IN_FLIGHT", "COMPLETED", "FAILED"

    @Column(name = "created_at", nullable = false)
    private long createdAt;

    @Column(name = "updated_at", nullable = false)
    private long updatedAt;

    public IdempotencyClaimEntity() {}

    public IdempotencyClaimEntity(String fingerprint, String status) {
        this.fingerprint = fingerprint;
        this.status = status;
        long now = System.currentTimeMillis();
        this.createdAt = now;
        this.updatedAt = now;
    }

    public String getFingerprint() {
        return fingerprint;
    }

    public void setFingerprint(String fingerprint) {
        this.fingerprint = fingerprint;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
        this.updatedAt = System.currentTimeMillis();
    }

    public long getCreatedAt() {
        return createdAt;
    }

    public long getUpdatedAt() {
        return updatedAt;
    }
}

