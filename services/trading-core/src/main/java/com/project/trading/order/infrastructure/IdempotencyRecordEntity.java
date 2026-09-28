package com.project.trading.order.infrastructure;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "idempotency_records")
public class IdempotencyRecordEntity {

    @Id
    @Column(name = "idempotency_key")
    private UUID key;

    @Column(name = "request_fingerprint", nullable = false, length = 64)
    private String fingerprint;

    @Column(name = "order_id")
    private UUID orderId;

    @Column(name = "response_status")
    private Integer responseStatus;

    @Column(name = "problem_body", columnDefinition = "text")
    private String problemBody;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "completed_at")
    private Instant completedAt;

    protected IdempotencyRecordEntity() {
    }

    public UUID getKey() {
        return key;
    }

    public String getFingerprint() {
        return fingerprint;
    }

    public UUID getOrderId() {
        return orderId;
    }

    public Integer getResponseStatus() {
        return responseStatus;
    }

    public String getProblemBody() {
        return problemBody;
    }

    public Instant getCompletedAt() {
        return completedAt;
    }

    public void linkOrder(UUID orderId) {
        this.orderId = orderId;
    }

    public void complete(int status, String problemBody, Instant at) {
        this.responseStatus = status;
        this.problemBody = problemBody;
        this.completedAt = at;
    }
}
