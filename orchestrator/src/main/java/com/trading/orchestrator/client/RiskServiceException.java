package com.trading.orchestrator.client;

import io.grpc.Status;

/**
 * Exception thrown when the Rust Risk Engine fails, times out (deadline exceeded),
 * or is unavailable.
 */
public class RiskServiceException extends RuntimeException {

    private final Status.Code statusCode;

    public RiskServiceException(String message, Status.Code statusCode, Throwable cause) {
        super(message, cause);
        this.statusCode = statusCode;
    }

    public Status.Code getStatusCode() {
        return statusCode;
    }
}
