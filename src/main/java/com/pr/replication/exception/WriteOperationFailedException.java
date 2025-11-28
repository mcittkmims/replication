package com.pr.replication.exception;

public class WriteOperationFailedException extends RuntimeException {
    public WriteOperationFailedException(String message) {
        super(message);
    }
}
