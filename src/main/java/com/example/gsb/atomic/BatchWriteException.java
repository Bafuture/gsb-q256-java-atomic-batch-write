package com.example.gsb.atomic;

/**
 * Thrown when a commit fails part-way through. All modifications already
 * applied by the batch have been rolled back before this exception is raised.
 */
public class BatchWriteException extends RuntimeException {

    public BatchWriteException(String message, Throwable cause) {
        super(message, cause);
    }
}
