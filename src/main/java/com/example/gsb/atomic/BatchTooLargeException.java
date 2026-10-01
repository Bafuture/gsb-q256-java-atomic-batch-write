package com.example.gsb.atomic;

/** Thrown when a batch exceeds the configured maximum batch size. */
public class BatchTooLargeException extends RuntimeException {

    public BatchTooLargeException(String batchId, int actualSize, int maxSize) {
        super("Batch '" + batchId + "' rejected: " + actualSize
                + " entries exceed the maximum allowed batch size of " + maxSize);
    }
}
