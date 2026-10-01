package com.example.gsb.atomic;

/** Thrown when a batch contains more entries than the configured limit. */
public class BatchTooLargeException extends RuntimeException {

    private final int actualSize;
    private final int maxSize;

    public BatchTooLargeException(int actualSize, int maxSize) {
        super("Batch size " + actualSize + " exceeds the configured maximum of "
                + maxSize + " entries per batch; split the batch into smaller units");
        this.actualSize = actualSize;
        this.maxSize = maxSize;
    }

    public int getActualSize() {
        return actualSize;
    }

    public int getMaxSize() {
        return maxSize;
    }
}
