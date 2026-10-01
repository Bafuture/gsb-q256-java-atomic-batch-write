package com.example.gsb.atomic;

/**
 * Thrown when a batch could not be applied and was rolled back: no entry of
 * the batch becomes visible, and an unfinished record left in the log is
 * discarded on recovery.
 */
public class BatchApplyException extends RuntimeException {

    private final String batchId;

    public BatchApplyException(String batchId, Throwable cause) {
        super("Batch '" + batchId + "' failed while applying and has been rolled back", cause);
        this.batchId = batchId;
    }

    public String getBatchId() {
        return batchId;
    }
}
