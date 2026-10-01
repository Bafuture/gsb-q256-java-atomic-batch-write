package com.example.gsb.atomic;

/** Outcome of submitting a batch. */
public enum CommitResult {
    /** The batch was committed by this call. */
    COMMITTED,
    /**
     * A batch with the same id was already committed (including after a crash
     * recovery replay); this call was a no-op.
     */
    ALREADY_COMMITTED
}
