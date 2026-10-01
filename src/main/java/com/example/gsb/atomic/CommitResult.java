package com.example.gsb.atomic;

/** Outcome of a successful {@link AtomicBatchStore#commit(Batch)} call. */
public enum CommitResult {
    /** The batch was applied and its commit record was flushed to the WAL. */
    COMMITTED,
    /** A batch with the same id was already committed; the call was a no-op. */
    ALREADY_COMMITTED
}
