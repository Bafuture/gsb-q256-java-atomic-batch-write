package com.example.gsb.atomic;

/**
 * Point-in-time statistics snapshot of an {@link AtomicBatchStore}.
 *
 * @param totalBatches       all commit() calls, including rejected batches and
 *                           idempotent no-op re-commits
 * @param committedBatches   batches successfully committed by this instance
 * @param rolledBackBatches  batches that failed mid-commit and were rolled back
 * @param rejectedBatches    batches rejected for exceeding the size limit
 * @param replayedOnRecovery committed batches replayed from the WAL when this
 *                           instance was opened (crash recovery)
 * @param averageBatchSize   mean entry count over batches actually attempted
 *                           (committed + rolled back); 0 when none
 */
public record Stats(long totalBatches,
                    long committedBatches,
                    long rolledBackBatches,
                    long rejectedBatches,
                    long replayedOnRecovery,
                    double averageBatchSize) {
}
