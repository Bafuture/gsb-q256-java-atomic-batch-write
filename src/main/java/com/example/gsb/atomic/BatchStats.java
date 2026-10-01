package com.example.gsb.atomic;

/**
 * Immutable snapshot of store counters.
 *
 * @param batchesSubmitted          batches that passed validation and entered the commit pipeline
 * @param batchesCommitted          batches durably committed
 * @param batchesRolledBack         batches that failed and were rolled back
 * @param batchesReplayedOnRecovery committed batches re-applied from the log during startup recovery
 * @param averageBatchSize          mean number of entries per submitted batch (0 when none submitted)
 */
public record BatchStats(long batchesSubmitted,
                         long batchesCommitted,
                         long batchesRolledBack,
                         long batchesReplayedOnRecovery,
                         double averageBatchSize) {
}
