package com.example.gsb.atomic;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Key/value store that commits a batch of writes as a single atomic unit.
 *
 * <p>Guarantees:
 * <ul>
 *   <li><b>Atomic visibility.</b> Entries of a batch never leak before commit.
 *       Readers observe either the whole previous snapshot or the whole new
 *       snapshot, never a mixture of the two.</li>
 *   <li><b>All or nothing.</b> If any entry fails to apply, the whole batch is
 *       rolled back and committed state is untouched.</li>
 *   <li><b>Crash recovery.</b> Durability is provided by a write-ahead log:
 *       a batch is only committed after its COMMIT marker is forced to disk.
 *       On restart, fully committed batches are replayed and batches interrupted
 *       before their COMMIT marker are discarded.</li>
 *   <li><b>Idempotent replay.</b> Batches carry caller-supplied ids; resubmitting
 *       an already committed id (including after recovery) is a no-op returning
 *       {@link CommitResult#ALREADY_COMMITTED}.</li>
 * </ul>
 *
 * <p>Reads are lock-free against an immutable snapshot reference; commits are
 * serialized through a lock.
 */
public final class AtomicBatchStore implements AutoCloseable {

    /** Hook invoked for every entry while a batch is being prepared. */
    @FunctionalInterface
    public interface ApplyHook {
        /**
         * Throwing from this method aborts the whole batch and rolls it back.
         * Useful for validation or for simulating mid-batch failures.
         */
        void beforeApply(String key, String value);
    }

    static final String WAL_FILE_NAME = "atomic-batch.wal";

    private static final ApplyHook NOOP_HOOK = (key, value) -> {
    };

    private final Path directory;
    private final int maxBatchSize;
    private final ApplyHook applyHook;
    private final ReentrantLock commitLock = new ReentrantLock();

    /** Batch ids known to be committed, including those replayed during recovery. */
    private final Set<String> committedBatchIds = new HashSet<>();

    /** Immutable, thread-safe snapshot published to readers. */
    private volatile Map<String, String> snapshot = Collections.emptyMap();

    private final WriteAheadLog wal;

    private final LongAdder batchesSubmitted = new LongAdder();
    private final LongAdder batchesCommitted = new LongAdder();
    private final LongAdder batchesRolledBack = new LongAdder();
    private final LongAdder batchesReplayedOnRecovery = new LongAdder();
    private final LongAdder entriesSubmitted = new LongAdder();

    public AtomicBatchStore(Path directory, int maxBatchSize) throws IOException {
        this(directory, maxBatchSize, NOOP_HOOK);
    }

    public AtomicBatchStore(Path directory, int maxBatchSize, ApplyHook applyHook) throws IOException {
        Objects.requireNonNull(directory, "directory");
        Objects.requireNonNull(applyHook, "applyHook");
        if (maxBatchSize < 1) {
            throw new IllegalArgumentException("maxBatchSize must be at least 1, got " + maxBatchSize);
        }
        this.directory = directory;
        this.maxBatchSize = maxBatchSize;
        this.applyHook = applyHook;
        Files.createDirectories(directory);

        recover();
        this.wal = WriteAheadLog.open(directory.resolve(WAL_FILE_NAME));
    }

    private void recover() throws IOException {
        List<WriteAheadLog.CommittedBatch> batches =
                WriteAheadLog.readCommitted(directory.resolve(WAL_FILE_NAME));
        Map<String, String> state = new HashMap<>();
        for (WriteAheadLog.CommittedBatch batch : batches) {
            // Dedup defensively: a batch id committed once is applied at most once.
            if (committedBatchIds.add(batch.batchId())) {
                state.putAll(batch.entries());
                batchesReplayedOnRecovery.increment();
            }
        }
        snapshot = Collections.unmodifiableMap(state);
    }

    /**
     * Atomically commits one batch of key/value writes.
     *
     * @throws BatchTooLargeException if the batch exceeds {@code maxBatchSize}
     * @throws BatchApplyException    if applying an entry fails; the batch is fully rolled back
     */
    public CommitResult commit(String batchId, Map<String, String> entries) {
        Objects.requireNonNull(batchId, "batchId");
        Objects.requireNonNull(entries, "entries");
        if (batchId.isEmpty()) {
            throw new IllegalArgumentException("batchId must not be empty");
        }
        if (entries.size() > maxBatchSize) {
            throw new BatchTooLargeException(entries.size(), maxBatchSize);
        }

        commitLock.lock();
        try {
            // Idempotency by batch id, covering post-crash replay as well.
            if (committedBatchIds.contains(batchId)) {
                return CommitResult.ALREADY_COMMITTED;
            }

            batchesSubmitted.increment();
            entriesSubmitted.add(entries.size());

            // Phase 1: write BEGIN. No durable commit point yet: if the process
            // dies here, recovery discards the orphan BEGIN.
            try {
                wal.appendBegin(batchId, entries);
            } catch (IOException failure) {
                batchesRolledBack.increment();
                throw new BatchApplyException(batchId, failure);
            }

            // Phase 2: prepare the new state on a private copy. A failure here
            // never touches the published snapshot, and the COMMIT-less BEGIN
            // in the log is discarded on recovery.
            Map<String, String> next = new HashMap<>(snapshot);
            try {
                for (Map.Entry<String, String> entry : entries.entrySet()) {
                    applyHook.beforeApply(entry.getKey(), entry.getValue());
                    next.put(entry.getKey(), entry.getValue());
                }
            } catch (RuntimeException failure) {
                batchesRolledBack.increment();
                throw new BatchApplyException(batchId, failure);
            }

            // Phase 3: the durable commit point. A crash after this completes
            // causes recovery to apply the whole batch; a crash before it
            // discards the whole batch.
            try {
                wal.appendCommit(batchId);
                wal.sync();
            } catch (IOException failure) {
                batchesRolledBack.increment();
                throw new BatchApplyException(batchId, failure);
            }

            // Phase 4: publish. Just a reference swap to an already fully built
            // immutable map, so it cannot fail or expose a partial state.
            snapshot = Collections.unmodifiableMap(next);
            committedBatchIds.add(batchId);
            batchesCommitted.increment();
            return CommitResult.COMMITTED;
        } finally {
            commitLock.unlock();
        }
    }

    /** Returns the current value of {@code key}, or empty if absent. */
    public Optional<String> get(String key) {
        Objects.requireNonNull(key, "key");
        return Optional.ofNullable(snapshot.get(key));
    }

    /**
     * Reads many keys from a single snapshot: all returned values belong to the
     * same committed version, so callers never observe a mixed batch state.
     */
    public Map<String, String> getAll(Collection<String> keys) {
        Objects.requireNonNull(keys, "keys");
        Map<String, String> view = snapshot;
        Map<String, String> result = new LinkedHashMap<>();
        for (String key : keys) {
            String value = view.get(Objects.requireNonNull(key, "key"));
            if (value != null) {
                result.put(key, value);
            }
        }
        return Collections.unmodifiableMap(result);
    }

    /** Number of committed keys currently visible. */
    public int size() {
        return snapshot.size();
    }

    /** Snapshot of the statistics counters. */
    public BatchStats stats() {
        long submitted = batchesSubmitted.sum();
        long committed = batchesCommitted.sum();
        long rolledBack = batchesRolledBack.sum();
        long replayed = batchesReplayedOnRecovery.sum();
        double average = submitted == 0 ? 0.0d : (double) entriesSubmitted.sum() / submitted;
        return new BatchStats(submitted, committed, rolledBack, replayed, average);
    }

    @Override
    public void close() throws IOException {
        wal.close();
    }
}
