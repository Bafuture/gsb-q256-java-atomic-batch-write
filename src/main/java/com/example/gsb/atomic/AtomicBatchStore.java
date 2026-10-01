package com.example.gsb.atomic;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A durable key-value store that commits batches of writes atomically.
 *
 * <p>Commit protocol: the full batch is written to the WAL as a BEGIN record
 * and fsynced, then applied to the in-memory table under the write lock, then
 * a COMMIT record is fsynced. Readers hold the read lock, so they always see
 * either the complete old state or the complete new state of a batch, never a
 * mixture.
 *
 * <p>Crash recovery: on open, the WAL is scanned. A batch takes effect only
 * if both its BEGIN and COMMIT records are present and intact; a crash before
 * the COMMIT record therefore leaves the batch fully ineffective, and a crash
 * after it leaves the batch fully effective after replay.
 *
 * <p>Idempotency: batch ids are unique. Re-committing an already committed
 * id (including after a restart) is a no-op returning
 * {@link CommitResult#ALREADY_COMMITTED}.
 */
public final class AtomicBatchStore implements Closeable {

    private static final String WAL_FILE_NAME = "atomic-batch.wal";

    private final WriteAheadLog wal;
    private final int maxBatchSize;

    private final ReentrantReadWriteLock dataLock = new ReentrantReadWriteLock();
    private final Object commitMutex = new Object();
    private final Map<String, String> data = new HashMap<>();
    private final Set<String> committedBatchIds = new HashSet<>();

    private final AtomicLong totalBatches = new AtomicLong();
    private final AtomicLong committedBatches = new AtomicLong();
    private final AtomicLong rolledBackBatches = new AtomicLong();
    private final AtomicLong rejectedBatches = new AtomicLong();
    private final AtomicLong replayedOnRecovery = new AtomicLong();
    private final AtomicLong attemptedEntries = new AtomicLong();

    private volatile CommitHook hook = CommitHook.NONE;
    private volatile boolean closed;

    public AtomicBatchStore(Path directory, int maxBatchSize) throws IOException {
        if (maxBatchSize < 1) {
            throw new IllegalArgumentException("maxBatchSize must be >= 1, got " + maxBatchSize);
        }
        this.maxBatchSize = maxBatchSize;
        this.wal = WriteAheadLog.open(directory.resolve(WAL_FILE_NAME));
        for (WriteAheadLog.RecoveredBatch batch : wal.recoveredBatches()) {
            data.putAll(batch.entries());
            committedBatchIds.add(batch.id());
            replayedOnRecovery.incrementAndGet();
        }
    }

    /** Installs a commit hook (mainly for tests). Never null. */
    public void setCommitHook(CommitHook hook) {
        this.hook = Objects.requireNonNull(hook, "hook");
    }

    /**
     * Atomically commits a batch. On return, either every entry is visible to
     * readers and durable, or (by exception) none is.
     *
     * @throws BatchTooLargeException if the batch exceeds the size limit
     * @throws BatchWriteException    if the commit fails part-way; applied
     *                                modifications have been rolled back
     * @throws SimulatedCrashException if the hook simulates a crash; no
     *                                rollback is performed, as in a real crash
     */
    public CommitResult commit(Batch batch) {
        Objects.requireNonNull(batch, "batch");
        ensureOpen();
        totalBatches.incrementAndGet();
        if (batch.size() > maxBatchSize) {
            rejectedBatches.incrementAndGet();
            throw new BatchTooLargeException(batch.id(), batch.size(), maxBatchSize);
        }
        synchronized (commitMutex) {
            if (committedBatchIds.contains(batch.id())) {
                return CommitResult.ALREADY_COMMITTED;
            }
            attemptedEntries.addAndGet(batch.size());
            try {
                wal.logBegin(batch.id(), batch.entries());
            } catch (IOException e) {
                rolledBackBatches.incrementAndGet();
                throw new BatchWriteException(
                        "Batch '" + batch.id() + "' failed before applying any change", e);
            }
            hook.afterBeginWritten(batch);

            // Undo log: previous value per key, null meaning "was absent".
            Map<String, String> undo = new HashMap<>();
            dataLock.writeLock().lock();
            try {
                try {
                    int index = 0;
                    for (Map.Entry<String, String> entry : batch.entries().entrySet()) {
                        hook.beforeApplyEntry(batch, entry.getKey(), index++);
                        undo.put(entry.getKey(), data.put(entry.getKey(), entry.getValue()));
                    }
                    hook.afterApplyCompleted(batch);
                    wal.logCommit(batch.id());
                    hook.afterCommitWritten(batch);
                } catch (SimulatedCrashException crash) {
                    throw crash; // a real crash performs no rollback either
                } catch (Exception failure) {
                    rollback(undo);
                    wal.logAbort(batch.id());
                    rolledBackBatches.incrementAndGet();
                    throw new BatchWriteException("Batch '" + batch.id()
                            + "' failed mid-commit; all applied changes were rolled back", failure);
                }
            } finally {
                dataLock.writeLock().unlock();
            }
            committedBatchIds.add(batch.id());
            committedBatches.incrementAndGet();
            return CommitResult.COMMITTED;
        }
    }

    private void rollback(Map<String, String> undo) {
        undo.forEach((key, previous) -> {
            if (previous == null) {
                data.remove(key);
            } else {
                data.put(key, previous);
            }
        });
    }

    /** Returns the current value of {@code key}, if present. */
    public Optional<String> get(String key) {
        ensureOpen();
        dataLock.readLock().lock();
        try {
            return Optional.ofNullable(data.get(key));
        } finally {
            dataLock.readLock().unlock();
        }
    }

    /**
     * Returns a consistent snapshot of the requested keys: under the read lock,
     * so a concurrent commit is observed either fully or not at all. Keys
     * without a value are absent from the returned map.
     */
    public Map<String, String> getAll(Collection<String> keys) {
        ensureOpen();
        dataLock.readLock().lock();
        try {
            Map<String, String> snapshot = new LinkedHashMap<>();
            for (String key : keys) {
                String value = data.get(key);
                if (value != null) {
                    snapshot.put(key, value);
                }
            }
            return snapshot;
        } finally {
            dataLock.readLock().unlock();
        }
    }

    public Stats stats() {
        long committed = committedBatches.get();
        long rolledBack = rolledBackBatches.get();
        long attempted = committed + rolledBack;
        double average = attempted == 0 ? 0.0 : (double) attemptedEntries.get() / attempted;
        return new Stats(totalBatches.get(), committed, rolledBack,
                rejectedBatches.get(), replayedOnRecovery.get(), average);
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("store is closed");
        }
    }

    @Override
    public void close() throws IOException {
        closed = true;
        wal.close();
    }
}
