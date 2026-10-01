package com.example.gsb.atomic;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class AtomicBatchStoreTest {

    @TempDir
    Path dir;

    private static Map<String, String> batchOf(String prefix, int count, String value) {
        Map<String, String> entries = new LinkedHashMap<>();
        for (int i = 0; i < count; i++) {
            entries.put(prefix + i, value);
        }
        return entries;
    }

    // 1) Atomic commit: entries invisible before commit, all visible after.
    @Test
    void commitsBatchAtomicallyAndKeepsItInvisibleBeforehand() throws Exception {
        try (AtomicBatchStore store = new AtomicBatchStore(dir, 100)) {
            Map<String, String> entries = batchOf("k", 5, "v1");
            for (String key : entries.keySet()) {
                assertThat(store.get(key)).isEmpty();
            }

            assertThat(store.commit("b1", entries)).isEqualTo(CommitResult.COMMITTED);

            assertThat(store.getAll(entries.keySet()))
                    .containsExactlyInAnyOrderEntriesOf(entries);
            assertThat(store.size()).isEqualTo(5);
        }
    }

    @Test
    void doesNotPublishEntriesWhileCommitIsInProgress() throws Exception {
        CountDownLatch applyStarted = new CountDownLatch(1);
        CountDownLatch releaseApply = new CountDownLatch(1);
        AtomicBatchStore.ApplyHook blockingHook = (key, value) -> {
            applyStarted.countDown();
            try {
                if (!releaseApply.await(5, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("timed out");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        };
        try (AtomicBatchStore store = new AtomicBatchStore(dir, 100, blockingHook)) {
            Map<String, String> entries = batchOf("k", 3, "v1");
            ExecutorService pool = Executors.newSingleThreadExecutor();
            Future<CommitResult> commit = pool.submit(() -> store.commit("b1", entries));
            try {
                assertThat(applyStarted.await(5, TimeUnit.SECONDS)).isTrue();
                // Commit is mid-flight: nothing of the batch may be visible.
                assertThat(store.getAll(entries.keySet())).isEmpty();
                releaseApply.countDown();
                assertThat(commit.get(5, TimeUnit.SECONDS)).isEqualTo(CommitResult.COMMITTED);
                assertThat(store.getAll(entries.keySet()))
                        .containsExactlyInAnyOrderEntriesOf(entries);
            } finally {
                releaseApply.countDown();
                pool.shutdownNow();
            }
        }
    }

    // 2) All-or-nothing rollback when a mid-batch step fails.
    @Test
    void rollsBackWholeBatchWhenApplyFailsMidway() throws Exception {
        AtomicInteger applied = new AtomicInteger();
        AtomicBatchStore.ApplyHook failingHook = (key, value) -> {
            if (applied.incrementAndGet() == 3) {
                throw new IllegalStateException("simulated failure on third entry");
            }
        };
        try (AtomicBatchStore store = new AtomicBatchStore(dir, 100, failingHook)) {
            store.commit("seed", Map.of("existing", "old"));

            Map<String, String> entries = batchOf("k", 5, "v1");
            assertThatThrownBy(() -> store.commit("bad-batch", entries))
                    .isInstanceOf(BatchApplyException.class)
                    .hasMessageContaining("bad-batch")
                    .hasMessageContaining("rolled back");

            // Nothing from the failed batch is visible; prior state untouched.
            assertThat(store.getAll(entries.keySet())).isEmpty();
            assertThat(store.get("existing")).contains("old");
            assertThat(store.stats().batchesRolledBack()).isEqualTo(1);

            // The store stays usable after a rollback.
            assertThat(store.commit("good", Map.of("after", "ok"))).isEqualTo(CommitResult.COMMITTED);
            assertThat(store.get("after")).contains("ok");
        }

        // The rolled-back batch leaves no COMMIT marker, so recovery discards it.
        try (AtomicBatchStore recovered = new AtomicBatchStore(dir, 100)) {
            assertThat(recovered.getAll(batchOf("k", 5, "v1").keySet())).isEmpty();
            assertThat(recovered.get("existing")).contains("old");
            assertThat(recovered.get("after")).contains("ok");
        }
    }

    // 3) Crash recovery, state "all applied": COMMIT marker reached disk.
    @Test
    void recoveryAppliesWholeBatchWhenCommitMarkerWasDurable() throws Exception {
        Map<String, String> entries = batchOf("k", 4, "v1");
        try (WriteAheadLog wal = WriteAheadLog.open(dir.resolve(AtomicBatchStore.WAL_FILE_NAME))) {
            wal.appendBegin("crashed-after-commit", entries);
            wal.appendCommit("crashed-after-commit");
            wal.sync();
            // "Crash": process dies before the in-memory state was ever updated.
        }

        try (AtomicBatchStore recovered = new AtomicBatchStore(dir, 100)) {
            assertThat(recovered.getAll(entries.keySet()))
                    .containsExactlyInAnyOrderEntriesOf(entries);
            assertThat(recovered.stats().batchesReplayedOnRecovery()).isEqualTo(1);
        }
    }

    // 3) Crash recovery, state "nothing applied": no COMMIT marker.
    @Test
    void recoveryDiscardsBatchWhenCrashHappenedBeforeCommitMarker() throws Exception {
        Map<String, String> entries = batchOf("k", 4, "v1");
        try (WriteAheadLog wal = WriteAheadLog.open(dir.resolve(AtomicBatchStore.WAL_FILE_NAME))) {
            wal.appendBegin("crashed-before-commit", entries);
            wal.sync();
            // "Crash": BEGIN is durable but the batch never reached its commit point.
        }

        try (AtomicBatchStore recovered = new AtomicBatchStore(dir, 100)) {
            assertThat(recovered.getAll(entries.keySet())).isEmpty();
            assertThat(recovered.size()).isZero();
            assertThat(recovered.stats().batchesReplayedOnRecovery()).isZero();
        }
    }

    @Test
    void recoveryIgnoresTruncatedTailLeftByCrashMidWrite() throws Exception {
        Map<String, String> entries = batchOf("k", 3, "v1");
        Path walPath = dir.resolve(AtomicBatchStore.WAL_FILE_NAME);
        try (WriteAheadLog wal = WriteAheadLog.open(walPath)) {
            wal.appendBegin("full", entries);
            wal.appendCommit("full");
            wal.appendBegin("partial", entries); // never committed
            wal.sync();
        }
        // Simulate a crash that tore the last write: chop the file mid-record.
        long size = java.nio.file.Files.size(walPath);
        try (var channel = java.nio.channels.FileChannel.open(
                walPath, java.nio.file.StandardOpenOption.WRITE)) {
            channel.truncate(size - 3);
        }

        try (AtomicBatchStore recovered = new AtomicBatchStore(dir, 100)) {
            assertThat(recovered.getAll(entries.keySet()))
                    .containsExactlyInAnyOrderEntriesOf(entries);
            assertThat(recovered.stats().batchesReplayedOnRecovery()).isEqualTo(1);
        }
    }

    // 4) Idempotent replay by batch id, in-process and across crash recovery.
    @Test
    void resubmittingSameBatchIdIsANoOp() throws Exception {
        try (AtomicBatchStore store = new AtomicBatchStore(dir, 100)) {
            assertThat(store.commit("b1", Map.of("k", "v1"))).isEqualTo(CommitResult.COMMITTED);
            assertThat(store.commit("b1", Map.of("k", "v2"))).isEqualTo(CommitResult.ALREADY_COMMITTED);

            assertThat(store.get("k")).contains("v1");
            assertThat(store.stats().batchesCommitted()).isEqualTo(1);
        }
    }

    @Test
    void resubmittingSameBatchIdAfterRecoveryIsANoOp() throws Exception {
        try (AtomicBatchStore store = new AtomicBatchStore(dir, 100)) {
            store.commit("b1", Map.of("k1", "v1", "k2", "v1"));
        }

        try (AtomicBatchStore recovered = new AtomicBatchStore(dir, 100)) {
            assertThat(recovered.stats().batchesReplayedOnRecovery()).isEqualTo(1);
            // Same id, different payload: must not take effect again.
            assertThat(recovered.commit("b1", Map.of("k1", "changed")))
                    .isEqualTo(CommitResult.ALREADY_COMMITTED);
            assertThat(recovered.get("k1")).contains("v1");
            assertThat(recovered.get("k2")).contains("v1");
            assertThat(recovered.stats().batchesCommitted()).isZero();
        }
    }

    @Test
    void recoveryAppliesDuplicateCommittedBatchIdsOnlyOnce() throws Exception {
        Path walPath = dir.resolve(AtomicBatchStore.WAL_FILE_NAME);
        try (WriteAheadLog wal = WriteAheadLog.open(walPath)) {
            wal.appendBegin("dup", Map.of("k", "v1"));
            wal.appendCommit("dup");
            wal.appendBegin("dup", Map.of("k", "v1"));
            wal.appendCommit("dup");
            wal.sync();
        }

        try (AtomicBatchStore recovered = new AtomicBatchStore(dir, 100)) {
            assertThat(recovered.get("k")).contains("v1");
            assertThat(recovered.stats().batchesReplayedOnRecovery()).isEqualTo(1);
        }
    }

    // 5) Concurrent readers never observe a mixed old/new batch state.
    @Test
    void concurrentReadersSeeEitherWholeOldOrWholeNewSnapshot() throws Exception {
        int keyCount = 8;
        int rounds = 60;
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < keyCount; i++) {
            keys.add("k" + i);
        }

        try (AtomicBatchStore store = new AtomicBatchStore(dir, 100)) {
            Map<String, String> initial = new LinkedHashMap<>();
            keys.forEach(k -> initial.put(k, "v0"));
            store.commit("init", initial);

            ConcurrentLinkedQueue<String> violations = new ConcurrentLinkedQueue<>();
            AtomicBoolean writerDone = new AtomicBoolean(false);
            ExecutorService pool = Executors.newFixedThreadPool(5);
            try {
                Future<?> writer = pool.submit(() -> {
                    for (int round = 1; round <= rounds; round++) {
                        Map<String, String> entries = new LinkedHashMap<>();
                        String value = "v" + round;
                        keys.forEach(k -> entries.put(k, value));
                        store.commit("round-" + round, entries);
                    }
                    writerDone.set(true);
                });

                List<Future<?>> readers = new ArrayList<>();
                for (int r = 0; r < 4; r++) {
                    readers.add(pool.submit(() -> {
                        while (!writerDone.get()) {
                            Map<String, String> view = store.getAll(keys);
                            if (view.size() != keyCount) {
                                violations.add("missing keys: " + view);
                                continue;
                            }
                            long distinct = view.values().stream().distinct().count();
                            if (distinct != 1) {
                                violations.add("mixed snapshot: " + view);
                            }
                        }
                    }));
                }

                writer.get(30, TimeUnit.SECONDS);
                for (Future<?> reader : readers) {
                    reader.get(30, TimeUnit.SECONDS);
                }
            } finally {
                pool.shutdownNow();
            }

            assertThat(violations).isEmpty();
            assertThat(store.getAll(keys).values()).allMatch(value -> value.equals("v" + rounds));
        }
    }

    // 6) Batch size limit with a clear rejection message.
    @Test
    void rejectsOversizedBatchWithDescriptiveError() throws Exception {
        try (AtomicBatchStore store = new AtomicBatchStore(dir, 3)) {
            Map<String, String> tooBig = batchOf("k", 4, "v1");
            assertThatThrownBy(() -> store.commit("too-big", tooBig))
                    .isInstanceOf(BatchTooLargeException.class)
                    .hasMessageContaining("4")
                    .hasMessageContaining("3");

            assertThat(store.getAll(tooBig.keySet())).isEmpty();
            // Rejected batches never enter the pipeline and are not counted.
            assertThat(store.stats().batchesSubmitted()).isZero();

            assertThat(store.commit("fits", batchOf("k", 3, "v1"))).isEqualTo(CommitResult.COMMITTED);
        }
    }

    // 7) Statistics: submitted, committed, rolled back, replayed, average size.
    @Test
    void tracksAllStatistics() throws Exception {
        AtomicBatchStore.ApplyHook failOnSecond = new AtomicBatchStore.ApplyHook() {
            private int count;

            @Override
            public void beforeApply(String key, String value) {
                if (key.startsWith("boom") && ++count > 1) {
                    throw new IllegalStateException("boom");
                }
            }
        };

        try (AtomicBatchStore store = new AtomicBatchStore(dir, 100, failOnSecond)) {
            store.commit("b1", batchOf("a", 2, "v"));          // 2 entries
            store.commit("b2", batchOf("b", 4, "v"));          // 4 entries
            assertThatThrownBy(() -> store.commit("b3", batchOf("boom", 2, "v")))
                    .isInstanceOf(BatchApplyException.class); // 2 entries, rolled back

            BatchStats stats = store.stats();
            assertThat(stats.batchesSubmitted()).isEqualTo(3);
            assertThat(stats.batchesCommitted()).isEqualTo(2);
            assertThat(stats.batchesRolledBack()).isEqualTo(1);
            assertThat(stats.batchesReplayedOnRecovery()).isZero();
            assertThat(stats.averageBatchSize()).isCloseTo(8.0 / 3.0, within(1e-9));
        }

        try (AtomicBatchStore recovered = new AtomicBatchStore(dir, 100)) {
            BatchStats stats = recovered.stats();
            assertThat(stats.batchesReplayedOnRecovery()).isEqualTo(2);
            assertThat(stats.batchesSubmitted()).isZero();
            assertThat(stats.averageBatchSize()).isZero();
        }
    }
}
