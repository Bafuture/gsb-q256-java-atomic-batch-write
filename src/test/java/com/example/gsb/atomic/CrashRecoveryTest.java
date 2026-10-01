package com.example.gsb.atomic;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CrashRecoveryTest {

    @TempDir
    Path dir;

    private static Batch batch(String id, String... kv) {
        Map<String, String> entries = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            entries.put(kv[i], kv[i + 1]);
        }
        return new Batch(id, entries);
    }

    private void abandonAfterCrash(AtomicBatchStore store) throws Exception {
        // Simulate the process disappearing. Closing only releases the handle;
        // it does not rewrite the WAL, so recovery sees exactly what was on disk.
        store.close();
    }

    @Test
    void crashBeforeCommitRecordLeavesBatchFullyIneffective() throws Exception {
        AtomicBatchStore store = new AtomicBatchStore(dir, 100);
        store.commit(batch("seed", "a", "oldA", "b", "oldB"));

        // Crash after BEGIN was fsynced but before any entry is applied.
        store.setCommitHook(new CommitHook() {
            @Override
            public void afterBeginWritten(Batch b) {
                throw new SimulatedCrashException("power loss before apply");
            }
        });
        catchThrowableOfType(() -> store.commit(batch("b1", "a", "newA", "b", "newB")),
                SimulatedCrashException.class);
        abandonAfterCrash(store);

        try (AtomicBatchStore recovered = new AtomicBatchStore(dir, 100)) {
            // 全部不生效: old state intact, nothing half-applied.
            assertThat(recovered.get("a")).contains("oldA");
            assertThat(recovered.get("b")).contains("oldB");
            // Only the seed batch is replayed.
            assertThat(recovered.stats().replayedOnRecovery()).isEqualTo(1);
        }
    }

    @Test
    void crashAfterBeginAndPartlyAppliedBeforeCommitIsStillAllOrNothing() throws Exception {
        AtomicBatchStore store = new AtomicBatchStore(dir, 100);
        store.commit(batch("seed", "a", "oldA", "b", "oldB", "c", "oldC"));

        // Crash mid-apply: BEGIN on disk, COMMIT missing, in-memory table half dirty
        // (the dirty memory dies with the "process").
        store.setCommitHook(new CommitHook() {
            @Override
            public void beforeApplyEntry(Batch b, String key, int index) {
                if (index == 1) {
                    throw new SimulatedCrashException("power loss during apply");
                }
            }
        });
        catchThrowableOfType(() -> store.commit(batch("b1", "a", "newA", "b", "newB", "c", "newC")),
                SimulatedCrashException.class);
        abandonAfterCrash(store);

        try (AtomicBatchStore recovered = new AtomicBatchStore(dir, 100)) {
            assertThat(recovered.get("a")).contains("oldA");
            assertThat(recovered.get("b")).contains("oldB");
            assertThat(recovered.get("c")).contains("oldC");
        }
    }

    @Test
    void crashAfterCommitRecordMakesBatchFullyEffective() throws Exception {
        AtomicBatchStore store = new AtomicBatchStore(dir, 100);
        store.commit(batch("seed", "a", "oldA", "b", "oldB"));

        // Crash right after the COMMIT record was fsynced: the batch is durable
        // even though nobody returns from commit().
        store.setCommitHook(new CommitHook() {
            @Override
            public void afterCommitWritten(Batch b) {
                throw new SimulatedCrashException("power loss after commit");
            }
        });
        catchThrowableOfType(() -> store.commit(batch("b1", "a", "newA", "b", "newB")),
                SimulatedCrashException.class);
        abandonAfterCrash(store);

        try (AtomicBatchStore recovered = new AtomicBatchStore(dir, 100)) {
            // 全部生效: replay reconstructs the whole batch.
            assertThat(recovered.get("a")).contains("newA");
            assertThat(recovered.get("b")).contains("newB");
            assertThat(recovered.stats().replayedOnRecovery()).isEqualTo(2);
        }
    }

    @Test
    void dataSurvivesRestartAndBatchesReplayInCommitOrder() throws Exception {
        try (AtomicBatchStore store = new AtomicBatchStore(dir, 100)) {
            store.commit(batch("b1", "k", "v1"));
            store.commit(batch("b2", "k", "v2"));
        }
        try (AtomicBatchStore recovered = new AtomicBatchStore(dir, 100)) {
            assertThat(recovered.get("k")).contains("v2");
            assertThat(recovered.stats().replayedOnRecovery()).isEqualTo(2);
        }
    }

    @Test
    void tornCommitRecordLeavesBatchUneffective() throws Exception {
        Path wal = dir.resolve("atomic-batch.wal");
        try (AtomicBatchStore store = new AtomicBatchStore(dir, 100)) {
            store.commit(batch("b1", "a", "1"));
        }
        long length = java.nio.file.Files.size(wal);

        // Simulate a torn trailing record: the COMMIT record is only partially
        // on disk, so the batch must count as not committed.
        try (java.nio.channels.FileChannel ch = java.nio.channels.FileChannel.open(
                wal, java.nio.file.StandardOpenOption.WRITE)) {
            ch.truncate(length - 3);
        }
        try (AtomicBatchStore recovered = new AtomicBatchStore(dir, 100)) {
            assertThat(recovered.get("a")).isEmpty();
        }
    }

    @Test
    void garbageWalTailIsTruncatedAndStoreKeepsWorking() throws Exception {
        Path wal = dir.resolve("atomic-batch.wal");
        try (AtomicBatchStore store = new AtomicBatchStore(dir, 100)) {
            store.commit(batch("b1", "a", "1"));
        }
        // Append garbage (e.g. zeroes from a failed fsynced write): must be discarded.
        try (java.io.OutputStream out = java.nio.file.Files.newOutputStream(
                wal, java.nio.file.StandardOpenOption.APPEND)) {
            out.write(new byte[] {7, 0, 0, 0, 42});
        }
        try (AtomicBatchStore recovered = new AtomicBatchStore(dir, 100)) {
            assertThat(recovered.get("a")).contains("1");
            // After recovery the store still works and logs clean records.
            assertThat(recovered.commit(batch("b2", "b", "2")))
                    .isEqualTo(CommitResult.COMMITTED);
        }
    }
}
