package com.example.gsb.atomic;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class StatsTest {

    @TempDir
    Path dir;

    private static Batch batch(String id, int size) {
        Map<String, String> entries = new LinkedHashMap<>();
        for (int i = 0; i < size; i++) {
            entries.put(id + "-k" + i, "v" + i);
        }
        return new Batch(id, entries);
    }

    @Test
    void statsTrackCommitsRollbacksRejectionsAndAverageSize() throws Exception {
        AtomicBatchStore store = newAtomicBatchStoreWithFailingHook();
        Stats stats = store.stats();
        assertThat(stats.totalBatches()).isEqualTo(4);
        assertThat(stats.committedBatches()).isEqualTo(2);
        assertThat(stats.rolledBackBatches()).isEqualTo(1);
        assertThat(stats.rejectedBatches()).isEqualTo(1);
        assertThat(stats.replayedOnRecovery()).isZero();
        // Attempted batches: sizes 2, 4, 2 -> average 8/3.
        assertThat(stats.averageBatchSize()).isCloseTo(8.0 / 3.0, within(1e-9));
        store.close();

        try (AtomicBatchStore recovered = new AtomicBatchStore(dir, 100)) {
            Stats after = recovered.stats();
            assertThat(after.replayedOnRecovery()).isEqualTo(2);
            assertThat(after.committedBatches()).isZero();
            assertThat(after.totalBatches()).isZero();
        }
    }

    private AtomicBatchStore newAtomicBatchStoreWithFailingHook() throws Exception {
        AtomicBatchStore store = new AtomicBatchStore(dir, 100);
        store.commit(batch("b1", 2));
        store.commit(batch("b2", 4));
        store.setCommitHook(new CommitHook() {
            @Override
            public void beforeApplyEntry(Batch b, String key, int index) {
                if (index == 1) {
                    throw new RuntimeException("boom");
                }
            }
        });
        assertThatThrownBy(() -> store.commit(batch("b3", 2)))
                .isInstanceOf(BatchWriteException.class);
        store.setCommitHook(CommitHook.NONE);
        assertThatThrownBy(() -> store.commit(batch("b4", 101)))
                .isInstanceOf(BatchTooLargeException.class);
        return store;
    }
}
