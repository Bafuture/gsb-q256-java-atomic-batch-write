package com.example.gsb.atomic;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RollbackTest {

    @TempDir
    Path dir;

    private static Batch batch(String id, String... kv) {
        Map<String, String> entries = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            entries.put(kv[i], kv[i + 1]);
        }
        return new Batch(id, entries);
    }

    @Test
    void failureDuringApplyRollsBackAllAppliedModifications() throws Exception {
        try (AtomicBatchStore store = new AtomicBatchStore(dir, 100)) {
            store.commit(batch("seed", "k0", "old0", "k1", "old1", "k2", "old2"));

            // Fails on the 2nd entry (index 1), so k0 has already been applied.
            store.setCommitHook(new CommitHook() {
                @Override
                public void beforeApplyEntry(Batch b, String key, int index) {
                    if (index == 1) {
                        throw new RuntimeException("simulated disk failure on " + key);
                    }
                }
            });

            assertThatThrownBy(() -> store.commit(
                    batch("b1", "k0", "new0", "k1", "new1", "k2", "new2")))
                    .isInstanceOf(BatchWriteException.class)
                    .hasMessageContaining("b1")
                    .cause().hasMessageContaining("simulated disk failure");

            // No half-applied state: every key keeps its old value.
            assertThat(store.get("k0")).contains("old0");
            assertThat(store.get("k1")).contains("old1");
            assertThat(store.get("k2")).contains("old2");

            Stats stats = store.stats();
            assertThat(stats.rolledBackBatches()).isEqualTo(1);
            assertThat(stats.committedBatches()).isEqualTo(1);

            // A new key introduced by the failed batch must also be undone.
            store.setCommitHook(new CommitHook() {
                @Override
                public void beforeApplyEntry(Batch b, String key, int index) {
                    if (index == 1) {
                        throw new RuntimeException("boom");
                    }
                }
            });
            assertThatThrownBy(() -> store.commit(batch("b2", "fresh", "x", "boom-key", "y")))
                    .isInstanceOf(BatchWriteException.class);
            assertThat(store.get("fresh")).isEmpty();
        }
    }

    @Test
    void failedBatchCanBeRetriedAndCommittedAfterwards() throws Exception {
        try (AtomicBatchStore store = new AtomicBatchStore(dir, 100)) {
            store.setCommitHook(new CommitHook() {
                private boolean armed = true;

                @Override
                public void beforeApplyEntry(Batch b, String key, int index) {
                    if (armed && index == 0) {
                        armed = false;
                        throw new RuntimeException("transient failure");
                    }
                }
            });
            assertThatThrownBy(() -> store.commit(batch("b1", "a", "1")))
                    .isInstanceOf(BatchWriteException.class);
            assertThat(store.commit(batch("b1", "a", "1"))).isEqualTo(CommitResult.COMMITTED);
            assertThat(store.get("a")).contains("1");
        }
    }
}
