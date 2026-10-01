package com.example.gsb.atomic;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class IdempotentReplayTest {

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
    void recommittingSameBatchIdAfterCrashRecoveryHasNoEffect() throws Exception {
        try (AtomicBatchStore store = new AtomicBatchStore(dir, 100)) {
            assertThat(store.commit(batch("b1", "a", "1", "b", "2")))
                    .isEqualTo(CommitResult.COMMITTED);
        }
        // Process restart: recovered store replays b1 from the WAL.
        try (AtomicBatchStore recovered = new AtomicBatchStore(dir, 100)) {
            assertThat(recovered.get("a")).contains("1");

            // Same id, even with different payload: must not take effect.
            CommitResult result = recovered.commit(batch("b1", "a", "999", "b", "888"));
            assertThat(result).isEqualTo(CommitResult.ALREADY_COMMITTED);
            assertThat(recovered.get("a")).contains("1");
            assertThat(recovered.get("b")).contains("2");

            Stats stats = recovered.stats();
            assertThat(stats.committedBatches()).isZero();
            assertThat(stats.totalBatches()).isEqualTo(1);
            assertThat(stats.replayedOnRecovery()).isEqualTo(1);
        }
    }

    @Test
    void duplicateIdWithinSameProcessIsAlsoIdempotent() throws Exception {
        try (AtomicBatchStore store = new AtomicBatchStore(dir, 100)) {
            assertThat(store.commit(batch("b1", "a", "1"))).isEqualTo(CommitResult.COMMITTED);
            assertThat(store.commit(batch("b1", "a", "2")))
                    .isEqualTo(CommitResult.ALREADY_COMMITTED);
            assertThat(store.get("a")).contains("1");
            assertThat(store.stats().committedBatches()).isEqualTo(1);
            assertThat(store.stats().totalBatches()).isEqualTo(2);
        }
    }
}
