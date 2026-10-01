package com.example.gsb.atomic;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class BatchSizeLimitTest {

    @TempDir
    Path dir;

    private static Batch batch(String id, int size) {
        Map<String, String> entries = new LinkedHashMap<>();
        for (int i = 0; i < size; i++) {
            entries.put("k" + i, "v" + i);
        }
        return new Batch(id, entries);
    }

    @Test
    void oversizedBatchIsRejectedWithHelpfulMessage() throws Exception {
        try (AtomicBatchStore store = new AtomicBatchStore(dir, 2)) {
            assertThat(store.commit(batch("ok", 2))).isEqualTo(CommitResult.COMMITTED);

            assertThatThrownBy(() -> store.commit(batch("too-big", 3)))
                    .isInstanceOf(BatchTooLargeException.class)
                    .hasMessageContaining("too-big")
                    .hasMessageContaining("3")
                    .hasMessageContaining("2");

            // Rejected batch applied nothing.
            assertThat(store.get("k2")).isEmpty();
            assertThat(store.get("k0")).contains("v0");

            Stats stats = store.stats();
            assertThat(stats.rejectedBatches()).isEqualTo(1);
            assertThat(stats.committedBatches()).isEqualTo(1);
        }
    }

    @Test
    void invalidLimitIsRejected() {
        org.assertj.core.api.Assertions.assertThatIllegalArgumentException()
                .isThrownBy(() -> new AtomicBatchStore(dir, 0))
                .withMessageContaining("maxBatchSize");
    }
}
