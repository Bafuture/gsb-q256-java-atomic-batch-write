package com.example.gsb.atomic;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class AtomicCommitTest {

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
    void batchIsInvisibleBeforeCommitAndFullyVisibleAfter() throws Exception {
        try (AtomicBatchStore store = new AtomicBatchStore(dir, 100)) {
            CountDownLatch insideCommit = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            store.setCommitHook(new CommitHook() {
                @Override
                public void afterBeginWritten(Batch b) {
                    insideCommit.countDown();
                    try {
                        assertThat(release.await(5, TimeUnit.SECONDS)).isTrue();
                    } catch (InterruptedException e) {
                        throw new AssertionError(e);
                    }
                }
            });

            Thread committer = new Thread(() -> store.commit(batch("b1", "a", "1", "b", "2")));
            committer.start();

            assertThat(insideCommit.await(5, TimeUnit.SECONDS)).isTrue();
            // Commit is in progress but not completed: nothing is visible.
            assertThat(store.get("a")).isEmpty();
            assertThat(store.get("b")).isEmpty();

            release.countDown();
            committer.join(5000);
            assertThat(committer.isAlive()).isFalse();

            assertThat(store.get("a")).contains("1");
            assertThat(store.get("b")).contains("2");
            assertThat(store.getAll(java.util.List.of("a", "b")))
                    .containsExactlyInAnyOrderEntriesOf(Map.of("a", "1", "b", "2"));
        }
    }

    @Test
    void laterBatchOverwritesAndAddsKeysAtomically() throws Exception {
        try (AtomicBatchStore store = new AtomicBatchStore(dir, 100)) {
            assertThat(store.commit(batch("b1", "a", "1", "b", "2")))
                    .isEqualTo(CommitResult.COMMITTED);
            assertThat(store.commit(batch("b2", "a", "10", "c", "3")))
                    .isEqualTo(CommitResult.COMMITTED);

            assertThat(store.get("a")).isEqualTo(Optional.of("10"));
            assertThat(store.get("b")).contains("2");
            assertThat(store.get("c")).contains("3");
        }
    }
}
