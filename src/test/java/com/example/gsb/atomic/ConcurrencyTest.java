package com.example.gsb.atomic;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ConcurrencyTest {

    @TempDir
    Path dir;

    @Test
    void readersObserveWholeBatchOldOrNewButNeverAMix() throws Exception {
        int keyCount = 8;
        int generations = 200;
        int readerCount = 4;
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < keyCount; i++) {
            keys.add("k" + i);
        }

        AtomicBatchStore store = new AtomicBatchStore(dir, 1000);
        // Widen the window while entries are being applied under the write lock.
        store.setCommitHook(new CommitHook() {
            @Override
            public void beforeApplyEntry(Batch batch, String key, int index) {
                Thread.yield();
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });

        ExecutorService pool = Executors.newFixedThreadPool(readerCount + 1);
        CountDownLatch start = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Set<String> observedGenerations = ConcurrentHashMap.newKeySet();

        Future<?> writer = pool.submit(() -> {
            start.await();
            for (int gen = 1; gen <= generations; gen++) {
                Map<String, String> entries = new LinkedHashMap<>();
                for (String key : keys) {
                    entries.put(key, "gen-" + gen);
                }
                store.commit(new Batch("gen-" + gen, entries));
                if (failure.get() != null) {
                    break;
                }
            }
            return null;
        });

        List<Future<?>> readers = new ArrayList<>();
        for (int r = 0; r < readerCount; r++) {
            readers.add(pool.submit(() -> {
                start.await();
                while (true) {
                    Map<String, String> snapshot = store.getAll(keys);
                    Set<String> distinct = keys.stream()
                            .map(k -> snapshot.getOrDefault(k, "<absent>"))
                            .collect(Collectors.toSet());
                    // The core invariant: never mix two generations (or new and absent).
                    if (distinct.size() != 1) {
                        failure.compareAndSet(null,
                                new AssertionError("torn read observed: " + distinct));
                        return null;
                    }
                    String value = distinct.iterator().next();
                    observedGenerations.add(value);
                    if (writer.isDone()) {
                        return null;
                    }
                }
            }));
        }

        start.countDown();
        writer.get(30, TimeUnit.SECONDS);
        for (Future<?> reader : readers) {
            reader.get(30, TimeUnit.SECONDS);
        }
        pool.shutdown();
        assertThat(pool.awaitTermination(10, TimeUnit.SECONDS)).isTrue();

        assertThat(failure.get()).isNull();
        // Final state is the last generation, visible consistently.
        Map<String, String> finalSnapshot = store.getAll(keys);
        assertThat(finalSnapshot).hasSize(keyCount);
        assertThat(finalSnapshot.values()).containsOnly("gen-" + generations);
        // Readers actually observed several generations, so the invariant was exercised.
        assertThat(observedGenerations.size()).isGreaterThan(1);
        store.close();
    }
}
