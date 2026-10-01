package com.example.gsb.atomic;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * An atomic unit of key-value writes identified by a unique batch id.
 * Entry iteration order is the insertion order, which makes apply/rollback
 * deterministic and testable.
 */
public record Batch(String id, Map<String, String> entries) {

    public Batch {
        Objects.requireNonNull(id, "batch id must not be null");
        if (id.isBlank()) {
            throw new IllegalArgumentException("batch id must not be blank");
        }
        Objects.requireNonNull(entries, "entries must not be null");
        if (entries.isEmpty()) {
            throw new IllegalArgumentException("batch must contain at least one entry");
        }
        entries.forEach((key, value) -> {
            if (key == null || key.isBlank()) {
                throw new IllegalArgumentException("batch keys must not be null or blank");
            }
            if (value == null) {
                throw new IllegalArgumentException("batch values must not be null (key: " + key + ")");
            }
        });
        entries = Collections.unmodifiableMap(new LinkedHashMap<>(entries));
    }

    public int size() {
        return entries.size();
    }
}
