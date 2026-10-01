package com.example.gsb.atomic;

/**
 * Thrown from a {@link CommitHook} to simulate a hard process crash in the
 * middle of a commit. The store performs no rollback and writes no further
 * WAL records, exactly as if the JVM had died at that point. The instance
 * should be abandoned afterwards; create a new instance on the same
 * directory to exercise crash recovery.
 */
public class SimulatedCrashException extends RuntimeException {

    public SimulatedCrashException(String message) {
        super(message);
    }
}
