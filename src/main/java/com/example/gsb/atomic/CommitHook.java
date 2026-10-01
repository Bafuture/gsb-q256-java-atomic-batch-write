package com.example.gsb.atomic;

/**
 * Test/observability hook invoked at the stages of a commit. Implementations
 * may throw {@link SimulatedCrashException} to simulate a crash, or any other
 * runtime exception to simulate an ordinary failure (which triggers rollback).
 */
public interface CommitHook {

    CommitHook NONE = new CommitHook() {
    };

    /** Called after the BEGIN record has been flushed to the WAL. */
    default void afterBeginWritten(Batch batch) {
    }

    /** Called before the entry at {@code index} is applied to the store. */
    default void beforeApplyEntry(Batch batch, String key, int index) {
    }

    /** Called after all entries have been applied, before the COMMIT record. */
    default void afterApplyCompleted(Batch batch) {
    }

    /** Called after the COMMIT record has been flushed to the WAL. */
    default void afterCommitWritten(Batch batch) {
    }
}
