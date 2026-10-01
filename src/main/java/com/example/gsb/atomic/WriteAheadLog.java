package com.example.gsb.atomic;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Append-only write-ahead log.
 *
 * <p>A batch occupies two kinds of records: a {@code BEGIN} record carrying the
 * batch id and every key/value pair, followed by a {@code COMMIT} record that
 * only carries the batch id. A batch is considered durable exactly when its
 * {@code COMMIT} record has been forced to disk. A {@code BEGIN} without a
 * matching {@code COMMIT} (process crashed in between, or a failure that rolled
 * the batch back) is discarded during recovery, so an unfinished batch can
 * never become visible.
 *
 * <p>Each record is length-encoded and self-contained, which lets recovery
 * safely ignore a truncated tail left by a crash in the middle of a write.
 */
public final class WriteAheadLog implements AutoCloseable {

    /** A batch whose COMMIT marker was found in the log. */
    public record CommittedBatch(String batchId, Map<String, String> entries) {
        public CommittedBatch {
            Objects.requireNonNull(batchId, "batchId");
            entries = Map.copyOf(entries);
        }
    }

    private static final byte TYPE_BEGIN = 1;
    private static final byte TYPE_COMMIT = 2;

    /** Upper bound on a single encoded string; protects recovery from corrupt data. */
    private static final int MAX_STRING_BYTES = 64 * 1024 * 1024;

    private final Path path;
    private final FileChannel channel;

    private WriteAheadLog(Path path, FileChannel channel) {
        this.path = path;
        this.channel = channel;
    }

    /** Opens (creating if needed) the log at {@code path} for appending. */
    public static WriteAheadLog open(Path path) throws IOException {
        Objects.requireNonNull(path, "path");
        Files.createDirectories(path.getParent());
        FileChannel channel = FileChannel.open(
                path,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.APPEND);
        return new WriteAheadLog(path, channel);
    }

    /** Appends the BEGIN record containing the full batch payload. */
    public synchronized void appendBegin(String batchId, Map<String, String> entries) throws IOException {
        Objects.requireNonNull(batchId, "batchId");
        Objects.requireNonNull(entries, "entries");
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(buffer)) {
            out.writeByte(TYPE_BEGIN);
            writeString(out, batchId);
            out.writeInt(entries.size());
            for (Map.Entry<String, String> entry : entries.entrySet()) {
                writeString(out, Objects.requireNonNull(entry.getKey(), "key"));
                writeString(out, Objects.requireNonNull(entry.getValue(), "value"));
            }
        }
        channel.write(ByteBuffer.wrap(buffer.toByteArray()));
    }

    /**
     * Appends the COMMIT marker. After this method returns together with a
     * successful {@link #sync()} the batch is the durable commit point.
     */
    public synchronized void appendCommit(String batchId) throws IOException {
        Objects.requireNonNull(batchId, "batchId");
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(buffer)) {
            out.writeByte(TYPE_COMMIT);
            writeString(out, batchId);
        }
        channel.write(ByteBuffer.wrap(buffer.toByteArray()));
    }

    /** Forces all buffered log data (and metadata) to stable storage. */
    public synchronized void sync() throws IOException {
        channel.force(true);
    }

    @Override
    public synchronized void close() throws IOException {
        channel.close();
    }

    /**
     * Replays the log file at {@code path} and returns the committed batches
     * in commit order. Any trailing records that are truncated or otherwise
     * malformed (a crash mid-write) are ignored; batches missing their COMMIT
     * marker are treated as aborted and are not returned.
     */
    public static List<CommittedBatch> readCommitted(Path path) throws IOException {
        List<CommittedBatch> committed = new ArrayList<>();
        if (!Files.exists(path)) {
            return committed;
        }
        Map<String, Map<String, String>> pending = new LinkedHashMap<>();
        try (DataInputStream in = new DataInputStream(Files.newInputStream(path))) {
            while (true) {
                byte type;
                try {
                    type = in.readByte();
                } catch (EOFException cleanEnd) {
                    break;
                }
                if (type == TYPE_BEGIN) {
                    String batchId = readString(in);
                    int entryCount = in.readInt();
                    if (entryCount < 0) {
                        break; // corrupt tail
                    }
                    Map<String, String> entries = new LinkedHashMap<>();
                    for (int i = 0; i < entryCount; i++) {
                        entries.put(readString(in), readString(in));
                    }
                    pending.put(batchId, entries);
                } else if (type == TYPE_COMMIT) {
                    String batchId = readString(in);
                    Map<String, String> entries = pending.remove(batchId);
                    if (entries != null) {
                        committed.add(new CommittedBatch(batchId, entries));
                    }
                } else {
                    break; // unknown record type: corrupt tail, stop here
                }
            }
        } catch (EOFException truncatedTail) {
            // A crash while appending a record can leave a partial tail: drop it.
        }
        return committed;
    }

    private static void writeString(DataOutputStream out, String value) throws IOException {
        byte[] bytes = value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    private static String readString(DataInputStream in) throws IOException {
        int length = in.readInt();
        if (length < 0 || length > MAX_STRING_BYTES) {
            throw new EOFException("invalid string length in log record");
        }
        byte[] bytes = in.readNBytes(length);
        if (bytes.length != length) {
            throw new EOFException("truncated log record");
        }
        return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
    }
}
