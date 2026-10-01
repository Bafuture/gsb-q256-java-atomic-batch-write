package com.example.gsb.atomic;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;

/**
 * Length-prefixed, CRC32-protected write-ahead log.
 *
 * <p>Record layout: {@code type(1) idLen(4) id(idLen) [BEGIN: count(4)
 * (kLen(4) key vLen(4) value)*] crc(8)}. Each record is built fully in memory
 * and flushed with {@code force(true)}, so either the whole record or nothing
 * survives a crash; a torn/trailing record is detected by the CRC on recovery
 * and truncated.
 */
final class WriteAheadLog implements Closeable {

    static final byte REC_BEGIN = 1;
    static final byte REC_COMMIT = 2;
    static final byte REC_ABORT = 3;

    private static final int MAGIC = 0x41425357; // "ABSW"
    private static final int VERSION = 1;
    private static final int HEADER_SIZE = 8;
    private static final int MAX_LEN = 1 << 20;

    record RecoveredBatch(String id, Map<String, String> entries) {
    }

    private final FileChannel channel;
    private final List<RecoveredBatch> recoveredBatches;

    private WriteAheadLog(FileChannel channel, List<RecoveredBatch> recoveredBatches) {
        this.channel = channel;
        this.recoveredBatches = recoveredBatches;
    }

    /** Opens the log (creating it if needed), recovers and truncates torn tails. */
    static WriteAheadLog open(Path path) throws IOException {
        if (path.getParent() != null) {
            Files.createDirectories(path.getParent());
        }
        List<RecoveredBatch> recovered = new ArrayList<>();
        int validLength = HEADER_SIZE;

        if (Files.notExists(path) || Files.size(path) == 0) {
            try (FileChannel init = FileChannel.open(path,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
                init.write(ByteBuffer.allocate(HEADER_SIZE)
                        .putInt(MAGIC).putInt(VERSION).flip());
                init.force(true);
            }
        } else {
            byte[] bytes = Files.readAllBytes(path);
            ByteBuffer buf = ByteBuffer.wrap(bytes);
            if (bytes.length < HEADER_SIZE || buf.getInt() != MAGIC || buf.getInt() != VERSION) {
                throw new IOException("Invalid WAL header in " + path);
            }

            // id -> BEGIN entries; state per batch discovered during the scan
            Map<String, Map<String, String>> begun = new LinkedHashMap<>();
            java.util.Set<String> alreadyCommitted = new java.util.HashSet<>();

            while (buf.hasRemaining()) {
                int recordStart = buf.position();
                try {
                    byte type = buf.get();
                    if (type != REC_BEGIN && type != REC_COMMIT && type != REC_ABORT) {
                        throw new IOException("unknown record type " + type);
                    }
                    String id = readBytes(buf, true);
                    Map<String, String> entries = null;
                    if (type == REC_BEGIN) {
                        int count = buf.getInt();
                        if (count < 0 || count > MAX_LEN) {
                            throw new IOException("invalid entry count " + count);
                        }
                        entries = new LinkedHashMap<>();
                        for (int i = 0; i < count; i++) {
                            String key = readBytes(buf, false);
                            String value = readBytes(buf, false);
                            entries.put(key, value);
                        }
                    }
                    long expectedCrc = buf.getLong();
                    CRC32 crc = new CRC32();
                    crc.update(bytes, recordStart, buf.position() - recordStart - 8);
                    if (crc.getValue() != expectedCrc) {
                        break; // torn tail
                    }
                    switch (type) {
                        case REC_BEGIN -> {
                            if (!alreadyCommitted.contains(id)) {
                                begun.put(id, entries);
                            }
                        }
                        case REC_COMMIT -> {
                            if (!alreadyCommitted.contains(id)) {
                                Map<String, String> committed = begun.get(id);
                                if (committed != null) {
                                    recovered.add(new RecoveredBatch(id, committed));
                                    begun.remove(id);
                                    alreadyCommitted.add(id);
                                }
                            }
                            begun.remove(id);
                        }
                        case REC_ABORT -> begun.remove(id);
                        default -> throw new AssertionError();
                    }
                    validLength = buf.position();
                } catch (BufferUnderflowException | IOException | IllegalArgumentException e) {
                    break; // incomplete or corrupt tail: discard everything from here
                }
            }

            if (validLength < bytes.length) {
                try (FileChannel ch = FileChannel.open(path, StandardOpenOption.WRITE)) {
                    ch.truncate(validLength);
                    ch.force(true);
                }
            }
        }

        FileChannel channel = FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
        return new WriteAheadLog(channel, recovered);
    }

    private static String readBytes(ByteBuffer buf, boolean id) {
        int len = buf.getInt();
        if (len <= 0 || len > MAX_LEN) {
            throw new IllegalArgumentException("invalid field length: " + len);
        }
        byte[] data = new byte[len];
        buf.get(data);
        return new String(data, StandardCharsets.UTF_8);
    }

    List<RecoveredBatch> recoveredBatches() {
        return recoveredBatches;
    }

    synchronized void logBegin(String id, Map<String, String> entries) throws IOException {
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(body);
        out.writeByte(REC_BEGIN);
        writeId(out, id);
        out.writeInt(entries.size());
        for (Map.Entry<String, String> e : entries.entrySet()) {
            writeField(out, e.getKey());
            writeField(out, e.getValue());
        }
        out.flush();
        writeRecord(body.toByteArray());
    }

    synchronized void logCommit(String id) throws IOException {
        writeMarker(REC_COMMIT, id);
    }

    synchronized void logAbort(String id) {
        try {
            writeMarker(REC_ABORT, id);
        } catch (IOException ignored) {
            // Best effort: an abort marker only avoids a redundant replay of a
            // batch that this instance is about to discard anyway.
        }
    }

    private void writeMarker(byte type, String id) throws IOException {
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(body);
        out.writeByte(type);
        writeId(out, id);
        out.flush();
        writeRecord(body.toByteArray());
    }

    private static void writeId(DataOutputStream out, String id) throws IOException {
        byte[] bytes = id.getBytes(StandardCharsets.UTF_8);
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    private static void writeField(DataOutputStream out, String value) throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    private void writeRecord(byte[] body) throws IOException {
        CRC32 crc = new CRC32();
        crc.update(body);
        ByteBuffer record = ByteBuffer.allocate(body.length + 8);
        record.put(body).putLong(crc.getValue()).flip();
        while (record.hasRemaining()) {
            channel.write(record);
        }
        channel.force(true);
    }

    @Override
    public synchronized void close() throws IOException {
        channel.close();
    }
}
