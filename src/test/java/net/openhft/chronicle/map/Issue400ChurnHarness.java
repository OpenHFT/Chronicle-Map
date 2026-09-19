/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import net.openhft.chronicle.core.Jvm;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Executable reproduction harness for Chronicle-Map#400.
 *
 * <p>The fixed parameters preserve the reported configuration and workload: 61,500 entries, fixed
 * 500-byte keys, fixed 5,120-byte values, removal of the first 7,000 entries encountered by
 * {@link ChronicleMap#forEachEntry}, and repeated refill. Segment count is an explicit argument
 * because the reporter left it CPU-dependent. Random data is deterministic.
 */
public final class Issue400ChurnHarness {

    private static final int ENTRIES = 61_500;
    private static final int KEY_SIZE = 500;
    private static final int VALUE_SIZE = 5_120;
    private static final int REMOVE_COUNT = 7_000;
    private static final long DATA_SEED = 0x400_500_5120_61500L;

    private Issue400ChurnHarness() {
    }

    /**
     * @param args {@code <version-label> <allow-segment-tiering> <actual-segments|auto> <rounds>
     *             <output.csv>}
     */
    public static void main(String[] args) throws FileNotFoundException {
        if (args.length != 5) {
            throw new IllegalArgumentException(
                    "Expected: <version-label> <allow-segment-tiering> <actual-segments|auto> " +
                            "<rounds> <output.csv>");
        }

        final String version = args[0];
        final boolean allowSegmentTiering = parseBoolean(args[1]);
        final boolean automaticSegments = args[2].equalsIgnoreCase("auto");
        final int requestedSegments = automaticSegments ? -1 : Integer.parseInt(args[2]);
        final int rounds = Integer.parseInt(args[3]);
        if (!automaticSegments && requestedSegments <= 0)
            throw new IllegalArgumentException("actual-segments must be positive or 'auto'");
        if (rounds <= 0)
            throw new IllegalArgumentException("rounds must be positive");
        final File output = new File(args[4]);
        final DeterministicByteStream data = new DeterministicByteStream(DATA_SEED);
        final ChronicleMapBuilder<ByteBuffer, ByteBuffer> builder = ChronicleMapBuilder
                .of(ByteBuffer.class, ByteBuffer.class)
                .name("issue-400")
                .entries(ENTRIES)
                .averageKey(ByteBuffer.wrap(new byte[KEY_SIZE]))
                .averageValue(ByteBuffer.wrap(new byte[VALUE_SIZE]))
                .allowSegmentTiering(allowSegmentTiering);
        if (!automaticSegments)
            builder.actualSegments(requestedSegments);

        try (PrintWriter csv = new PrintWriter(output);
             ChronicleMap<ByteBuffer, ByteBuffer> map = builder.create()) {
            writeHeader(csv);
            run(version, allowSegmentTiering, map.segments(), rounds, data, map, csv);
        }
    }

    private static void run(
            String version,
            boolean allowSegmentTiering,
            int actualSegments,
            int rounds,
            DeterministicByteStream data,
            ChronicleMap<ByteBuffer, ByteBuffer> map,
            PrintWriter csv) {
        int failureCount = 0;
        long totalSuccessfulPuts = 0;

        snapshot(csv, version, allowSegmentTiering, actualSegments, 0, "created", map,
                0, 0, 0, failureCount, -1, "");

        for (int round = 1; round <= rounds && failureCount < 10; round++) {
            int attemptedPuts = 0;
            int successfulPuts = 0;
            int failedSegment = -1;
            String failure = "";

            while (map.size() < ENTRIES) {
                final ByteBuffer key = data.nextBuffer(KEY_SIZE);
                final ByteBuffer value = data.nextBuffer(VALUE_SIZE);
                attemptedPuts++;
                try {
                    map.put(key, value);
                    successfulPuts++;
                    totalSuccessfulPuts++;
                } catch (IllegalStateException thrown) {
                    failureCount++;
                    failedSegment = segmentFor(map, key);
                    failure = thrown.getClass().getName() + ": " + thrown.getMessage();
                    break;
                }
            }

            snapshot(csv, version, allowSegmentTiering, actualSegments, round, "after-refill", map,
                    attemptedPuts, successfulPuts, 0, failureCount, failedSegment, failure);

            final AtomicInteger removed = new AtomicInteger();
            map.forEachEntry(entry -> {
                if (removed.get() < REMOVE_COUNT) {
                    entry.context().remove(entry);
                    removed.incrementAndGet();
                }
            });

            snapshot(csv, version, allowSegmentTiering, actualSegments, round, "after-remove", map,
                    attemptedPuts, successfulPuts, removed.get(), failureCount, failedSegment,
                    failure);
            System.out.println("version=" + version + " tiering=" + allowSegmentTiering +
                    " round=" + round + " size=" + map.size() + " successfulPuts=" +
                    totalSuccessfulPuts + " failures=" + failureCount);
        }
    }

    private static int segmentFor(
            ChronicleMap<ByteBuffer, ByteBuffer> map, ByteBuffer key) {
        try (ExternalMapQueryContext<ByteBuffer, ByteBuffer, ?> context =
                     map.queryContext(key)) {
            return context.segmentIndex();
        }
    }

    private static void writeHeader(PrintWriter csv) {
        csv.println("record_type,version,allow_segment_tiering,actual_segments,data_seed,round,phase," +
                "segment,map_size,attempted_puts,successful_puts,removed,failure_count," +
                "failed_segment,segment_entries,used_bytes,capacity_bytes,free_chunks," +
                "chunk_size_bytes,tiers,off_heap_bytes,remaining_auto_resizes,failure");
    }

    private static void snapshot(
            PrintWriter csv,
            String version,
            boolean allowSegmentTiering,
            int actualSegments,
            int round,
            String phase,
            ChronicleMap<ByteBuffer, ByteBuffer> map,
            int attemptedPuts,
            int successfulPuts,
            int removed,
            int failureCount,
            int failedSegment,
            String failure) {
        final long chunkSize = ((Number) Jvm.getValue(map, "chunkSize")).longValue();
        final ChronicleMap.SegmentStats[] stats = map.segmentStats();
        for (int segment = 0; segment < stats.length; segment++) {
            final ChronicleMap.SegmentStats stat = stats[segment];
            final long freeChunks = (stat.sizeInBytes() - stat.usedBytes()) / chunkSize;
            final long segmentEntries;
            try (MapSegmentContext<ByteBuffer, ByteBuffer, ?> context =
                         map.segmentContext(segment)) {
                segmentEntries = context.size();
            }
            csv.println("segment," + csv(version) + ',' + allowSegmentTiering + ',' +
                    actualSegments + ',' + DATA_SEED + ',' + round + ',' + phase + ',' + segment + ',' +
                    map.size() + ',' + attemptedPuts + ',' + successfulPuts + ',' + removed + ',' +
                    failureCount + ',' + failedSegment + ',' + segmentEntries + ',' +
                    stat.usedBytes() + ',' + stat.sizeInBytes() + ',' + freeChunks + ',' +
                    chunkSize + ',' + stat.tiers() + ',' + map.offHeapMemoryUsed() + ',' +
                    map.remainingAutoResizes() + ',' + csv(failure));
        }
        csv.flush();
        if (csv.checkError())
            throw new IllegalStateException("Failed to write harness CSV output");
    }

    private static boolean parseBoolean(String value) {
        if (value.equalsIgnoreCase("true"))
            return true;
        if (value.equalsIgnoreCase("false"))
            return false;
        throw new IllegalArgumentException("allow-segment-tiering must be 'true' or 'false'");
    }

    private static String csv(String value) {
        if (value == null || value.isEmpty())
            return "";
        return '"' + value.replace("\"", "\"\"").replace('\n', ' ') + '"';
    }

    /** SHA-256 counter mode provides deterministic, well-distributed bytes across JDK versions. */
    private static final class DeterministicByteStream {
        private final MessageDigest digest;
        private final long seed;
        private long counter;

        private DeterministicByteStream(long seed) {
            this.seed = seed;
            try {
                digest = MessageDigest.getInstance("SHA-256");
            } catch (NoSuchAlgorithmException e) {
                throw new AssertionError(e);
            }
        }

        private ByteBuffer nextBuffer(int size) {
            final byte[] bytes = new byte[size];
            int offset = 0;
            while (offset < bytes.length) {
                digest.update(longBytes(seed));
                digest.update(longBytes(counter++));
                final byte[] block = digest.digest();
                final int length = Math.min(block.length, bytes.length - offset);
                System.arraycopy(block, 0, bytes, offset, length);
                offset += length;
            }
            return ByteBuffer.wrap(bytes);
        }

        private static byte[] longBytes(long value) {
            return new byte[]{
                    (byte) (value >>> 56), (byte) (value >>> 48),
                    (byte) (value >>> 40), (byte) (value >>> 32),
                    (byte) (value >>> 24), (byte) (value >>> 16),
                    (byte) (value >>> 8), (byte) value
            };
        }
    }
}
