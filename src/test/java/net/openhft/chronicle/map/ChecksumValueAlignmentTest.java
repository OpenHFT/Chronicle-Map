/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.hash.ChecksumEntry;
import net.openhft.chronicle.hash.ChronicleHashBuilderPrivateAPI;
import net.openhft.chronicle.values.Values;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The entry checksum is stored after the value, so it must not be counted as part of the bytes
 * preceding the value when the value needs alignment padding. Otherwise, for some key lengths, an
 * entry is allocated one chunk too few and its checksum overwrites the start of the next entry.
 */
class ChecksumValueAlignmentTest {

    private static final int ENTRIES = 100;
    private static final int MIN_KEY_LENGTH = 8;
    private static final int MAX_KEY_LENGTH = 72;
    private static final int SMALL_CHUNK_SIZE = 0;
    // beyond this the entries outgrow a map sized for short keys
    private static final int SMALL_CHUNK_MAX_KEY_LENGTH = 24;

    @TempDir
    File tempDir;

    enum Rewrite {
        PUT {
            @Override
            void apply(ChronicleMap<CharSequence, LongValue> map, String key, LongValue value) {
                map.put(key, value);
            }
        },
        REPLACE {
            @Override
            void apply(ChronicleMap<CharSequence, LongValue> map, String key, LongValue value) {
                assertNotNull(map.replace(key, value));
            }
        },
        COMPUTE {
            @Override
            void apply(ChronicleMap<CharSequence, LongValue> map, String key, LongValue value) {
                map.compute(key, (k, v) -> value);
            }
        },
        REMOVE_THEN_PUT {
            @Override
            void apply(ChronicleMap<CharSequence, LongValue> map, String key, LongValue value) {
                map.remove(key);
                map.put(key, value);
            }
        };

        abstract void apply(ChronicleMap<CharSequence, LongValue> map, String key, LongValue value);
    }

    static Stream<Arguments> data() {
        List<Arguments> args = new ArrayList<>();
        // the default chunk size depends on the environment (e.g. chronicle.map.sparseFile), so pin it;
        // SMALL_CHUNK_SIZE requests the default for short keys, which is smaller than the value alignment
        // (replicated entries are too large to ever get such a default chunk size)
        for (int chunkSize : new int[]{SMALL_CHUNK_SIZE, 8, 16, 32}) {
            for (boolean replicated : new boolean[]{false, true}) {
                if (replicated && chunkSize == SMALL_CHUNK_SIZE)
                    continue;
                for (boolean persisted : new boolean[]{true, false}) {
                    for (Rewrite rewrite : Rewrite.values()) {
                        args.add(Arguments.of(chunkSize, replicated, persisted, rewrite));
                    }
                }
            }
        }
        return args.stream();
    }

    @ParameterizedTest(name = "chunkSize={0}, replicated={1}, persisted={2}, rewrite={3}")
    @MethodSource("data")
    void rewritingLongValueEntriesKeepsNeighboursAndChecksumsIntact(
            int chunkSize, boolean replicated, boolean persisted, Rewrite rewrite) throws IOException {
        boolean smallChunks = chunkSize == SMALL_CHUNK_SIZE;
        int maxKeyLength = smallChunks ? SMALL_CHUNK_MAX_KEY_LENGTH : MAX_KEY_LENGTH;
        for (int keyLength = MIN_KEY_LENGTH; keyLength <= maxKeyLength; keyLength++) {
            ChronicleMapBuilder<CharSequence, LongValue> builder = ChronicleMapBuilder
                    .of(CharSequence.class, LongValue.class)
                    .entries(ENTRIES)
                    .checksumEntries(true);
            if (smallChunks) {
                // sparseFile enlarges the default chunk size
                builder.averageKey(key(MIN_KEY_LENGTH, 0)).sparseFile(false);
            } else {
                builder.averageKey("clorderidfollowedbyclientcode_clientcode")
                        .actualChunkSize(chunkSize);
            }
            if (replicated) {
                ChronicleHashBuilderPrivateAPI<?, ?> privateAPI =
                        Objects.requireNonNull(Jvm.getValue(builder, "privateAPI"));
                privateAPI.replication((byte) 1);
            }
            File file = new File(tempDir, "map-" + keyLength + ".cm3");
            try (ChronicleMap<CharSequence, LongValue> map =
                         persisted ? builder.createPersistedTo(file) : builder.create()) {
                if (smallChunks) {
                    VanillaChronicleMap<?, ?, ?> vcm = (VanillaChronicleMap<?, ?, ?>) map;
                    assertTrue(vcm.chunkSize < vcm.alignment, "chunkSize=" + vcm.chunkSize);
                    assertTrue(vcm.couldNotDetermineAlignmentBeforeAllocation);
                }
                LongValue value = Values.newHeapInstance(LongValue.class);

                for (int i = 0; i < ENTRIES; i++) {
                    value.setValue(i);
                    map.put(key(keyLength, i), value);
                }
                assertAllChecksumsValid(map, keyLength, "after insert");

                for (int i = 0; i < ENTRIES; i++) {
                    value.setValue(i + 1L);
                    rewrite.apply(map, key(keyLength, i), value);
                }
                assertAllChecksumsValid(map, keyLength, "after " + rewrite);

                Set<String> seen = new HashSet<>();
                for (Map.Entry<CharSequence, LongValue> e : map.entrySet()) {
                    String key = e.getKey().toString();
                    assertTrue(seen.add(key), "keyLength=" + keyLength + " duplicate key " + key);
                    assertEquals(index(key) + 1L, e.getValue().getValue(), "keyLength=" + keyLength + " key=" + key);
                }
                assertEquals(ENTRIES, seen.size(), "keyLength=" + keyLength);
                assertEquals(ENTRIES, map.size(), "keyLength=" + keyLength);
            }
        }
    }

    private static void assertAllChecksumsValid(ChronicleMap<CharSequence, LongValue> map, int keyLength, String when) {
        for (int i = 0; i < ENTRIES; i++) {
            String key = key(keyLength, i);
            try (ExternalMapQueryContext<CharSequence, LongValue, ?> c = map.queryContext(key)) {
                c.updateLock().lock();
                MapEntry<CharSequence, LongValue> entry = c.entry();
                assertNotNull(entry, "keyLength=" + keyLength + " " + when + " missing " + key);
                assertTrue(((ChecksumEntry) entry).checkSum(),
                        "keyLength=" + keyLength + " " + when + " bad checksum for " + key);
            }
        }
    }

    private static String key(int length, int i) {
        String digits = String.valueOf(1000 + i);
        return new String(new char[length - digits.length()]).replace('\0', 'K') + digits;
    }

    private static int index(String key) {
        return Integer.parseInt(key.substring(key.length() - 4)) - 1000;
    }
}
