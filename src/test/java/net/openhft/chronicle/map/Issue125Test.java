/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.util.Time;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

/**
 * Created by Peter Lawrey on 04/06/17.
 */
public class Issue125Test {
    @Test
    public void test() throws IOException {
        final File cacheRoot = new File(OS.getTarget() + "/test-" + Time.uniqueId() + ".cm3");
        ChronicleMapBuilder<byte[], byte[]> shaToNodeBuilder =
                ChronicleMapBuilder.of(byte[].class, byte[].class)
                        // .name("bytes-to-bytes")
                        .entries(1000000).
                        averageKeySize(20).
                        averageValueSize(30);

        byte[] key = "1".getBytes();
        byte[] value = "2".getBytes();
        try (ChronicleMap<byte[], byte[]> shaToNode = shaToNodeBuilder.createPersistedTo(cacheRoot)) {
            shaToNode.put(key, value);
            try (ChronicleMap<byte[], byte[]> shaToNode2 = shaToNodeBuilder.createPersistedTo(cacheRoot)) {
                assertArrayEquals(value, shaToNode2.get(key), "second map instance should see existing entry");
            }
        } finally {
            cacheRoot.delete();
        }
    }
}
