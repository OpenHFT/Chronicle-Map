/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import com.google.common.io.Files;
import net.openhft.chronicle.set.Builder;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.URL;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Disabled("As per https://github.com/OpenHFT/Chronicle-Map/issues/324, there is no compatibility anymore")
public class ChronicleMap3_12IntegerKeyCompatibilityTest {

    @Test
    public void testWithChecksums() throws Exception {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        URL fileUrl = cl.getResource("chronicle-map-3-12-with-checksums.dat");
        File file = new File(fileUrl.toURI());
        File persistenceFile = Builder.getPersistenceFile();
        Files.copy(file, persistenceFile);
        try (ChronicleMap<Integer, String> map = ChronicleMap.of(Integer.class, String.class)
                .averageValue("1")
                .entries(1)
                .recoverPersistedTo(persistenceFile, false)) {
            assertEquals(2, map.size(), "map.size()");
            assertEquals("1", map.get(1), "map.get(1)");
            assertEquals("-1", map.get(-1), "map.get(-1)");
        }
    }

    @Test
    public void testNoChecksums() throws Exception {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        URL fileUrl = cl.getResource("chronicle-map-3-12-no-checksums.dat");
        File file = new File(fileUrl.toURI());
        File persistenceFile = Builder.getPersistenceFile();
        Files.copy(file, persistenceFile);
        try (ChronicleMap<Integer, String> map = ChronicleMap.of(Integer.class, String.class)
                .averageValue("1")
                .entries(1)
                .checksumEntries(false)
                .recoverPersistedTo(persistenceFile, false)) {
            assertEquals(2, map.size(), "map.size()");
            assertEquals("1", map.get(1), "map.get(1)");
            assertEquals("-1", map.get(-1), "map.get(-1)");
        }
    }
}
