/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class PersistedMapHeaderTest {
    @Rule
    public final TemporaryFolder temporaryFolder = TemporaryFolder.builder().assureDeletion().build();

    @Test
    public void createReopenAndRecoverPreserveEntries() throws IOException {
        File file = temporaryFolder.newFile("header.map");
        ChronicleMapBuilder<Long, Long> builder = ChronicleMap.of(Long.class, Long.class).entries(10);

        try (ChronicleMap<Long, Long> created = builder.createPersistedTo(file)) {
            created.put(1L, 42L);
            assertEquals(Long.valueOf(42L), created.get(1L));
        }
        try (ChronicleMap<Long, Long> reopened = builder.createPersistedTo(file)) {
            assertEquals(Long.valueOf(42L), reopened.get(1L));
            reopened.put(2L, 43L);
        }
        try (ChronicleMap<Long, Long> recovered = builder.recoverPersistedTo(file, true)) {
            assertEquals(2, recovered.size());
            assertEquals(Long.valueOf(42L), recovered.get(1L));
            assertEquals(Long.valueOf(43L), recovered.get(2L));
        }
        try (ChronicleMap<Long, Long> reopened = builder.createPersistedTo(file)) {
            assertEquals(2, reopened.size());
            assertEquals(Long.valueOf(42L), reopened.get(1L));
            assertEquals(Long.valueOf(43L), reopened.get(2L));
        }
    }
}
