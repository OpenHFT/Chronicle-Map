/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.hash.ChronicleHashRecoveryFailedException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class Issue229Test {

    private File mapFile;

    @BeforeEach
    public void setup() {
        mapFile = new File("test_map");
    }

    @AfterEach
    public void cleanup() {
        mapFile.delete();
    }

    @Test
    public void assureExclusiveAccess() throws IOException {
        Assumptions.assumeFalse(OS.isWindows());

        try (ChronicleMap<Long, Long> readMap = ChronicleMap
                .of(Long.class, Long.class)
                .entries(10)
                .createPersistedTo(mapFile)) {
            assertNotNull(readMap, "map should be successfully created and persisted to file");

            // It shall not be possible to recover since the
            // file is open by the readMap
            assertThrows(ChronicleHashRecoveryFailedException.class, () -> {
                try (ChronicleMap<Long, Long> recoverMap = ChronicleMap
                        .of(Long.class, Long.class)
                        .entries(10)
                        .recoverPersistedTo(mapFile, true)) {
                    assertNotNull(recoverMap, "recovery should not succeed when file is already open");
                }
            });
        }
    }
}
