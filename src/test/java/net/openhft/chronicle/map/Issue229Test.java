/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.hash.ChronicleHashRecoveryFailedException;
import org.junit.jupiter.api.*;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.*;

class Issue229Test {

    private File mapFile;

    @BeforeEach
    void setup() {
        mapFile = new File("test_map");
    }

    @AfterEach
    void cleanup() {
        mapFile.delete();
    }

    @Test
    void assureExclusiveAccess() throws IOException {
        assumeFalse(OS.isWindows());

        try (ChronicleMap<Long, Long> readMap = ChronicleMap
                .of(Long.class, Long.class)
                .entries(10)
                .createPersistedTo(mapFile)) {
            assertNotNull(readMap);

            // It shall not be possible to recover since the
            // file is open by the readMap
            assertThrows(ChronicleHashRecoveryFailedException.class, () -> recoverPersistedMap(mapFile));
        }
    }

    private static void recoverPersistedMap(File mapFile) throws IOException {
        try (ChronicleMap<Long, Long> recoverMap = ChronicleMap
                .of(Long.class, Long.class)
                .entries(10)
                .recoverPersistedTo(mapFile, true)) {
            assertNotNull(recoverMap);
        }
    }
}
