/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.hash.ChronicleHashRecoveryFailedException;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

public class Issue229Test {

    private File mapFile;

    @Before
    public void setup() {
        mapFile = new File("test_map");
    }

    @After
    public void cleanup() {
        mapFile.delete();
    }

    @Test
    public void assureExclusiveAccess() throws IOException {
        Assume.assumeFalse(OS.isWindows());

        try (ChronicleMap<Long, Long> readMap = ChronicleMap
                .of(Long.class, Long.class)
                .entries(10)
                .createPersistedTo(mapFile)) {
            assertNotNull(readMap);

            ChronicleMapBuilder<Long, Long> recoveryBuilder = ChronicleMap
                    .of(Long.class, Long.class)
                    .entries(10);
            AtomicReference<ChronicleMap<Long, Long>> unexpectedRecovery = new AtomicReference<>();
            try {
                //! Only recovery may supply the expected failure; opening and cleanup stay outside the assertion.
                assertThrows(ChronicleHashRecoveryFailedException.class,
                        () -> unexpectedRecovery.set(recoveryBuilder.recoverPersistedTo(mapFile, true)));
            } finally {
                Closeable.closeQuietly(unexpectedRecovery.get());
            }
        }
    }
}
