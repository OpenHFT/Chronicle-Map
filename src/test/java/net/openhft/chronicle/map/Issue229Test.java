//
// Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
//

package net.openhft.chronicle.map;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.hash.ChronicleHashRecoveryFailedException;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertNotNull;

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

    @Test(expected = ChronicleHashRecoveryFailedException.class)
    public void assureExclusiveAccess() throws IOException {
        Assume.assumeFalse(OS.isWindows());

        try (ChronicleMap<Long, Long> readMap = ChronicleMap
                .of(Long.class, Long.class)
                .entries(10)
                .createPersistedTo(mapFile)) {
            assertNotNull(readMap);

// It shall not be possible to recover since the
            // file is open by the readMap
            try (ChronicleMap<Long, Long> recoverMap = ChronicleMap
                    .of(Long.class, Long.class)
                    .entries(10)
                    .recoverPersistedTo(mapFile, true)) {
                assertNotNull(recoverMap);
            }
        }
    }
}
