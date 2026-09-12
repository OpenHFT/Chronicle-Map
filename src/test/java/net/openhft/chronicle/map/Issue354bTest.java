/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import net.openhft.chronicle.core.values.LongValue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class Issue354bTest {

    @TempDir
    Path testFolder;

    @Test
    void build_toFile() throws IOException {
        String baseDirectory = testFolder.toString();
        File file = new File(baseDirectory, "chronicle.dat");
        try (ChronicleMap<LongValue, LongValue> map = ChronicleMapBuilder.of(LongValue.class, LongValue.class)
                .name("test")
                .entries(5)
                .createPersistedTo(file)) {
            assertTrue(map.file().isFile());
        }
    }
}
