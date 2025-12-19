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

import static org.junit.jupiter.api.Assertions.assertTrue;

public class Issue354bTest {

    @TempDir
    Path testFolder;

    @Test
    public void build_toFile() throws IOException {
        File file = testFolder.resolve("chronicle.dat").toFile();
        ChronicleMap<LongValue, LongValue> map = ChronicleMapBuilder.of(LongValue.class, LongValue.class)
                .name("test")
                .entries(5)
                .createPersistedTo(file);

        assertTrue(file.isFile(), "file.isFile()");
    }
}
