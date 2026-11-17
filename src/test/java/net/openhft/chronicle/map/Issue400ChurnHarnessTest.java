/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class Issue400ChurnHarnessTest {
    @TempDir
    Path directory;

    @Test
    void recordsBoundedChurnForBothTieringModes() throws Exception {
        for (boolean tiering : new boolean[]{false, true}) {
            Path output = directory.resolve("churn-" + tiering + ".csv");
            Issue400ChurnHarness.main(new String[]{"test", Boolean.toString(tiering), "16", "2", output.toString()});
            List<String> rows = Files.readAllLines(output);
            assertTrue(rows.get(0).startsWith("record_type,version,allow_segment_tiering,"));
            assertTrue(rows.size() > 16, "Each segment needs an initial and subsequent observation");
            assertTrue(rows.stream().anyMatch(line -> line.contains(",created,")));
            assertTrue(rows.stream().anyMatch(line -> line.contains(",after-remove,")));
            assertTrue(rows.stream().allMatch(line -> !line.contains("NaN")));
        }
    }

    @Test
    void rejectsUnboundedOrInvalidInputsBeforeCreatingOutput() {
        String output = directory.resolve("invalid.csv").toString();
        assertThrows(IllegalArgumentException.class, () -> Issue400ChurnHarness.main(new String[]{"test", "maybe", "16", "2", output}));
        assertThrows(IllegalArgumentException.class, () -> Issue400ChurnHarness.main(new String[]{"test", "false", "0", "2", output}));
        assertThrows(IllegalArgumentException.class, () -> Issue400ChurnHarness.main(new String[]{"test", "false", "16", "0", output}));
        assertFalse(Files.exists(directory.resolve("invalid.csv")));
    }
}
