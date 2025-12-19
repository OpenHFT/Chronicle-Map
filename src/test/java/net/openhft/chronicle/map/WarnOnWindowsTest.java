/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class WarnOnWindowsTest {

    @Test
    // TODO this does not emit a proper warning, there are TODOs in ChronicleMapBuilder/VanillaHash
    @Disabled("This test results to OOM/jvm crash, run manually to verify warning output")
    public void warnOnWindowsTest() {
        ChronicleMapBuilder<Long, Long> builder = ChronicleMapBuilder
                .of(Long.class, Long.class)
                .entries(1_000_000_000);
        assertNotNull(builder, "builder created");
        builder.create();
    }
}
