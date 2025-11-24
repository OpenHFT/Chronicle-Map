/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import org.junit.Ignore;
import org.junit.Test;

/**
 * Placeholder test intended to verify warning behaviour on Windows; currently
 * ignored due to OOM/crash risk when allocating huge maps.
 */
public class WarnOnWindowsTest {

    @Test
    // TODO this does not emit a proper warning, there are TODOs in ChronicleMapBuilder/VanillaHash
    @Ignore("This test results to OOM/jvm crash, run manually to verify warning output")
    public void warnOnWindowsTest() {
        ChronicleMapBuilder.of(Long.class, Long.class)
                .entries(1_000_000_000).create();
    }
}
