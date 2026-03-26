/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import net.openhft.chronicle.hash.impl.util.BuildVersion;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Rob Austin.
 */
class BuildVersionTest {

    @Test
    void test() throws IOException, InterruptedException {
        // checks that we always get a version
        assertNotNull(BuildVersion.version());
    }

    /**
     * check that the map records the version
     *
     */
    @Test
    void testVersion() throws IOException, InterruptedException {

        try (ChronicleMap<Integer, Double> expected = ChronicleMap.of(Integer.class, Double.class)
                .entries(1).create()) {
            expected.put(1, 1.0);

            String version = ((VanillaChronicleMap<?, ?, ?>) expected).persistedDataVersion();
            assertNotNull(version, BuildVersion.version());

        }
    }
}
