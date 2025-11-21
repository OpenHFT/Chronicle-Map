/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import net.openhft.chronicle.hash.impl.util.BuildVersion;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * @author Rob Austin.
 */
public class BuildVersionTest {

    @Test
    public void test() {
        // checks that we always get a version
        Assert.assertNotNull(BuildVersion.version());
    }

    /**
     * check that the map records the version
     *
     */
    @Test
    public void testVersion() {

        try (ChronicleMap<Integer, Double> expected = ChronicleMap.of(Integer.class, Double.class)
                .entries(1).create()) {
            expected.put(1, 1.0);

            String version = ((VanillaChronicleMap<?, ?, ?>) expected).persistedDataVersion();
            Assert.assertNotNull(BuildVersion.version(), version);

        }
    }
}
