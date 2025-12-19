/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Created by peter.lawrey on 28/02/14.
 */
public class HistogramTest {

    @Test
    public void testHistogram() {
        Histogram hist = new Histogram();
        hist.sample(1);
        hist.sample(10);
        hist.sample(100);
        assertEquals(3L, hist.count, "sample count");
        hist.printResults();
    }
}
