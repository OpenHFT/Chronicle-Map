/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.set;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ChronicleSetBuilderTest {

    @Test
    @Disabled("see https://teamcity.chronicle.software/viewLog.html?buildId=639348&tab=buildResultsDiv&buildTypeId=OpenHFT_BuildAll_BuildJava8compileJava8")
    void test() {

        try (ChronicleSet<Integer> integers = ChronicleSet.of(Integer.class).entries(10).create()) {
            for (int i = 0; i < 10; i++) {
                integers.add(i);
            }

            assertTrue(integers.contains(5));
            assertEquals(10, integers.size());
        }
    }
}
