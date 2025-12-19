/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.set;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class Issue3Test {

    @Disabled("https://teamcity.chronicle.software/viewLog.html?buildId=639348&tab=buildResultsDiv&buildTypeId=OpenHFT_BuildAll_BuildJava8compileJava8")
    @Test
    public void test() {
        try (ChronicleSet<Long> set = ChronicleSetBuilder.of(Long.class)
                .actualSegments(1)
                .entriesPerSegment(1000)
                .create()) {
            Random r = new Random();
            for (int i = 0; i < 10; i++) {
                for (int j = 0; j < 300; j++) {
                    set.add(r.nextLong());
                }
                set.clear();
                assertEquals(0, set.size(), "set should be empty after clear");
            }
        }
    }
}
