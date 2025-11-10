//
// Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
//

package net.openhft.chronicle.map;

import org.junit.Test;

public class HColl423Test {

    @Test
    public void hColl423Test() {
        try {
            ChronicleMap<String, Integer> map = ChronicleMap
                    .of(String.class, Integer.class)
                    .averageKeySize(128)
                    .averageValueSize(100)
                    .entries(2_000_000L)
                    .maxBloatFactor(1_000.0)
                    .create();
            throw new AssertionError("should throw IllegalStateException");
        } catch (IllegalStateException expected) {
            // do nothing
        }
    }
}
