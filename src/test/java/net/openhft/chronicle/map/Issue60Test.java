/*
 * Copyright 2013-2026 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import org.junit.Assert;
import org.junit.Test;

public class Issue60Test {

    @Test
    public void issue60Test() {
        int entries = 200;
        int maxBloatFactor = 10;
        int loop = 1000;

        try (ChronicleMap<String, String> map = ChronicleMap
                .of(String.class, String.class)
                .entries(entries)
                .averageKeySize(12)
                .averageValueSize(12)
                .maxBloatFactor(maxBloatFactor)
                .create()) {

            //System.out.println("begin test " + map.size());
            for (int i = 0; i < loop; i++) {
                map.put("key" + i, "value" + i);
            }
            //System.out.println("map size " + map.size());

            int failedGet = 0;
            for (int i = 0; i < loop; i++) {
                String value = map.get("key" + i);
                if (value == null) {
                    failedGet++;
                }
            }
            Assert.assertEquals(0, failedGet);
            //System.out.println("failedGet " + failedGet);
            //System.out.println("map " + map);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void maxBloatFactorShouldBeLessThan1000() {
	ChronicleMapBuilder.of(String.class, String.class).maxBloatFactor(1000.01);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIllegalMaxBloatFactor() {
	ChronicleMapBuilder.of(Object.class, Object.class).maxBloatFactor(0.0);
    }
}
