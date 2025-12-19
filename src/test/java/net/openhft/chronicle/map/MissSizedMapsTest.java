/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

/**
 * Created by catst01 on 24/10/2018.
 */
public class MissSizedMapsTest {

    @Timeout(value = 60000, unit = TimeUnit.MILLISECONDS)
    @Test
    public void testSmallEntries() throws IOException, URISyntaxException {

        try (ChronicleMap<String, String> actual = ChronicleMapBuilder.of(String.class, String.class)
                .averageKey("D-6.0149935894066442E18").averageValue("226|16533|4|1|1|testHarness").entries(150 << 10)
                .createPersistedTo(File.createTempFile("chronicle", "cmap"))) {
            check(actual);
        }
    }

    private void check(final ChronicleMap<String, String> actual) throws IOException, URISyntaxException {
        URI uri = MissSizedMapsTest.class.getResource("/input.txt.gz").toURI();

        Map<String, String> expected = new HashMap<>();

        int maxKey = 0;
        int maxValue = 0;
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new GZIPInputStream(uri.toURL().openStream())))) {
            for (String s; (s = br.readLine()) != null; ) {
                String[] split = s.split("&");
                expected.put(split[0], split[1]);
                actual.put(split[0], split[1]);
                maxKey = Integer.max(maxKey, split[0].length());
                maxValue = Integer.max(maxValue, split[1].length());
            }
        }

        Assertions.assertEquals(actual.size(), expected.size(), "expected.size()");
        Assertions.assertEquals(actual.keySet().size(), expected.keySet().size(), "expected.keySet().size()");

        for (String key : actual.keySet()) {
            if (!expected.containsKey(key)) {
                Assertions.fail(key + " not in key set but in map expected");
            }
        }

        for (String key : expected.keySet()) {
            if (!actual.containsKey(key)) {
                Assertions.fail(key + " not in key set but in map actual");
            }
        }
    }
}
