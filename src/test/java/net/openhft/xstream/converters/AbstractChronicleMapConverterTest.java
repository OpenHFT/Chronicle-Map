/*
 * Copyright 2013-2026 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.xstream.converters;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.HierarchicalStreamReader;
import com.thoughtworks.xstream.io.ReaderWrapper;
import com.thoughtworks.xstream.io.json.JettisonMappedXmlDriver;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.util.Time;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Regression coverage for the {@link AbstractChronicleMapConverter#unmarshal}
 * fix that removed the spurious {@code reader.moveDown()/moveUp()} pair
 * around the entry-iteration loop. Under xstream 1.4.21 the original code
 * descended into the first {@code entry}'s first child and then asked
 * {@code hasMoreChildren()} on the wrong tree level, throwing
 * {@code "unable to convert node named=java.lang.String"} for any non-empty
 * map. Each of the three tests below fails against the pre-fix code and
 * passes against the fix.
 */
public class AbstractChronicleMapConverterTest {

    private static final String TMP = OS.getTarget();

    @Test
    public void singleEntryRoundTripPreservesContent() throws IOException {
        File file = newJsonFile();
        try {
            try (ChronicleMap<String, String> src = stringMap();
                 ChronicleMap<String, String> dst = stringMap()) {
                src.put("only", "entry");
                src.getAll(file);
                dst.putAll(file);
                assertEquals(1, dst.size());
                assertEquals("entry", dst.get("only"));
            }
        } finally {
            file.delete();
        }
    }

    @Test
    public void multiEntryRoundTripPreservesEveryEntry() throws IOException {
        File file = newJsonFile();
        try {
            Map<String, String> expected = new LinkedHashMap<>();
            for (int i = 0; i < 25; i++) {
                expected.put("k" + i, "v" + i);
            }
            try (ChronicleMap<String, String> src = stringMap();
                 ChronicleMap<String, String> dst = stringMap()) {
                src.putAll(expected);
                src.getAll(file);
                dst.putAll(file);
                assertEquals(expected.size(), dst.size());
                for (Map.Entry<String, String> e : expected.entrySet()) {
                    assertEquals("entry " + e.getKey(), e.getValue(), dst.get(e.getKey()));
                }
            }
        } finally {
            file.delete();
        }
    }

    /**
     * The converter must leave the reader balanced — every {@code moveDown}
     * matched by a {@code moveUp}. The pre-fix code was structurally
     * unbalanced once the loop body executed at all.
     */
    @Test
    public void unmarshalLeavesReaderBalanced() throws IOException {
        File file = newJsonFile();
        try {
            try (ChronicleMap<String, String> src = stringMap()) {
                for (int i = 0; i < 5; i++) {
                    src.put("k" + i, "v" + i);
                }
                src.getAll(file);
            }
            byte[] bytes = Files.readAllBytes(file.toPath());

            try (ChronicleMap<String, String> dst = stringMap()) {
                JettisonMappedXmlDriver driver = new JettisonMappedXmlDriver();
                XStream xstream = new XStream(driver);
                xstream.setMode(XStream.NO_REFERENCES);
                xstream.alias("cmap", dst.getClass());
                xstream.registerConverter(new VanillaChronicleMapConverter<>(dst));
                CountingReader counter = new CountingReader(
                        driver.createReader(new ByteArrayInputStream(bytes)));
                xstream.unmarshal(counter, dst);
                assertEquals("moveDown vs moveUp imbalance",
                        counter.downCount, counter.upCount);
                assertEquals(5, dst.size());
            }
        } finally {
            file.delete();
        }
    }

    private static File newJsonFile() throws IOException {
        File f = new File(TMP + "/converter-test-" + Time.uniqueId() + ".json");
        f.deleteOnExit();
        return f;
    }

    private static ChronicleMap<String, String> stringMap() {
        return ChronicleMapBuilder
                .of(String.class, String.class)
                .averageKeySize(8).averageValueSize(8)
                .entries(64)
                .create();
    }

    private static final class CountingReader extends ReaderWrapper {
        int downCount;
        int upCount;

        CountingReader(HierarchicalStreamReader wrapped) {
            super(wrapped);
        }

        @Override
        public void moveDown() {
            downCount++;
            super.moveDown();
        }

        @Override
        public void moveUp() {
            upCount++;
            super.moveUp();
        }
    }
}
