/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.xstream.converters;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.converters.ConversionException;
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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Mutation-tested coverage for AbstractChronicleMapConverter.unmarshal.
 * <p>
 * Each test in this file was retained because it kills at least one mutation
 * (real bug or contrived) that no other kept test catches. Tests that did not
 * earn their keep against the catalogued mutations were dropped.
 * <p>
 * Mutations covered:
 * - drop spurious moveDown()/moveUp() (the original 1.4.20→1.4.21 regression)
 * - drop "entry" node-name validation
 * - drop "cmap" top-level validation
 * - force the always-wrapped path (drop else branch)
 * - force the always-flat path (drop if branch — needs legacy fixture)
 * - drop empty-map early-return
 * - drop the readEntries() loop body
 */
public class AbstractChronicleMapConverterFuzzTest {

    /** Empty-map early-return path. Only test that exercises the leading
     *  {@code reader.getValue()} probe. */
    @Test
    public void roundTrip_emptyMap() throws IOException {
        roundTripStringMap(new LinkedHashMap<>());
    }

    /** Single-entry hits the else branch with no remaining children, calling
     *  readEntry once and readEntries against an empty cursor. Distinct from
     *  the multi-entry case because killing only the readEntries loop still
     *  leaves this test passing. */
    @Test
    public void roundTrip_singleEntry() throws IOException {
        Map<String, String> m = new LinkedHashMap<>();
        m.put("only", "entry");
        roundTripStringMap(m);
    }

    /** Multi-entry exercises the readEntries while-loop body, which the
     *  single-entry case does not reach. */
    @Test
    public void roundTrip_twoEntries() throws IOException {
        Map<String, String> m = new LinkedHashMap<>();
        m.put("a", "1");
        m.put("b", "2");
        roundTripStringMap(m);
    }

    /** Structural cursor-balance backstop. A future change that drops a
     *  moveUp without breaking content (Jettison is forgiving) is caught
     *  here even when round-trip equality still passes. */
    @Test
    public void readerBalanced_twoEntries() throws IOException {
        Map<String, String> m = new LinkedHashMap<>();
        m.put("a", "1");
        m.put("b", "2");
        File f = newJsonFile();
        try {
            try (ChronicleMap<String, String> src = stringMap()) {
                src.putAll(m);
                src.getAll(f);
            }
            byte[] bytes = Files.readAllBytes(f.toPath());
            try (ChronicleMap<String, String> dst = stringMap()) {
                JettisonMappedXmlDriver driver = new JettisonMappedXmlDriver();
                XStream xs = new XStream(driver);
                xs.setMode(XStream.NO_REFERENCES);
                xs.alias("cmap", dst.getClass());
                xs.registerConverter(new VanillaChronicleMapConverter<>(dst));
                CountingReader counter = new CountingReader(
                        driver.createReader(new ByteArrayInputStream(bytes)));
                xs.unmarshal(counter, dst);
                assertEquals("moveDown vs moveUp imbalance",
                        counter.downCount, counter.upCount);
                assertEquals(m.size(), dst.size());
            }
        } finally {
            Files.delete(f.toPath());
        }
    }

    /** Defensive top-level cmap check; reachable only via direct converter
     *  invocation since XStream's class resolver short-circuits the
     *  fromXML route. */
    @Test
    public void errorOnNonCmapTopLevel() {
        try (ChronicleMap<String, String> dst = stringMap()) {
            try {
                directUnmarshal("{\"notcmap\":{\"entry\":{}}}", dst);
                fail("expected ConversionException");
            } catch (ConversionException e) {
                assertTrue("message: " + e.getMessage(),
                        e.getMessage().contains("cmap"));
            }
        }
    }

    /** entry-node validation inside readEntry. */
    @Test
    public void errorOnNonEntryChild() {
        try (ChronicleMap<String, String> dst = stringMap()) {
            try {
                directUnmarshal("{\"cmap\":{\"notanentry\":{}}}", dst);
                fail("expected ConversionException");
            } catch (ConversionException e) {
                assertTrue("name should appear in: " + e.getMessage(),
                        e.getMessage().contains("notanentry"));
            }
        }
    }

    /** The double-cmap wrapper shape from XStream 1.4.20 lives only in this
     *  fixture: 1.4.21 round-trips never produce it, so without this test
     *  the wrapped branch in unmarshal is dead code. */
    @Test
    public void pinned_legacy_1_4_20_doubleCmapWrapper() throws IOException {
        byte[] fixture = readFixture("legacy-1.4.20-multi-entry.json");
        try (ChronicleMap<String, String> dst = stringMap()) {
            unmarshalBytes(fixture, dst);
            assertEquals(3, dst.size());
            assertEquals("1", dst.get("a"));
            assertEquals("2", dst.get("b"));
            assertEquals("3", dst.get("c"));
        }
    }

    /** Pinned snapshot of the current XStream 1.4.21 output. Backstops a
     *  silent shape change in a future XStream/Jettison upgrade. */
    @Test
    public void pinned_currentShape_multipleEntries() throws IOException {
        byte[] fixture = readFixture("pinned-multi-entry.json");
        try (ChronicleMap<String, String> dst = stringMap()) {
            unmarshalBytes(fixture, dst);
            assertEquals(3, dst.size());
            assertEquals("1", dst.get("a"));
            assertEquals("2", dst.get("b"));
            assertEquals("3", dst.get("c"));
        }
    }

    // ---------- helpers ----------

    private static void roundTripStringMap(Map<String, String> expected) throws IOException {
        File f = newJsonFile();
        try {
            try (ChronicleMap<String, String> src = stringMap();
                 ChronicleMap<String, String> dst = stringMap()) {
                src.putAll(expected);
                src.getAll(f);
                dst.putAll(f);
                assertEquals(expected.size(), dst.size());
                for (Map.Entry<String, String> e : expected.entrySet())
                    assertEquals("entry " + e.getKey(), e.getValue(), dst.get(e.getKey()));
            }
        } finally {
            Files.delete(f.toPath());
        }
    }

    private static void unmarshalBytes(byte[] bytes, ChronicleMap<String, String> dst) {
        JettisonMappedXmlDriver driver = new JettisonMappedXmlDriver();
        XStream xs = new XStream(driver);
        xs.setMode(XStream.NO_REFERENCES);
        xs.alias("cmap", dst.getClass());
        xs.registerConverter(new VanillaChronicleMapConverter<>(dst));
        xs.unmarshal(driver.createReader(new ByteArrayInputStream(bytes)), dst);
    }

    private static void directUnmarshal(String json, ChronicleMap<String, String> dst) {
        JettisonMappedXmlDriver driver = new JettisonMappedXmlDriver();
        HierarchicalStreamReader reader = driver.createReader(
                new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)));
        new VanillaChronicleMapConverter<>(dst).unmarshal(reader, null);
    }

    private static byte[] readFixture(String name) throws IOException {
        try (java.io.InputStream in = AbstractChronicleMapConverterFuzzTest.class
                .getResourceAsStream("/xstream/" + name)) {
            if (in == null) throw new IOException("missing fixture: " + name);
            java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
            byte[] buf = new byte[1024];
            for (int n; (n = in.read(buf)) >= 0; ) out.write(buf, 0, n);
            return out.toByteArray();
        }
    }

    private static File newJsonFile() {
        File f = new File(OS.getTarget() + "/converter-fuzz-" + Time.uniqueId() + ".json");
        f.deleteOnExit();
        return f;
    }

    private static ChronicleMap<String, String> stringMap() {
        return ChronicleMapBuilder
                .of(String.class, String.class)
                .averageKeySize(8).averageValueSize(8)
                .entries(256)
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
