/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.json.JettisonMappedXmlDriver;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.util.Time;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.map.fromdocs.BondVOInterface;
import net.openhft.chronicle.values.Values;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author Rob Austin.
 */
@SuppressWarnings({"rawtypes", "unchecked", "try"})
public class ChronicleMapImportExportTest {

    public static final String TMP = OS.getTarget();

    @Test
    public void test() throws IOException {

        File file = new File(TMP + "/chronicle-map-" + Time.uniqueId() + ".json");
        file.deleteOnExit();

        ChronicleMapBuilder<String, String> builder = ChronicleMapBuilder
                .of(String.class, String.class)
                .averageKeySize(10).averageValueSize(10)
                .entries(1000);
        try (ChronicleMap<String, String> expected = builder.create()) {
            expected.put("hello", "world");
            expected.put("aKey", "aValue");
            expected.getAll(file);

            try (ChronicleMap<String, String> actual = builder.create()) {
                actual.putAll(file);

                Assertions.assertEquals(expected, actual, "imported string map should match exported map");
            }
        } finally {
            file.delete();
        }
    }

    @Test
    public void testWithMapValue() throws IOException {

        File file = new File(TMP + "/chronicle-map-" + Time.uniqueId() + ".json");
        file.deleteOnExit();

        System.out.println(file.getAbsoluteFile());
        ChronicleMapBuilder<String, Map> builder = ChronicleMapBuilder
                .of(String.class, Map.class)
                .averageKeySize("hello".length())
                .averageValueSize(100)
                .entries(1000);
        try (ChronicleMap<String, Map> expected = builder
                .create()) {
            HashMap<String, String> data = new HashMap<>();
            data.put("myKey", "myValue");
            expected.put("hello", data);
            expected.getAll(file);

            try (ChronicleMap<String, Map> actual = builder.create()) {
                actual.putAll(file);

                Assertions.assertEquals(expected, actual, "imported map with Map values should match exported map");
            }
        } finally {
            file.delete();
        }
    }

    @Test
    public void testWithMapOfMapValue() throws IOException {

        File file = new File(TMP + "/chronicle-map-" + Time.uniqueId() + ".json");
        file.deleteOnExit();

        System.out.println(file.getAbsoluteFile());
        ChronicleMapBuilder<String, Map> builder = ChronicleMapBuilder
                .of(String.class, Map.class)
                .averageKeySize("hello".length()).averageValueSize(100).entries(1000);
        try (ChronicleMap<String, Map> expected = builder.create()) {
            HashMap<String, Map> data = new HashMap<>();
            HashMap<String, String> data2 = new HashMap<>();
            data2.put("nested", "map");
            data.put("myKey", data2);
            expected.put("hello", data);

            expected.getAll(file);

            try (ChronicleMap<String, Map> actual = builder
                    .create()) {
                actual.putAll(file);

                Assertions.assertEquals(expected, actual, "imported map with nested Map values should match exported map");
            }
        } finally {
            file.delete();
        }
    }

    @Test
    public void testWithIntegerAndDouble() throws IOException {

        File file = new File(TMP + "/chronicle-map-" + Time.uniqueId() + ".json");
        file.deleteOnExit();

        try (ChronicleMap<Integer, Double> expected = ChronicleMap.of(Integer.class, Double.class)
                .entries(1).create()) {
            expected.put(1, 1.0);

            expected.getAll(file);

            try (ChronicleMap<Integer, Double> actual = ChronicleMap.of(Integer.class, Double.class)
                    .entries(1).create()) {
                actual.putAll(file);

                Assertions.assertEquals(expected, actual, "imported Integer-Double map should match exported map");
            }
        } finally {
            file.delete();
        }
    }

    @Test
    public void testWithCharSeq() throws IOException {

        File file = new File(TMP + "/chronicle-map-" + Time.uniqueId() + ".json");
        file.deleteOnExit();

        System.out.println(file.getAbsolutePath());
        ChronicleMapBuilder<CharSequence, CharSequence> builder =
                ChronicleMapBuilder.of(CharSequence.class, CharSequence.class)
                        .averageKeySize("hello".length()).averageValueSize("world".length())
                        .entries(1000);
        try (ChronicleMap<CharSequence, CharSequence> expected = builder
                .create()) {
            expected.put("hello", "world");

            expected.getAll(file);

            try (ChronicleMap<CharSequence, CharSequence> actual = builder
                    .create()) {
                actual.putAll(file);

                Assertions.assertEquals(expected, actual, "imported CharSequence map should match exported map");
            }
        } finally {
            file.delete();
        }
    }

    @Test
    public void testFromHashMap() throws IOException {

        File file = new File(TMP + "/chronicle-map-" + Time.uniqueId() + ".json");
        System.out.println(file.getCanonicalFile());

        File file2 = new File(TMP + "/chronicle-map-2" + Time.uniqueId() + ".json");
        System.out.println(file2.getCanonicalFile());

        HashMap<Integer, String> map = new HashMap<Integer, String>();
        map.put(1, "one");
        map.put(2, "two");

        final XStream xstream = new XStream(new JettisonMappedXmlDriver());
        xstream.setMode(XStream.NO_REFERENCES);

        xstream.toXML(map, Files.newOutputStream(file.toPath()));

        try (ChronicleMap<Integer, String> expected = ChronicleMapBuilder
                .of(Integer.class, String.class)
                .averageValueSize(10)
                .entries(1000)
                .create()) {

            expected.put(1, "one");
            expected.put(2, "two");

            expected.getAll(file2);
            expected.putAll(file2);

            Assertions.assertEquals(2, expected.size(), "map size should be 2 after round-trip export and import");
            Assertions.assertEquals("one", expected.get(1), "value for key 1 should be 'one' after round-trip");
            Assertions.assertEquals("two", expected.get(2), "value for key 2 should be 'two' after round-trip");
        }

        file.deleteOnExit();
    }

    @Test
    public void testWithLongValue() throws IOException {

        File file = new File(TMP + "/chronicle-map-" + Time.uniqueId() + ".json");
        //file.deleteOnExit();

        System.out.println(file.getAbsolutePath());
        ChronicleMapBuilder<CharSequence, LongValue> builder = ChronicleMapBuilder
                .of(CharSequence.class, LongValue.class)
                .averageKeySize("one".length())
                .entries(1000);
        try (ChronicleMap<CharSequence, LongValue> expected = builder.create()) {
            LongValue value = Values.newNativeReference(LongValue.class);

            // this will add the entry
            try (Closeable c =
                         expected.acquireContext("one", value)) {
                assertEquals(0, value.getValue(), "newly acquired LongValue should have initial value of 0");
                value.addValue(1);
            }

            expected.getAll(file);

            try (ChronicleMap<CharSequence, LongValue> actual = builder.create()) {

                actual.putAll(file);

                Assertions.assertEquals(expected, actual, "imported LongValue map should match exported map");
            }
        } finally {
            file.delete();
        }
    }

    @Test
    public void testBondVOInterface() throws IOException {

        File file = new File(TMP + "/chronicle-map-" + Time.uniqueId() + ".json");
        file.deleteOnExit();

        System.out.println(file.getAbsolutePath());
        ChronicleMapBuilder<CharSequence, BondVOInterface> builder =
                ChronicleMapBuilder.of(CharSequence.class, BondVOInterface.class)
                        .averageKeySize("one".length()).entries(1000);
        try (ChronicleMap<CharSequence, BondVOInterface> expected =
                     builder.create()) {

            final BondVOInterface value = Values.newNativeReference(BondVOInterface.class);

            // this will add the entry
            try (Closeable c = expected.acquireContext("one", value)) {
                value.setCoupon(8.98);
                BondVOInterface.MarketPx marketPxIntraDayHistoryAt =
                        value.getMarketPxIntraDayHistoryAt(1);

                marketPxIntraDayHistoryAt.setAskPx(12.0);
            }

            expected.getAll(file);

            try (ChronicleMap<CharSequence, BondVOInterface> actual = builder.create()) {

                actual.putAll(file);

                Assertions.assertEquals(expected.get("one").getCoupon(),
                        actual.get("one").getCoupon(), 0, "actual.get(<str>).getCoupon()");
            }
        } finally {
            file.delete();
        }
    }
}
