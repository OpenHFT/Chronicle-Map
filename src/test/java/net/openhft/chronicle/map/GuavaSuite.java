/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import com.google.common.collect.testing.MapTestSuiteBuilder;
import com.google.common.collect.testing.SampleElements;
import com.google.common.collect.testing.TestMapGenerator;
import com.google.common.collect.testing.features.CollectionFeature;
import com.google.common.collect.testing.features.CollectionSize;
import net.openhft.chronicle.hash.Data;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.testing.MapTestSuiteBuilder.using;
import static com.google.common.collect.testing.features.MapFeature.*;

@SuppressWarnings({"rawtypes", "unchecked", "serial"})
public class GuavaSuite {

    @Test
    public void runGuavaMapTestSuite() {
        junit.framework.Test suite = suite();
        junit.framework.TestResult result = new junit.framework.TestResult();
        suite.run(result);
        Assertions.assertEquals(0, result.failureCount(), "guava suite: failures");
        Assertions.assertEquals(0, result.errorCount(), "guava suite: errors");
    }

    public static junit.framework.Test suite() {
        MapTestSuiteBuilder<String, String> chmSuite = using(new CHMTestGenerator());
        configureSuite(chmSuite);
        junit.framework.TestSuite chmTests = chmSuite.named("Guava tests of Chronicle Map").createTestSuite();

        MapTestSuiteBuilder<String, String> backed = using(new BackedUpMapGenerator());
        configureSuite(backed);
        junit.framework.TestSuite backedTests = backed
                .named("Guava tests tests of Chronicle Map, backed with HashMap")
                .createTestSuite();

        junit.framework.TestSuite tests = new junit.framework.TestSuite();
        tests.addTest(chmTests);
        // TODO
        //tests.addTest(backedTests);
        return tests;
    }

    private static void configureSuite(MapTestSuiteBuilder<String, String> suite) {
        suite.withFeatures(GENERAL_PURPOSE)
                .withFeatures(CollectionSize.ANY)
                .withFeatures(CollectionFeature.REMOVE_OPERATIONS)
                .withFeatures(RESTRICTS_KEYS, RESTRICTS_VALUES);
    }

    abstract static class TestGenerator
            implements TestMapGenerator<String, String> {

        abstract Map<String, String> newMap();

        @Override
        public String[] createKeyArray(int length) {
            return new String[length];
        }

        @Override
        public String[] createValueArray(int length) {
            return new String[length];
        }

        @Override
        public SampleElements<Map.Entry<String, String>> samples() {
            return SampleElements.mapEntries(
                    new SampleElements<>("key1", "key2", "key3", "key4", "key5"),
                    new SampleElements<>("val1", "val2", "val3", "val4", "val5")
            );
        }

        @Override
        public Map<String, String> create(Object... objects) {
            Map<String, String> map = newMap();
            for (Object obj : objects) {
                Map.Entry e = (Map.Entry) obj;
                map.put((String) e.getKey(),
                        (String) e.getValue());
            }
            return map;
        }

        @Override
        public Map.Entry<String, String>[] createArray(int length) {
            //noinspection unchecked
            return new Map.Entry[length];
        }

        @Override
        public Iterable<Map.Entry<String, String>> order(
                List<Map.Entry<String, String>> insertionOrder) {
            return insertionOrder;
        }
    }

    static class CHMTestGenerator extends TestGenerator {
        final ChronicleMapBuilder<String, String> builder =
                ChronicleMapBuilder.of(String.class, String.class)
                        .entries(100)
                        .averageKeySize(10).averageValueSize(10)
                        .minSegments(2);

        @Override
        Map<String, String> newMap() {
            return builder.create();
        }
    }

    static class BackedUpMapGenerator extends CHMTestGenerator {

        @Override
        Map<String, String> newMap() {
            Map<String, String> m = new HashMap<>();
            builder.entryOperations(new MapEntryOperations<String, String, Void>() {
                @Override
                public Void remove(@NotNull MapEntry<String, String> entry) {
                    Assertions.assertEquals(m, entry.context().map(), "entry.context().map()");
                    m.remove(entry.key().get());
                    return MapEntryOperations.super.remove(entry);
                }

                @Override
                public Void replaceValue(@NotNull MapEntry<String, String> entry,
                                         net.openhft.chronicle.hash.Data<String> newValue) {
                    Assertions.assertEquals(m, entry.context().map(), "entry.context().map()");
                    m.put(entry.key().get(), newValue.get());
                    return MapEntryOperations.super.replaceValue(entry, newValue);
                }

                @Override
                public Void insert(@NotNull MapAbsentEntry<String, String> absentEntry,
                                   Data<String> value) {
                    Assertions.assertEquals(m, absentEntry.context().map(), "absentEntry.context().map()");
                    m.put(absentEntry.absentKey().get(), value.get());
                    return MapEntryOperations.super.insert(absentEntry, value);
                }
            });
            return builder.create();
        }
    }
}
