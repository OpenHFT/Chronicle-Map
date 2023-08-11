/*
 * Copyright 2012-2018 Chronicle Map Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.map;

import com.google.common.collect.testing.MapTestSuiteBuilder;
import com.google.common.collect.testing.SampleElements;
import com.google.common.collect.testing.TestMapGenerator;
import com.google.common.collect.testing.features.CollectionFeature;
import com.google.common.collect.testing.features.CollectionSize;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import net.openhft.chronicle.hash.Data;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.testing.MapTestSuiteBuilder.using;
import static com.google.common.collect.testing.features.MapFeature.*;

public class GuavaTest extends TestCase {

    public static Test suite() {
        MapTestSuiteBuilder<String, String> chmSuite = using(new CHMTestGenerator());
        configureSuite(chmSuite);
        TestSuite chmTests = chmSuite.named("Guava tests of Chronicle Map").createTestSuite();

        MapTestSuiteBuilder<String, String> backed = using(new BackedUpMapGenerator());
        configureSuite(backed);
        TestSuite backedTests = backed
                .named("Guava tests tests of Chronicle Map, backed with HashMap")
                .createTestSuite();

        TestSuite tests = new TestSuite();
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

    static abstract class TestGenerator
            implements TestMapGenerator<String, String> {

        abstract Map<String, String> newMap();

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
        ChronicleMapBuilder<String, String> builder =
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
                    Assert.assertEquals(m, entry.context().map());
                    m.remove(entry.key().get());
                    return MapEntryOperations.super.remove(entry);
                }

                @Override
                public Void replaceValue(@NotNull MapEntry<String, String> entry,
                                         net.openhft.chronicle.hash.Data<String> newValue) {
                    Assert.assertEquals(m, entry.context().map());
                    m.put(entry.key().get(), newValue.get());
                    return MapEntryOperations.super.replaceValue(entry, newValue);
                }

                @Override
                public Void insert(@NotNull MapAbsentEntry<String, String> absentEntry,
                                   Data<String> value) {
                    Assert.assertEquals(m, absentEntry.context().map());
                    m.put(absentEntry.absentKey().get(), value.get());
                    return MapEntryOperations.super.insert(absentEntry, value);
                }
            });
            return builder.create();
        }
    }
}
