/*
 * Copyright 2014 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
        MapTestSuiteBuilder<CharSequence, CharSequence> chmSuite = using(new CHMTestGenerator());
        configureSuite(chmSuite);
        TestSuite chmTests = chmSuite.named("Guava tests of Chronicle Map").createTestSuite();

        MapTestSuiteBuilder<CharSequence, CharSequence> backed = using(new BackedUpMapGenerator());
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

    private static void configureSuite(MapTestSuiteBuilder<CharSequence, CharSequence> suite) {
        suite.withFeatures(GENERAL_PURPOSE)
                .withFeatures(CollectionSize.ANY)
                .withFeatures(CollectionFeature.REMOVE_OPERATIONS)
                .withFeatures(RESTRICTS_KEYS, RESTRICTS_VALUES);
    }

    static abstract class TestGenerator
            implements TestMapGenerator<CharSequence, CharSequence> {

        abstract Map<CharSequence, CharSequence> newMap();

        public CharSequence[] createKeyArray(int length) {
            return new CharSequence[length];
        }

        @Override
        public CharSequence[] createValueArray(int length) {
            return new CharSequence[length];
        }

        @Override
        public SampleElements<Map.Entry<CharSequence, CharSequence>> samples() {
            return SampleElements.mapEntries(
                    new SampleElements<CharSequence>(
                            "key1", "key2", "key3", "key4", "key5"
                    ),
                    new SampleElements<CharSequence>(
                            "val1", "val2", "val3", "val4", "val5"
                    )
            );
        }

        @Override
        public Map<CharSequence, CharSequence> create(Object... objects) {
            Map<CharSequence, CharSequence> map = newMap();
            for (Object obj : objects) {
                Map.Entry e = (Map.Entry) obj;
                map.put((CharSequence) e.getKey(),
                        (CharSequence) e.getValue());
            }
            return map;
        }

        @Override
        public Map.Entry<CharSequence, CharSequence>[] createArray(int length) {
            return new Map.Entry[length];
        }

        @Override
        public Iterable<Map.Entry<CharSequence, CharSequence>> order(
                List<Map.Entry<CharSequence, CharSequence>> insertionOrder) {
            return insertionOrder;
        }
    }

    static class CHMTestGenerator extends TestGenerator {
        ChronicleMapBuilder<CharSequence, CharSequence> builder =
                ChronicleMapBuilder.of(CharSequence.class, CharSequence.class)
                        .entries(100)
                        .averageKeySize(10).averageValueSize(10)
                        .minSegments(2);

        @Override
        Map<CharSequence, CharSequence> newMap() {
            return builder.create();
        }
    }
    
    static class BackedUpMapGenerator extends CHMTestGenerator {
        
        @Override
        Map<CharSequence, CharSequence> newMap() {
            Map<CharSequence, CharSequence> m = new HashMap<>();
            builder.entryOperations(new MapEntryOperations<CharSequence, CharSequence, Void>() {
                @Override
                public Void remove(@NotNull MapEntry<CharSequence, CharSequence> entry) {
                    Assert.assertEquals(m, entry.context().map());
                    m.remove(entry.key().get().toString());
                    return MapEntryOperations.super.remove(entry);
                }

                @Override
                public Void replaceValue(@NotNull MapEntry<CharSequence, CharSequence> entry,
                                         net.openhft.chronicle.hash.Data<CharSequence, ?> newValue) {
                    Assert.assertEquals(m, entry.context().map());
                    m.put(entry.key().get().toString(), newValue.get().toString());
                    return MapEntryOperations.super.replaceValue(entry, newValue);
                }

                @Override
                public Void insert(@NotNull MapAbsentEntry<CharSequence, CharSequence> absentEntry,
                                   Data<CharSequence, ?> value) {
                    Assert.assertEquals(m, absentEntry.context().map());
                    m.put(absentEntry.absentKey().get().toString(), value.get().toString());
                    return MapEntryOperations.super.insert(absentEntry, value);
                }
            });
            return builder.create();
        }
    }
}
