/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
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

import java.util.List;
import java.util.Map;

import static com.google.common.collect.testing.features.MapFeature.*;

public class GuavaTest extends TestCase {

    public static Test suite() {
        TestSuite hhmTests = MapTestSuiteBuilder.using(new HHMTestGenerator())
                .named("HugeHashMap Guava tests")
                .withFeatures(GENERAL_PURPOSE)
                .withFeatures(CollectionSize.ANY)
                .withFeatures(CollectionFeature.REMOVE_OPERATIONS)
                .withFeatures(RESTRICTS_KEYS, RESTRICTS_VALUES)
                .createTestSuite();
        TestSuite tests = new TestSuite();
        tests.addTest(hhmTests);
        return tests;
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

    static class HHMTestGenerator extends CHMTestGenerator {
        @Override
        Map<CharSequence, CharSequence> newMap() {
            return builder.create();
        }
    }
}
