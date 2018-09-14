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

package eg;

import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.MapSegmentContext;
import org.junit.Test;

import java.util.Arrays;
import java.util.LongSummaryStatistics;

public class AverageValueSizeTest {

    public static <V> double averageValueSize(Class<V> valueClass, Iterable<V> values) {
        try (ChronicleMap<Integer, V> testMap = ChronicleMap.of(Integer.class, valueClass)
                // doesn't matter, anyway not a single value will be written to a map
                .averageValueSize(1)
                .entries(1)
                .create()) {
            LongSummaryStatistics statistics = new LongSummaryStatistics();
            for (V value : values) {
                try (MapSegmentContext<Integer, V, ?> c = testMap.segmentContext(0)) {
                    statistics.accept(c.wrapValueAsData(value).size());
                }
            }
            return statistics.getAverage();
        }
    }

    @Test
    public void averageValueSizeTest() {
        System.out.println(averageValueSize(String.class,
                Arrays.asList("banana", "apple", "watermelon")));
    }
}
