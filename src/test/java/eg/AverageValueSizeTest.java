/*
 * Copyright 2013-2026 chronicle.software; SPDX-License-Identifier: Apache-2.0
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
