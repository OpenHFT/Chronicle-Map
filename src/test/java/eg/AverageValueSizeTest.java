/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package eg;

import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.MapSegmentContext;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.LongSummaryStatistics;

import static org.junit.jupiter.api.Assertions.assertTrue;

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
        double average = averageValueSize(String.class, Arrays.asList("banana", "apple", "watermelon"));
        assertTrue(average > 0, "averageValueSize should be positive");
        System.out.println(average);
    }
}
