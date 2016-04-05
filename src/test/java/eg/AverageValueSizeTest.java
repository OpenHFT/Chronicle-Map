/*
 *      Copyright (C) 2015  higherfrequencytrading.com
 *
 *      This program is free software: you can redistribute it and/or modify
 *      it under the terms of the GNU Lesser General Public License as published by
 *      the Free Software Foundation, either version 3 of the License.
 *
 *      This program is distributed in the hope that it will be useful,
 *      but WITHOUT ANY WARRANTY; without even the implied warranty of
 *      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *      GNU Lesser General Public License for more details.
 *
 *      You should have received a copy of the GNU Lesser General Public License
 *      along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
