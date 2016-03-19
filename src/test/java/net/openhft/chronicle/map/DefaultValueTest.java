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

package net.openhft.chronicle.map;

import net.openhft.chronicle.hash.serialization.ListMarshaller;
import net.openhft.chronicle.hash.serialization.impl.IntegerMarshaller;
import net.openhft.chronicle.set.Builder;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class DefaultValueTest {

    @Test
    public void test() throws IllegalAccessException, InstantiationException, IOException {
        File file = Builder.getPersistenceFile();
        try {

            ArrayList<Integer> defaultValue = new ArrayList<Integer>();
            defaultValue.add(42);
            try (ChronicleMap<String, List<Integer>> map = ChronicleMap
                    .of(String.class, (Class<List<Integer>>) ((Class) List.class))
                    .valueMarshaller(ListMarshaller.of(IntegerMarshaller.INSTANCE))
                    .entries(3)
                    .averageKey("a").averageValue(Arrays.asList(1, 2))
                    .defaultValueProvider(absentEntry ->
                            absentEntry.context().wrapValueAsData(defaultValue))
                    .createPersistedTo(file)) {
                ArrayList<Integer> using = new ArrayList<Integer>();
                assertEquals(defaultValue, map.acquireUsing("a", using));
                assertEquals(1, map.size());

                map.put("b", Arrays.asList(1, 2));
                assertEquals(Arrays.asList(1, 2), map.acquireUsing("b", using));
            }

            ArrayList<Integer> using = new ArrayList<Integer>();
            try (ChronicleMap<String, List<Integer>> map = ChronicleMap
                    .of(String.class, (Class<List<Integer>>) ((Class) List.class))
                    .defaultValueProvider(absentEntry ->
                            absentEntry.context().wrapValueAsData(defaultValue))
                    .createPersistedTo(file)) {
                assertEquals(defaultValue, map.acquireUsing("c", using));
            }
        } finally {
            file.delete();
        }
    }
}
