/*
 *      Copyright (C) 2012, 2016  higherfrequencytrading.com
 *      Copyright (C) 2016 Roman Leventov
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

package net.openhft.chronicle.map.example;

import net.openhft.chronicle.core.values.IntValue;
import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.map.*;
import net.openhft.chronicle.values.Values;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static net.openhft.chronicle.map.example.SimpleMapOperationsListeningTest.SimpleLoggingDefaultValueProvider.simpleLoggingDefaultValueProvider;
import static net.openhft.chronicle.map.example.SimpleMapOperationsListeningTest.SimpleLoggingMapEntryOperations.simpleLoggingMapEntryOperations;

public class SimpleMapOperationsListeningTest {

    @Test
    public void simpleLoggingTest() {
        ChronicleMap<Integer, IntValue> map = ChronicleMapBuilder
                .of(Integer.class, IntValue.class)
                .entries(100)
                .entryOperations(simpleLoggingMapEntryOperations())
                .defaultValueProvider(simpleLoggingDefaultValueProvider())
                .create();

        IntValue value = Values.newHeapInstance(IntValue.class);
        value.setValue(2);
        map.put(1, value);
        map.remove(1);
        map.acquireUsing(3, Values.newNativeReference(IntValue.class)).addAtomicValue(1);
        IntValue value2 = Values.newHeapInstance(IntValue.class);
        value2.setValue(5);
        map.forEachEntry(e -> e.context().replaceValue(e, e.context().wrapValueAsData(value2)));
        map.forEachEntry(e -> e.context().remove(e));

    }

    static class SimpleLoggingMapEntryOperations<K, V> implements MapEntryOperations<K, V, Void> {

        private static final SimpleLoggingMapEntryOperations INSTANCE =
                new SimpleLoggingMapEntryOperations();

        private SimpleLoggingMapEntryOperations() {
        }

        public static <K, V> MapEntryOperations<K, V, Void> simpleLoggingMapEntryOperations() {
            return SimpleLoggingMapEntryOperations.INSTANCE;
        }

        @Override
        public Void remove(@NotNull MapEntry<K, V> entry) {
            System.out.println("remove " + entry.key() + ": " + entry.value());
            entry.doRemove();
            return null;
        }

        @Override
        public Void replaceValue(@NotNull MapEntry<K, V> entry, Data<V> newValue) {
            System.out.println("replace " + entry.key() + ": " + entry.value() + " -> " + newValue);
            entry.doReplaceValue(newValue);
            return null;
        }

        @Override
        public Void insert(@NotNull MapAbsentEntry<K, V> absentEntry, Data<V> value) {
            System.out.println("insert " + absentEntry.absentKey() + " -> " + value);
            absentEntry.doInsert(value);
            return null;
        }
    }

    static class SimpleLoggingDefaultValueProvider<K, V> implements DefaultValueProvider<K, V> {

        private static final SimpleLoggingDefaultValueProvider INSTANCE =
                new SimpleLoggingDefaultValueProvider();

        private SimpleLoggingDefaultValueProvider() {
        }

        public static <K, V> DefaultValueProvider<K, V> simpleLoggingDefaultValueProvider() {
            return INSTANCE;
        }

        @Override
        public Data<V> defaultValue(@NotNull MapAbsentEntry<K, V> absentEntry) {
            Data<V> defaultValue = absentEntry.defaultValue();
            System.out.println("default " + absentEntry.absentKey() + " -> " + defaultValue);
            return defaultValue;
        }
    }
}
