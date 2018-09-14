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
