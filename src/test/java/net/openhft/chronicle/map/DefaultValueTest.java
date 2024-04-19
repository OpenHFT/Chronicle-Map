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

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesMarshallable;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.serialization.ListMarshaller;
import net.openhft.chronicle.hash.serialization.impl.IntegerMarshaller;
import net.openhft.chronicle.set.Builder;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

@SuppressWarnings({"rawtypes", "unchecked"})
public class DefaultValueTest {

    @Test
    public void test1() {
        Bytes<byte[]> bytes = Bytes.allocateElasticOnHeap();

        final Wire wire = WireType.BINARY.apply(bytes);

        wire.getValueOut().object(new SimpleDefaultValueProvider<Integer, A>(new A(2, "b")));

        final Object object = wire.getValueIn().object();

        System.out.println(object);
    }

    private static class SimpleDefaultValueProvider<K, V> extends SelfDescribingMarshallable implements DefaultValueProvider<K, V> {

        private final V defaultValue;

        public SimpleDefaultValueProvider(V defaultValue) {
            this.defaultValue = defaultValue;
        }

        @Override
        public Data<V> defaultValue(@NotNull MapAbsentEntry<K, V> absentEntry) {
            return null;
        }
    }

    private static class A implements BytesMarshallable {
        public A(int x, String y) {
            this.x = x;
            this.y = y;
        }

        int x;
        String y;

        @Override
        public void readMarshallable(BytesIn<?> bytes) throws IORuntimeException, BufferUnderflowException, IllegalStateException {
            x = bytes.readInt();
            y = bytes.readUtf8();
        }

        @Override
        public void writeMarshallable(BytesOut<?> bytes) throws IllegalStateException, BufferOverflowException, BufferUnderflowException, ArithmeticException {
            bytes.writeInt(x);
            bytes.writeUtf8(y);
        }
    }

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
