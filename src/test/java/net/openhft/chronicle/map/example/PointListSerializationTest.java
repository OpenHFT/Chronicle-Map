/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map.example;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesMarshallable;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.chronicle.hash.serialization.BytesWriter;
import net.openhft.chronicle.hash.serialization.impl.EnumMarshallable;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

class PointListSerializationTest {

    @Test
    void testComplexSerialization() {
        try (ChronicleMap<String, A> map = ChronicleMapBuilder
                .of(String.class, A.class)
                .valueMarshaller(AMarshaller.INSTANCE)
                .entries(5)
                .averageKeySize(4)
                .averageValueSize(1000)
                .create()) {
            A objectA = new A();
            objectA.value = "a";
            objectA.items = new ArrayList<>();
            B b = new B();
            b.text = "b";
            objectA.items.add(b);
            map.put("KEY1", objectA);
            map.get("KEY1");
        }
    }

    static final class AMarshaller implements BytesReader<A>, BytesWriter<A>,
            EnumMarshallable<AMarshaller> {
        public static final AMarshaller INSTANCE = new AMarshaller();

        private AMarshaller() {
        }

        @Override
        public void write(Bytes<?> out, @NotNull A toWrite) {
            out.writeUtf8(toWrite.value);
            if (toWrite.items != null) {
                int size = toWrite.items.size();
                out.writeStopBit(size);
                for (int i = 0; i < size; i++) {
                    toWrite.items.get(i).writeMarshallable(out);
                }
            } else {
                out.writeStopBit(-1);
            }
        }

        @NotNull
        @Override
        public A read(Bytes<?> in, A using) {
            if (using == null)
                using = new A();
            using.value = in.readUtf8();
            int size = (int) in.readStopBit();
            if (size >= 0) {
                if (using.items == null) {
                    using.items = new ArrayList<>(size);
                } else {
                    using.items.clear();
                    if (using.items instanceof ArrayList)
                        ((ArrayList<?>) using.items).ensureCapacity(size);
                }
                for (int i = 0; i < size; i++) {
                    B b = new B();
                    b.readMarshallable(in);
                    using.items.add(b);
                }
            } else {
                assert size == -1;
                using.items = null;
            }
            return using;
        }

        @NotNull
        @Override
        public AMarshaller readResolve() {
            return INSTANCE;
        }
    }

    static class A {
        String value;
        List<B> items;
    }

    static class B implements BytesMarshallable {
        String text;

        @Override
        public void readMarshallable(BytesIn<?> in) {
            text = in.readUtf8();
        }

        @Override
        public void writeMarshallable(BytesOut<?> out) {
            out.writeUtf8(text);
        }
    }
}
