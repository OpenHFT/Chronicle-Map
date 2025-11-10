//
// Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
//

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
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class PointListSerializationTest {

    @Test
    public void testComplexSerialization() {
        try (ChronicleMap<String, A> map = ChronicleMapBuilder
                .of(String.class, A.class)
                .valueMarshaller(AMarshaller.INSTANCE)
                .entries(5)
                .averageKeySize(4)
                .averageValueSize(1000)
                .create()) {
            A obj_A = new A();
            obj_A.str_ = "a";
            obj_A.list_ = new ArrayList<>();
            B b = new B();
            b.str_ = "b";
            obj_A.list_.add(b);
            map.put("KEY1", obj_A);
            map.get("KEY1");
        }
    }

    static final class AMarshaller implements BytesReader<A>, BytesWriter<A>,
            EnumMarshallable<AMarshaller> {
        static final AMarshaller INSTANCE = new AMarshaller();

        private AMarshaller() {
        }

        @Override
        public void write(Bytes<?> out, @NotNull A toWrite) {
            out.writeUtf8(toWrite.str_);
            if (toWrite.list_ != null) {
                int size = toWrite.list_.size();
                out.writeStopBit(size);
                for (int i = 0; i < size; i++) {
                    toWrite.list_.get(i).writeMarshallable(out);
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
            using.str_ = in.readUtf8();
            int size = (int) in.readStopBit();
            if (size >= 0) {
                if (using.list_ == null) {
                    using.list_ = new ArrayList<>(size);
                } else {
                    using.list_.clear();
                    if (using.list_ instanceof ArrayList)
                        ((ArrayList<?>) using.list_).ensureCapacity(size);
                }
                for (int i = 0; i < size; i++) {
                    B b = new B();
                    b.readMarshallable(in);
                    using.list_.add(b);
                }
            } else {
                assert size == -1;
                using.list_ = null;
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
        String str_;
        List<B> list_;
    }

    static class B implements BytesMarshallable {
        String str_;

        @Override
        public void readMarshallable(BytesIn<?> in) {
            str_ = in.readUtf8();
        }

        @Override
        public void writeMarshallable(BytesOut<?> out) {
            out.writeUtf8(str_);
        }
    }
}
