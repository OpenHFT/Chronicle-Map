/*
 * Copyright 2015 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.map.example;

import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.serialization.BytesMarshallable;
import net.openhft.lang.io.serialization.BytesMarshaller;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class ComplexSerializationTest {

    static class A {
        String str_;
        List<B> list_;
    }

    enum AMarshaller implements BytesMarshaller<A> {
        INSTANCE;

        @Override
        public void write(Bytes out, A a) {
            out.writeUTF(a.str_);
            if (a.list_ != null) {
                int size = a.list_.size();
                out.writeStopBit(size);
                for (int i = 0; i < size; i++) {
                    a.list_.get(i).writeMarshallable(out);
                }
            } else {
                out.writeStopBit(-1);
            }
        }

        @Override
        public A read(Bytes in) {
            return read(in, null);
        }

        @Override
        public A read(Bytes in, A a) {
            if (a == null)
                a = new A();
            a.str_ = in.readUTF();
            int size = (int) in.readStopBit();
            if (size >= 0) {
                if (a.list_ == null) {
                    a.list_ = new ArrayList<>(size);
                } else {
                    a.list_.clear();
                    if (a.list_ instanceof ArrayList)
                        ((ArrayList) a.list_).ensureCapacity(size);
                }
                for (int i = 0; i < size; i++) {
                    B b = new B();
                    b.readMarshallable(in);
                    a.list_.add(b);
                }
            } else {
                assert size == -1;
                a.list_ = null;
            }
            return a;
        }
    }

    static class B implements BytesMarshallable {
        String str_;

        @Override public void readMarshallable(Bytes in) throws IllegalStateException {
            str_ = in.readUTF();
        }

        @Override public void writeMarshallable(Bytes out) {
            out.writeUTF(str_);
        }
    }

    @Test
    public void testComplexSerialization() throws Exception {
        try (ChronicleMap<String, A> map = ChronicleMapBuilder
                .of(String.class, A.class)
                .valueMarshaller(AMarshaller.INSTANCE)
                .entries(5)
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
}
