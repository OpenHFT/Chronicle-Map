/*
 * Copyright 2014 Higher Frequency Trading
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

package net.openhft.chronicle.map;

import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.serialization.BytesMarshallable;
import net.openhft.lang.model.constraints.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Collections;

public class SerializerTest {

    @Test
    public void testValueMarshallable() throws Exception {
        testReadWriteValue("Test");
        testReadWriteValue(1);
        testReadWriteValue(1L);
        testReadWriteValue(1.0);
        testReadWriteValue(1.0f);
        testReadWriteValue(Collections.singleton("Test"));
        testReadWriteValue(Collections.EMPTY_MAP);
        testReadWriteValue(new MyTestClass(3));
        testReadWriteValue(new MyTestClassMarshallable(3));

        testReadWriteValue(new MyTestClassExternalizable(3));
        testReadWriteValue(new MyTestClassObjectGraph(3));
    }


    @Test
    public void testKeyMarshallable() throws Exception {
        testReadWriteKey("Test");
        testReadWriteKey(1);
        testReadWriteKey(1L);
        testReadWriteKey(1.0);
        testReadWriteKey(1.0f);
        testReadWriteKey(Collections.singleton("Test"));
        testReadWriteKey(Collections.EMPTY_MAP);
        testReadWriteKey(new MyTestClass(3));
        testReadWriteKey(new MyTestClassMarshallable(3));

        testReadWriteKey(new MyTestClassExternalizable(3));
        testReadWriteKey(new MyTestClassObjectGraph(3));
    }


    public void testReadWriteValue(Object value) throws Exception {

        Class valueClass = value.getClass();

        final ByteBufferBytes out = new ByteBufferBytes(ByteBuffer.allocateDirect(1024));
        ByteBufferBytes in = out.slice();

        ChronicleMapBuilder builder = ChronicleMapBuilder.of(Integer.class, valueClass);
        builder.preMapConstruction();

        Serializer v = new Serializer(builder.valueBuilder);

        v.writeMarshallable(value, out, null);

        long position = out.position();
        in.limit(position);


        Object actual = v.readMarshallable(in, null);
        Assert.assertEquals(actual, value);
    }


    public void testReadWriteKey(Object key) throws Exception {

        Class clazz = key.getClass();

        final ByteBufferBytes out = new ByteBufferBytes(ByteBuffer.allocateDirect(1024));
        ByteBufferBytes in = out.slice();

        ChronicleMapBuilder builder = ChronicleMapBuilder.of(clazz, Integer.class);
        builder.preMapConstruction();
        {
            Serializer v = new Serializer(builder.keyBuilder);


            v.writeMarshallable(key, out, null);

            long position = out.position();
            in.limit(position);

            Object actual = v.readMarshallable(in, null);
            Assert.assertEquals(actual, key);

        }


    }

    public static class MyTestClassExternalizable implements Externalizable {
        int a;

        public MyTestClassExternalizable() {
        }


        MyTestClassExternalizable(int a) {
            this.a = a;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            MyTestClassExternalizable that = (MyTestClassExternalizable) o;

            if (a != that.a) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return a;
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            out.write(a);
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            a = in.readInt();
        }
    }

    public static class MyTestClass implements Serializable {
        int a;

        MyTestClass(int a) {
            this.a = a;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            MyTestClass myTestClass = (MyTestClass) o;

            if (a != myTestClass.a) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return a;
        }
    }

    public static class MyTestClassMarshallable implements BytesMarshallable {
        int a;

        public MyTestClassMarshallable() {
        }

        public MyTestClassMarshallable(int a) {
            this.a = a;
        }

        public void setA(int a) {
            this.a = a;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            MyTestClassMarshallable that = (MyTestClassMarshallable) o;

            if (a != that.a) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return a;
        }

        public void readMarshallable(@NotNull Bytes in) throws IllegalStateException {
            a = in.readInt();
        }

        /**
         * write an object to bytes
         *
         * @param out to write to
         */
        public void writeMarshallable(@NotNull Bytes out) {
            out.writeInt(a);
        }
    }

    public static class MyTestClassObjectGraph implements Serializable {

        MyTestClass delegate;

        public MyTestClassObjectGraph() {

        }

        MyTestClassObjectGraph(int a) {
            delegate = new MyTestClass(a);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            MyTestClassObjectGraph that = (MyTestClassObjectGraph) o;

            if (delegate != null ? !delegate.equals(that.delegate) : that.delegate != null)
                return false;

            return true;
        }

        @Override
        public int hashCode() {
            return delegate != null ? delegate.hashCode() : 0;
        }
    }


}