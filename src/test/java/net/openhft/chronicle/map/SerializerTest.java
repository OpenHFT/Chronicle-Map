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

import net.openhft.chronicle.hash.serialization.internal.ReaderWithSize;
import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.serialization.BytesMarshallable;
import net.openhft.lang.model.constraints.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;

public class SerializerTest {

    @Test
    public void testPrefixStringFunctionSerialization() {

        ByteBufferBytes b = new ByteBufferBytes(ByteBuffer.allocate(512));

        CHMUseCasesTest.PrefixStringFunction expected = new CHMUseCasesTest.PrefixStringFunction("New ");
        b.writeObject(expected);

        b.clear();
        CHMUseCasesTest.PrefixStringFunction actual = b.readObject(CHMUseCasesTest.PrefixStringFunction.class);
        assertEquals(expected, actual);

    }

    @Test
    public void testStringSerialization() throws ExecutionException,
            InterruptedException, IOException {

        ByteBufferBytes b = new ByteBufferBytes(ByteBuffer.allocate(512));

        String expected = "Hello";
        b.writeObject(expected);

        b.clear();

        String actual = (String) b.readObject();
        assertEquals(expected, actual);

    }


    @Test
    public void testValueMarshallable() {
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
    public void testKeyMarshallable() {
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

    public void testReadWriteValue(Object value) {

        Class valueClass = value.getClass();

        final ByteBufferBytes out = new ByteBufferBytes(ByteBuffer.allocateDirect(1024));
        ByteBufferBytes in = out.slice();

        ChronicleMapBuilder builder =
                ChronicleMapBuilder.of(Integer.class, valueClass);

        builder.preMapConstruction(false);

        WriterWithSize valueWriterWithSize = new WriterWithSize(builder.valueBuilder,null);
        ReaderWithSize valueReaderWithSize = new ReaderWithSize(builder.valueBuilder);

        valueWriterWithSize.write(out, value, null);

        long position = out.position();
        in.limit(position);

        Object actual = valueReaderWithSize.read(in, null, null);
        Assert.assertEquals(actual, value);
    }

    public void testReadWriteKey(Object key) {

        Class clazz = key.getClass();

        final ByteBufferBytes out = new ByteBufferBytes(ByteBuffer.allocateDirect(1024));
        ByteBufferBytes in = out.slice();

        ChronicleMapBuilder builder = ChronicleMapBuilder.of(clazz, Integer.class);

        builder.preMapConstruction(false);
        {
            WriterWithSize keyWriterWithSize = new WriterWithSize(builder.keyBuilder, null);
            ReaderWithSize keyReaderWithSize = new ReaderWithSize(builder.keyBuilder);

            keyWriterWithSize.write(out, key, null);

            long position = out.position();
            in.limit(position);

            Object actual = keyReaderWithSize.read(in, null, null);
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
            if (o == null || MyTestClassMarshallable.class != o.getClass()) return false;

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