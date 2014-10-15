package net.openhft.chronicle.map;

import junit.framework.TestCase;
import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.serialization.BytesMarshallable;
import net.openhft.lang.model.constraints.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Collections;

public class StatelessMapClientTest extends TestCase {


    @Test
    public void testReadValueWriteIntegerValue() throws Exception {
        testReadValueWriteValue("Test");
        testReadValueWriteValue(1);
        testReadValueWriteValue(1L);
        testReadValueWriteValue(1.0);
        testReadValueWriteValue(1.0f);
        testReadValueWriteValue(Collections.singleton("Test"));
        testReadValueWriteValue(Collections.EMPTY_MAP);
        testReadValueWriteValue(new MyTestClass(3));
      testReadValueWriteValue(new MyTestClassMarshallable(3));

      testReadValueWriteValue(new MyTestClassExternalizable(3));
 testReadValueWriteValue(new MyTestClassObjectGraph(3));


    }

    public void testReadValueWriteValue(Object value) throws Exception {

        Class valueClass = value.getClass();

        final ByteBufferBytes buffer = new ByteBufferBytes(ByteBuffer.allocateDirect(1024));
        ByteBufferBytes slice = buffer.slice();

        ChronicleMapBuilder builder = ChronicleMapBuilder.of(Integer.class, valueClass);
        builder.preMapConstruction();

        StatelessMapClient statelessMap = new StatelessMapClient(buffer, builder);

        statelessMap.writeValue(value);
        long position = buffer.position();
        slice.limit(position);

        Object actual = statelessMap.readValue(slice);
        Assert.assertEquals(actual, value);
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