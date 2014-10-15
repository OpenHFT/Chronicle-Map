package net.openhft.chronicle.map;

import junit.framework.TestCase;
import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.serialization.BytesMarshallable;
import net.openhft.lang.model.constraints.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.io.Serializable;
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


}