/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import net.openhft.chronicle.hash.serialization.DataAccess;
import net.openhft.chronicle.hash.serialization.impl.ExternalizableDataAccess;
import net.openhft.chronicle.hash.serialization.impl.ExternalizableReader;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SuppressWarnings({"rawtypes", "unchecked", "serial"})
public class ConstantSizeBySampleTest {

    @Test
    public void testConstantKeys() {
        try (ChronicleMap<byte[], Long> map = ChronicleMapBuilder.of(byte[].class, Long.class)
                .constantKeySizeBySample(new byte[8])
                .entries(100)
                .create()) {

            byte[] zero = ByteBuffer.allocate(8).putLong(0L).array();
            map.put(zero, 0L);
            assertEquals(0L, (long) map.get(zero), "(long) map.get(zero)");

            byte[] one = ByteBuffer.allocate(8).putLong(1L).array();
            map.put(one, 1L);
            assertEquals(1L, (long) map.get(one), "(long) map.get(one)");

            map.put(one, 0L);
            assertEquals(0L, (long) map.get(one), "(long) map.get(one)");
        }
    }

    @Test
    public void testUnexpectedlyLongConstantByteArrayValues() {
        try (ChronicleMap<Long, byte[]> map = ChronicleMapBuilder.of(Long.class, byte[].class)
                .constantValueSizeBySample(new byte[512 * 1024])
                .entries(100)
                .actualSegments(1)
                .create()) {
            byte[] value = new byte[512 * 1024];

            value[42] = 1;
            map.put(1L, value);
            Assertions.assertArrayEquals(map.get(1L), value, "large constant-size byte array should be stored and retrieved correctly");
        }
    }

    @Test
    public void testUnexpectedlyLongConstantExternalizableValues() {
        try (ChronicleMap<Long, ExternalizableData> map =
                     ChronicleMapBuilder.of(Long.class, ExternalizableData.class)
                             .valueReaderAndDataAccess(new ExternalizableDataReader(),
                                     new ExternalizableDataDataAccess())
                             .constantValueSizeBySample(new ExternalizableData())
                             .entries(100)
                             .actualSegments(1)
                             .create()) {
            ExternalizableData value = new ExternalizableData();
            value.data[42] = 1;
            map.put(1L, value);
            Assertions.assertEquals(map.get(1L), value, "large constant-size externalizable object should be stored and retrieved correctly");
        }
    }

    @Test
    public void testUnexpectedlyLongConstantSerializableValues() {
        try (ChronicleMap<Long, SerializableData> map =
                     ChronicleMapBuilder.of(Long.class, SerializableData.class)
                             .constantValueSizeBySample(new SerializableData())
                             .entries(100)
                             .actualSegments(1)
                             .create()) {
            SerializableData value = new SerializableData();
            value.data[42] = 1;
            map.put(1L, value);
            Assertions.assertEquals(map.get(1L), value, "large constant-size serializable object should be stored and retrieved correctly");
        }
    }

    static final class ExternalizableData implements Externalizable {
        byte[] data = new byte[512 * 1024];

        public ExternalizableData() {
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof ExternalizableData))
                return false;
            return Arrays.equals(((ExternalizableData) obj).data, data);
        }

        @Override
        public int hashCode() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            out.write(data);
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException {
            in.read(data = new byte[512 * 1024]);
        }
    }

    static final class SerializableData implements Serializable {
        final byte[] data = new byte[512 * 1024];

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof SerializableData))
                return false;
            return Arrays.equals(((SerializableData) obj).data, data);
        }

        @Override
        public int hashCode() {
            throw new UnsupportedOperationException();
        }
    }

    private static final class ExternalizableDataDataAccess
            extends ExternalizableDataAccess<ExternalizableData> implements Serializable {
        public ExternalizableDataDataAccess() {
            super(ExternalizableData.class);
        }

        @Override
        protected ExternalizableData createInstance() {
            return new ExternalizableData();
        }

        @Override
        public DataAccess<ExternalizableData> copy() {
            return new ExternalizableDataDataAccess();
        }
    }

    private static final class ExternalizableDataReader extends ExternalizableReader<ExternalizableData> {
        public ExternalizableDataReader() {
            super(ExternalizableData.class);
        }

        @Override
        protected ExternalizableData createInstance() {
            return new ExternalizableData();
        }
    }
}
