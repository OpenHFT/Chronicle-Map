/*
 *      Copyright (C) 2012, 2016  higherfrequencytrading.com
 *      Copyright (C) 2016 Roman Leventov
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

import net.openhft.chronicle.hash.serialization.DataAccess;
import net.openhft.chronicle.hash.serialization.impl.ExternalizableDataAccess;
import net.openhft.chronicle.hash.serialization.impl.ExternalizableReader;
import org.junit.Assert;
import org.junit.Test;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class ConstantSizeBySampleTest {

    @Test
    public void testConstantKeys() throws IOException {
        ChronicleMap<byte[], Long> map = ChronicleMapBuilder.of(byte[].class, Long.class)
                .constantKeySizeBySample(new byte[8])
                .entries(100)
                .create();

        byte[] zero = ByteBuffer.allocate(8).putLong(0L).array();
        map.put(zero, 0L);
        assertEquals(0L, (long) map.get(zero));

        byte[] one = ByteBuffer.allocate(8).putLong(1L).array();
        map.put(one, 1L);
        assertEquals(1L, (long) map.get(one));

        map.put(one, 0L);
        assertEquals(0L, (long) map.get(one));
    }

    @Test
    public void testUnexpectedlyLongConstantByteArrayValues() throws IOException {
        try (ChronicleMap<Long, byte[]> map = ChronicleMapBuilder.of(Long.class, byte[].class)
                .constantValueSizeBySample(new byte[512 * 1024])
                .entries(100)
                .actualSegments(1)
                .create()) {
            byte[] value = new byte[512 * 1024];

            value[42] = 1;
            map.put(1L, value);
            Assert.assertTrue(Arrays.equals(map.get(1L), value));
        }
    }

    @Test
    public void testUnexpectedlyLongConstantExternalizableValues() throws IOException {
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
            Assert.assertEquals(map.get(1L), value);
        }
    }

    @Test
    public void testUnexpectedlyLongConstantSerializableValues() throws IOException {
        try (ChronicleMap<Long, SerializableData> map =
                     ChronicleMapBuilder.of(Long.class, SerializableData.class)
                             .constantValueSizeBySample(new SerializableData())
                             .entries(100)
                             .actualSegments(1)
                             .create()) {
            SerializableData value = new SerializableData();
            value.data[42] = 1;
            map.put(1L, value);
            Assert.assertEquals(map.get(1L), value);
        }
    }

    static class ExternalizableData implements Externalizable {
        byte[] data = new byte[512 * 1024];

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof ExternalizableData))
                return false;
            return Arrays.equals(((ExternalizableData) obj).data, data);
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            out.write(data);
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            in.read(data = new byte[512 * 1024]);
        }
    }

    static class SerializableData implements Serializable {
        byte[] data = new byte[512 * 1024];

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof SerializableData))
                return false;
            return Arrays.equals(((SerializableData) obj).data, data);
        }
    }

    private static class ExternalizableDataDataAccess
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

    private static class ExternalizableDataReader extends ExternalizableReader<ExternalizableData> {
        public ExternalizableDataReader() {
            super(ExternalizableData.class);
        }

        @Override
        protected ExternalizableData createInstance() {
            return new ExternalizableData();
        }
    }
}
