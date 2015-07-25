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

import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.chronicle.hash.serialization.internal.MetaBytesInterop;
import net.openhft.chronicle.hash.serialization.internal.MetaBytesWriter;
import net.openhft.chronicle.map.impl.stage.data.instance.WrappedValueInstanceData;
import net.openhft.lang.io.ByteBufferBytes;

import java.io.Serializable;
import java.nio.ByteBuffer;

public final class ConstantValueProvider<V> implements Serializable {
    private static final long serialVersionUID = 0L;

    private transient V value;
    private final int size;
    private final byte[] serializedValueBytes;

    <W> ConstantValueProvider(V value, MetaBytesWriter<V, W> metaValueWriter, W valueWriter) {
        this.value = value;
        size = (int) metaValueWriter.size(valueWriter, value);
        serializedValueBytes = new byte[size];
        ByteBufferBytes bytes = new ByteBufferBytes(ByteBuffer.wrap(serializedValueBytes));
        metaValueWriter.write(valueWriter, bytes, value);
    }

    boolean wasDeserialized() {
        return value == null;
    }

    void initTransients(BytesReader<V> reader) {
        value = reader.read(new ByteBufferBytes(ByteBuffer.wrap(serializedValueBytes)), size);
    }

    public V defaultValue() {
        return value;
    }
}
