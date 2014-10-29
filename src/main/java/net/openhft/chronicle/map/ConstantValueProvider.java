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

import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.chronicle.hash.serialization.MetaBytesWriter;
import net.openhft.lang.io.ByteBufferBytes;

import java.nio.ByteBuffer;

final class ConstantValueProvider<K, V> implements DefaultValueProvider<K, V> {
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

    @Override
    public V get(K key) {
        return value;
    }
}
