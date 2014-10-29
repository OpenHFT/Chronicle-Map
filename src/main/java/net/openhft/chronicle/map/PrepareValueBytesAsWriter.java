/*
 * Copyright 2014 Higher Frequency Trading http://www.higherfrequencytrading.com
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

import net.openhft.chronicle.hash.serialization.BytesWriter;
import net.openhft.chronicle.hash.serialization.MetaBytesWriter;
import net.openhft.lang.io.Bytes;

import java.io.Serializable;

final class PrepareValueBytesAsWriter<K>
        implements BytesWriter<K>, MetaBytesWriter<K, PrepareValueBytesAsWriter<K>>,
        Serializable {
    private static final long serialVersionUID = 0L;
    private final PrepareValueBytes<K> prepareValueBytes;
    private final long valueSize;

    PrepareValueBytesAsWriter(PrepareValueBytes<K> prepareValueBytes, long valueSize) {
        this.prepareValueBytes = prepareValueBytes;
        this.valueSize = valueSize;
    }

    @Override
    public long size(K k) {
        return valueSize;
    }

    @Override
    public void write(Bytes bytes, K k) {
        prepareValueBytes.prepare(bytes, k);
    }

    @Override
    public long size(PrepareValueBytesAsWriter<K> writer, K k) {
        return size(k);
    }

    @Override
    public void write(PrepareValueBytesAsWriter<K> writer, Bytes bytes, K k) {
        write(bytes, k);
    }
}
