/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.map;

import net.openhft.chronicle.hash.serialization.BytesWriter;
import net.openhft.chronicle.hash.serialization.internal.MetaBytesWriter;
import net.openhft.lang.io.Bytes;

import java.io.Serializable;

final class PrepareValueBytesAsWriter<K>
        implements BytesWriter<K>, MetaBytesWriter<K, PrepareValueBytesAsWriter<K>>,
        Serializable {
    private static final long serialVersionUID = 0L;
    private final PrepareValueBytes<K, ?> prepareValueBytes;
    private final long valueSize;

    PrepareValueBytesAsWriter(PrepareValueBytes<K, ?> prepareValueBytes, long valueSize) {
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
