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

package net.openhft.chronicle.map.impl;

import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.serialization.DataAccess;
import net.openhft.chronicle.map.ExternalMapQueryContext;
import net.openhft.chronicle.map.impl.ret.InstanceReturnValue;
import net.openhft.chronicle.map.impl.ret.UsableReturnValue;

public interface QueryContextInterface<K, V, R> extends ExternalMapQueryContext<K, V, R> {

    void initInputKey(Data<K> inputKey);

    Data<K> getInputKeyBytesAsData(BytesStore bytesStore, long offset, long size);

    DataAccess<K> inputKeyDataAccess();

    InstanceReturnValue<V> defaultReturnValue();

    UsableReturnValue<V> usingReturnValue();

    DataAccess<V> inputValueDataAccess();

    Closeable acquireHandle();

    void initSegmentIndex(int segmentIndex);

    boolean segmentIndexInit();
}
