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

package net.openhft.chronicle.map.impl.stage.map;

import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.impl.stage.hash.CheckOnEachPublicOperation;
import net.openhft.chronicle.hash.impl.util.Objects;
import net.openhft.chronicle.map.MapContext;
import net.openhft.chronicle.map.impl.stage.data.bytes.WrappedValueBytesData;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

@Staged
public abstract class WrappedValueBytesDataAccess<K, V, R> implements MapContext<K, V, R> {

    @StageRef
    CheckOnEachPublicOperation checkOnEachPublicOperation;
    @StageRef
    WrappedValueBytesData<V> wrappedValueBytesData;

    @Override
    public Data<V> wrapValueBytesAsData(BytesStore bytesStore, long offset, long size) {
        Objects.requireNonNull(bytesStore);
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        WrappedValueBytesData<V> wrapped = this.wrappedValueBytesData;
        wrapped = wrapped.getUnusedWrappedValueBytesData();
        wrapped.initWrappedValueBytesStore(bytesStore, offset, size);
        return wrapped;
    }
}
