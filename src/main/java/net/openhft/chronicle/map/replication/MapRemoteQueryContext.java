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

package net.openhft.chronicle.map.replication;

import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.replication.RemoteOperationContext;
import net.openhft.chronicle.hash.serialization.SizeMarshaller;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import net.openhft.chronicle.map.MapEntryOperations;
import net.openhft.chronicle.map.MapQueryContext;
import org.jetbrains.annotations.Nullable;

/**
 * Context of remote {@link ChronicleMap} queries and internal replication operations.
 *
 * @param <K> the map key type
 * @param <V> the map value type
 * @param <R> the return type of {@link MapEntryOperations} specified for the queried map
 * @see MapRemoteOperations
 */
public interface MapRemoteQueryContext<K, V, R> extends MapQueryContext<K, V, R>,
        RemoteOperationContext<K> {
    @Nullable
    @Override
    MapReplicableEntry<K, V> entry();

    /**
     * The value used as a tombstone, for removed entries, to save space. This value has {@linkplain
     * SizeMarshaller#minStorableSize()} minimum possible size} for {@linkplain
     * ChronicleMapBuilder#valueSizeMarshaller(SizeMarshaller) the configured value size
     * marshaller}.
     * <p>
     * <p>The returned value doesn't have object form (i. e. it's {@link Data#get()} and {@link
     * Data#getUsing(Object)} methods throw {@code UnsupportedOperationException}), it has only
     * bytes form, of all zero bytes.
     *
     * @return dummy value of all zeros, of the minimum possible size.
     */
    Data<V> dummyZeroValue();
}
