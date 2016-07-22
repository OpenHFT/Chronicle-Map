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

package net.openhft.chronicle.set.replication;

import net.openhft.chronicle.hash.replication.RemoteOperationContext;
import net.openhft.chronicle.set.ChronicleSet;
import net.openhft.chronicle.set.SetEntryOperations;
import net.openhft.chronicle.set.SetQueryContext;
import org.jetbrains.annotations.Nullable;

/**
 * Context of remote {@link ChronicleSet} queries and internal replication operations.
 *
 * @param <K> the set key type
 * @param <R> the return type of {@link SetEntryOperations} specialized for the queried set
 * @see SetRemoteOperations
 */
public interface SetRemoteQueryContext<K, R>
        extends SetQueryContext<K, R>, RemoteOperationContext<K> {
    @Override
    @Nullable
    SetReplicableEntry<K> entry();
}
