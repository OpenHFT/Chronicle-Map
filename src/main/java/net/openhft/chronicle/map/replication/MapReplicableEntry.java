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

package net.openhft.chronicle.map.replication;

import net.openhft.chronicle.hash.replication.ReplicableEntry;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.MapEntry;

/**
 * A context of a <i>present</i> entry in the replicated {@link ChronicleMap}.
 *
 * @param <K> the map key type
 * @param <V> the map value type
 * @see MapRemoteOperations
 * @see MapRemoteQueryContext
 */
public interface MapReplicableEntry<K, V> extends MapEntry<K, V>, ReplicableEntry {
}
