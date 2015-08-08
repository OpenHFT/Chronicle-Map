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

package net.openhft.chronicle.set.replication;

import net.openhft.chronicle.hash.replication.ReplicableEntry;
import net.openhft.chronicle.set.ChronicleSet;
import net.openhft.chronicle.set.SetEntry;

/**
 * A context of a <i>present</i> entry in the replicated {@link ChronicleSet}.
 *
 * @param <K> the set key type
 * @see SetRemoteOperations
 * @see SetRemoteQueryContext
 */
public interface SetReplicableEntry<K> extends SetEntry<K>, ReplicableEntry {
}
