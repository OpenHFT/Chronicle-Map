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

import java.io.Closeable;
import java.io.IOException;

abstract class Replicator {

    /**
     * A constructor for use in subclasses.
     */
    protected Replicator() {
    }

    /**
     * Applies the replicator to the map instance and returns a Closeable token to manage resources,
     * associated with the replication.  <p>This method isn't intended to be called from the client
     * code.
     *
     * @param builder             the builder from which the map was constructed. The replicator may
     *                            obtain some map configurations, not accessible via the map
     *                            instance.
     * @param map                 a replicated map instance. Provides basic tools for replication
     *                            implementation.
     * @param entryExternalizable the callback for ser/deser implementation in the replicator
     * @param chronicleMap        to wrap.
     * @return a {@code Closeable} token to control replication resources. It should be closed on
     * closing the replicated map.
     * @throws java.io.IOException   if an io error occurred during the replicator setup
     * @throws IllegalStateException if this replicator doesn't support application to more than one
     *                               map (or the specified number of maps), and this replicator has
     *                               already been applied to a map (or the specified number of
     *                               maps)
     */
    protected abstract Closeable applyTo(ChronicleMapBuilder builder,
                                         Replica map, Replica.EntryExternalizable entryExternalizable,
                                         final ChronicleMap chronicleMap) throws IOException;
}
