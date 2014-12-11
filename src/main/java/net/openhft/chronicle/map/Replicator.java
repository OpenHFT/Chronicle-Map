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
