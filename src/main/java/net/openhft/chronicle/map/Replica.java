/*
 * Copyright 2014 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
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

import net.openhft.lang.io.Bytes;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;

/**
 * @author Rob Austin.
 */
public interface Replica extends Closeable {

    /**
     * Provides the unique Identifier associated with this map instance. <p> An identifier is used to
     * determine which replicating node made the change. <p> If two nodes update their map at the same time
     * with different values, we have to deterministically resolve which update wins, because of eventual
     * consistency both nodes should end up locally holding the same data. Although it is rare two remote
     * nodes could receive an update to their maps at exactly the same time for the same key, we have to
     * handle this edge case, its therefore important not to rely on timestamps alone to reconcile the
     * updates. Typically the update with the newest timestamp should win,  but in this example both
     * timestamps are the same, and the decision made to one node should be identical to the decision made to
     * the other. We resolve this simple dilemma by using a node identifier, each node will have a unique
     * identifier, the update from the node with the smallest identifier wins.
     *
     * @return the unique Identifier associated with this map instance
     */
    byte identifier();

    /**
     * Gets (if it does not exist, creates) an instance of ModificationIterator associated with a remote node,
     * this weak associated is bound using the {@code identifier}.
     *
     * @param remoteIdentifier     the identifier of the remote node
     * @param modificationNotifier called when ever there is a change applied to the modification iterator
     * @return the ModificationIterator dedicated for replication to the remote node with the given identifier
     * @see #identifier()
     */
    ModificationIterator acquireModificationIterator(short remoteIdentifier,
                                                     ModificationNotifier modificationNotifier);

    /**
     * Returns the timestamp of the last change from the specified remote node, already replicated to this
     * Replica.  <p>Used in conjunction with replication, to back fill data from a remote node. This node
     * may have missed updates while it was not been running or connected via TCP.
     *
     * @param remoteIdentifier the identifier of the remote node to check last replicated update time from
     * @return a timestamp of the last modification to an entry, or 0 if there are no entries.
     * @see #identifier()
     */
    long lastModificationTime(byte remoteIdentifier);


    /**
     * notifies when there is a changed to the modification iterator
     */
    interface ModificationNotifier {
        public static final ModificationNotifier NOP = new ModificationNotifier() {
            @Override
            public void onChange() {
            }
        };

        /**
         * called when ever there is a change applied to the modification iterator
         */
        void onChange();
    }


    /**
     * Holds a record of which entries have modification. Each remote map supported will require a
     * corresponding ModificationIterator instance
     */
    interface ModificationIterator {

        /**
         * @return {@code true} if the is another entry to be received via {@link
         * #nextEntry(Replica.EntryCallback, int chronicleId)}
         */
        boolean hasNext();

        /**
         * A non-blocking call that provides the entry that has changed to {@code callback.onEntry()}.
         *
         * @param callback    a callback which will be called when a new entry becomes available.
         * @param chronicleId only assigned when using chronicle channels
         * @return {@code true} if the entry was accepted by the {@code callback.onEntry()} method, {@code
         * false} if the entry was not accepted or was not available
         */
        boolean nextEntry(@NotNull final EntryCallback callback, final int chronicleId);

        /**
         * Dirties all entries with a modification time equal to {@code fromTimeStamp} or newer. It means all
         * these entries will be considered as "new" by this ModificationIterator and iterated once again no
         * matter if they have already been.  <p>This functionality is used to publish recently modified
         * entries to a new remote node as it connects.
         *
         * @param fromTimeStamp the timestamp from which all entries should be dirty
         */
        void dirtyEntries(long fromTimeStamp);
    }


    /**
     * supports reading and writing serialize entries
     */
    interface EntryExternalizable {

        /**
         * The map implements this method to save its contents.
         *
         * @param entry       the byte location of the entry to be stored
         * @param destination a buffer the entry will be written to, the segment may reject this operation and
         *                    add zeroBytes, if the identifier in the entry did not match the maps local
         * @param chronicleId is the channel id used to identify the canonical map or queue
         */
        void writeExternalEntry(@NotNull Bytes entry, @NotNull Bytes destination, int chronicleId);

        /**
         * The map implements this method to restore its contents. This method must read the values in the
         * same sequence and with the same types as were written by {@code writeExternalEntry()}. This method
         * is typically called when we receive a remote replication event, this event could originate from
         * either a remote {@code put(K key, V value)} or {@code remove(Object key)}
         *
         * @param source bytes to read an entry from
         */
        void readExternalEntry(@NotNull Bytes source);

    }


    /**
     * provides a key and value from NativeBytes, this can be used in conjunction with the modification
     * iterator to get the key and value out of the NativeBytes
     */
    interface EntryResolver<K, V> {

        /**
         * gets the key from the entry
         *
         * @param entry    the bytes which the bytes which point to the entry
         * @param usingKey the key object to reuse, if possible
         * @return the key which is in the entry
         */
        K key(@NotNull Bytes entry, K usingKey);


        /**
         * gets the value from the entry
         *
         * @param entry      the bytes which reference to the entry
         * @param usingValue the value object to reuse, if possible
         * @return the value which is in the entry or null if the value has been remove from the map
         */
        V value(@NotNull Bytes entry, V usingValue);

        boolean wasRemoved(@NotNull Bytes entry);

    }

    /**
     * Implemented typically by a replicator, This interface provides the event, which will get called
     * whenever a put() or remove() has occurred to the map
     */
    abstract class EntryCallback {

        /**
         * Called whenever a put() or remove() has occurred to a replicating map.
         *
         * @param entry       the entry you will receive, this does not have to be locked, as locking is
         *                    already provided from the caller.
         * @param chronicleId only assigned when clustering
         * @return {@code false} if this entry should be ignored because the identifier of the source node is
         * not from one of our changes, WARNING even though we check the identifier in the
         * ModificationIterator the entry may have been updated.
         */
        public abstract boolean onEntry(final Bytes entry, final int chronicleId);

        /**
         * Called just after {@link #onEntry(Bytes, int)}. No-op by default.
         */
        public void onAfterEntry() {
        }

        /**
         * Called just before {@link #onEntry(Bytes, int)}. No-op by default.
         */
        public void onBeforeEntry() {
        }
    }
}

