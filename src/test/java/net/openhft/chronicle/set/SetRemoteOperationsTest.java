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

package net.openhft.chronicle.set;

import net.openhft.chronicle.hash.replication.ReplicableEntry;
import net.openhft.chronicle.hash.replication.TcpTransportAndNetworkConfig;
import net.openhft.chronicle.map.ChannelReplicationTest;
import net.openhft.chronicle.set.replication.SetRemoteOperations;
import net.openhft.chronicle.set.replication.SetRemoteQueryContext;
import net.openhft.chronicle.set.replication.SetReplicableEntry;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static net.openhft.chronicle.hash.replication.DefaultEventualConsistencyStrategy.AcceptanceDecision.ACCEPT;
import static net.openhft.chronicle.hash.replication.DefaultEventualConsistencyStrategy.AcceptanceDecision.DISCARD;
import static net.openhft.chronicle.hash.replication.DefaultEventualConsistencyStrategy.decideOnRemoteModification;
import static org.junit.Assert.assertEquals;

public class SetRemoteOperationsTest {

    static int s_port = 12050;
    private ChronicleSet<Integer> set1;
    private ChronicleSet<Integer> set2;
    private AtomicInteger set1PutCounter;
    private AtomicInteger set2PutCounter;
    private AtomicInteger set1RemoveCounter;
    private AtomicInteger set2RemoveCounter;

    @Before
    public void setup() throws IOException {
        int port = s_port;
        set1PutCounter = new AtomicInteger();
        set1RemoveCounter = new AtomicInteger();
        ChronicleSetBuilder<Integer> set1Builder = ChronicleSet
                .of(Integer.class)
                .remoteOperations(new SetRemoteOperations<Integer, Void>() {
                    @Override
                    public void remove(SetRemoteQueryContext<Integer, Void> q) {
                        SetReplicableEntry<Integer> entry = q.entry();
                        if (entry != null) {
                            if (decideOnRemoteModification(entry, q) == ACCEPT) {
                                q.remove(entry);
                                ReplicableEntry replicableAbsentEntry =
                                        (ReplicableEntry) q.absentEntry();
                                replicableAbsentEntry
                                        .updateOrigin(q.remoteIdentifier(), q.remoteTimestamp());
                                replicableAbsentEntry.dropChanged();
                                set1RemoveCounter.addAndGet(1);
                            }
                        } else {
                            SetAbsentEntry<Integer> absentEntry = q.absentEntry();
                            ReplicableEntry replicableAbsentEntry;
                            if (!(absentEntry instanceof ReplicableEntry)) {
                                absentEntry.doInsert();
                                q.entry().doRemove();
                                replicableAbsentEntry = (ReplicableEntry) q.absentEntry();
                            } else {
                                replicableAbsentEntry = (ReplicableEntry) absentEntry;
                                if (decideOnRemoteModification(replicableAbsentEntry, q) == DISCARD)
                                    return;
                            }
                            replicableAbsentEntry
                                    .updateOrigin(q.remoteIdentifier(), q.remoteTimestamp());
                            replicableAbsentEntry.dropChanged();
                            set1RemoveCounter.addAndGet(1);
                        }
                    }

                    @Override
                    public void put(SetRemoteQueryContext<Integer, Void> q) {
                        SetReplicableEntry<Integer> entry = q.entry();
                        if (entry != null) {
                            if (decideOnRemoteModification(entry, q) == ACCEPT) {
                                entry.updateOrigin(q.remoteIdentifier(), q.remoteTimestamp());
                                entry.dropChanged();
                                set1PutCounter.addAndGet(1);
                            }
                        } else {
                            SetAbsentEntry<Integer> absentEntry = q.absentEntry();
                            assert absentEntry != null;
                            if (!(absentEntry instanceof ReplicableEntry) ||
                                    decideOnRemoteModification((ReplicableEntry) absentEntry, q) ==
                                            ACCEPT) {
                                q.insert(absentEntry);
                                entry = q.entry(); // q.entry() is not null after insert
                                assert entry != null;
                                entry.updateOrigin(q.remoteIdentifier(), q.remoteTimestamp());
                                entry.dropChanged();
                                set1PutCounter.addAndGet(1);
                            }
                        }
                    }
                })
                .replication((byte) 1, TcpTransportAndNetworkConfig
                        .of(port, new InetSocketAddress("localhost", port + 1)));
        set1 = set1Builder.entries(Builder.SIZE).create();

        set2PutCounter = new AtomicInteger();
        set2RemoveCounter = new AtomicInteger();
        ChronicleSetBuilder<Integer> set2Builder = ChronicleSet
                .of(Integer.class)
                .remoteOperations(new SetRemoteOperations<Integer, Void>() {
                    @Override
                    public void remove(SetRemoteQueryContext<Integer, Void> q) {
                        SetReplicableEntry<Integer> entry = q.entry();
                        if (entry != null) {
                            if (decideOnRemoteModification(entry, q) == ACCEPT) {
                                q.remove(entry);
                                ReplicableEntry replicableAbsentEntry =
                                        (ReplicableEntry) q.absentEntry();
                                replicableAbsentEntry
                                        .updateOrigin(q.remoteIdentifier(), q.remoteTimestamp());
                                replicableAbsentEntry.dropChanged();
                                set2RemoveCounter.addAndGet(1);
                            }
                        } else {
                            SetAbsentEntry<Integer> absentEntry = q.absentEntry();
                            ReplicableEntry replicableAbsentEntry;
                            if (!(absentEntry instanceof ReplicableEntry)) {
                                absentEntry.doInsert();
                                q.entry().doRemove();
                                replicableAbsentEntry = (ReplicableEntry) q.absentEntry();
                            } else {
                                replicableAbsentEntry = (ReplicableEntry) absentEntry;
                                if (decideOnRemoteModification(replicableAbsentEntry, q) == DISCARD)
                                    return;
                            }
                            replicableAbsentEntry
                                    .updateOrigin(q.remoteIdentifier(), q.remoteTimestamp());
                            replicableAbsentEntry.dropChanged();
                            set2RemoveCounter.addAndGet(1);
                        }
                    }

                    @Override
                    public void put(SetRemoteQueryContext<Integer, Void> q) {
                        SetReplicableEntry<Integer> entry = q.entry();
                        if (entry != null) {
                            if (decideOnRemoteModification(entry, q) == ACCEPT) {
                                entry.updateOrigin(q.remoteIdentifier(), q.remoteTimestamp());
                                entry.dropChanged();
                                set2PutCounter.addAndGet(1);
                            }
                        } else {
                            SetAbsentEntry<Integer> absentEntry = q.absentEntry();
                            assert absentEntry != null;
                            if (!(absentEntry instanceof ReplicableEntry) ||
                                    decideOnRemoteModification((ReplicableEntry) absentEntry, q) ==
                                            ACCEPT) {
                                q.insert(absentEntry);
                                entry = q.entry(); // q.entry() is not null after insert
                                assert entry != null;
                                entry.updateOrigin(q.remoteIdentifier(), q.remoteTimestamp());
                                entry.dropChanged();
                                set2PutCounter.addAndGet(1);
                            }
                        }
                    }
                })
                .replication((byte) 2, TcpTransportAndNetworkConfig.of(port + 1));
        set2 = set2Builder.entries(Builder.SIZE).create();
        s_port += 2;
    }

    @After
    public void tearDown() throws InterruptedException {

        for (final Closeable closeable : new Closeable[]{set1, set2}) {
            try {
                closeable.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        System.gc();
    }

    Set<Thread> threads;

    @Before
    public void sampleThreads() {
        threads = Thread.getAllStackTraces().keySet();
    }

    @After
    public void checkThreadsShutdown() {
        ChannelReplicationTest.checkThreadsShutdown(threads);
    }

    @Test
    public void setRemoteOperationsTest() throws InterruptedException {
        set1.add(1);
        set2.add(2);

        waitTillEqual(5000);
        assertEquals(set1, set2);

        set1.remove(2);
        set2.remove(1);

        waitTillEqual(5000);
        assertEquals(set1, set2);

        assertEquals(1, set1PutCounter.get());
        assertEquals(1, set2PutCounter.get());
        assertEquals(1, set1RemoveCounter.get());
        assertEquals(1, set2RemoveCounter.get());
    }

    private void waitTillEqual(final int timeOutMs) throws InterruptedException {
        int t = 0;
        for (; t < timeOutMs; t++) {
            if (set1.equals(set2))
                break;
            Thread.sleep(1);
        }
    }
}
