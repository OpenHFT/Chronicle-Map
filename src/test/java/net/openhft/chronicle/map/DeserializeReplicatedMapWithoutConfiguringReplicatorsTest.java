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

package net.openhft.chronicle.map;

import com.google.common.collect.ImmutableMap;
import net.openhft.chronicle.hash.replication.TcpTransportAndNetworkConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class DeserializeReplicatedMapWithoutConfiguringReplicatorsTest {

    @Test
    public void deserializeReplicatedMapWithoutConfiguringReplicatorsTest()
            throws IOException, InterruptedException {

        File file1 = File.createTempFile("test", "cron");
        file1.deleteOnExit();
        try (ChronicleMap<Integer, Integer> map1 = ChronicleMapBuilder
                .of(Integer.class, Integer.class)
                .entries(100)
                .replication((byte) 1)
                .createPersistedTo(file1)) {
            map1.put(1, 1);
        }

        try (ChronicleMap<Integer, Integer> map1 = ChronicleMapBuilder
                .of(Integer.class, Integer.class)
                .entries(100)
//                .replication((byte) 1)
                .createPersistedTo(file1)) {
            map1.put(2, 2);
        }

        File file2 = File.createTempFile("test", "cron");
        file2.deleteOnExit();
        try (ChronicleMap<Integer, Integer> map2 = ChronicleMapBuilder
                .of(Integer.class, Integer.class)
                .entries(100)
                .replication((byte) 2)
                .createPersistedTo(file2)) {
            map2.put(3, 3);
        }

        // Let the subsequent remove from map2 be later than put in map1
        Thread.sleep(1);

        try (ChronicleMap<Integer, Integer> map2 = ChronicleMapBuilder
                .of(Integer.class, Integer.class)
                .entries(100)
//                .replication((byte) 2)
                .createPersistedTo(file2)) {
            map2.put(2, 0);
            map2.remove(2);
        }

        try (ChronicleMap<Integer, Integer> map1 = ChronicleMapBuilder
                .of(Integer.class, Integer.class)
                .entries(100)
                .replication((byte) 1, TcpTransportAndNetworkConfig
                        .of(10000, new InetSocketAddress("localhost", 10001)))
                .createPersistedTo(file1);
             ChronicleMap<Integer, Integer> map2 = ChronicleMapBuilder
                     .of(Integer.class, Integer.class)
                     .entries(100)
                     .replication((byte) 2, TcpTransportAndNetworkConfig
                             .of(10001, new InetSocketAddress("localhost", 10000)))
                     .createPersistedTo(file2)) {
            waitTillEqual(map1, map2, 3000);
            assertEquals(map1, map2);
            assertEquals(map1, ImmutableMap.of(1, 1, 3, 3));
            assertEquals(map2, ImmutableMap.of(1, 1, 3, 3));
        }
    }

    private static void waitTillEqual(
            ChronicleMap<Integer, Integer> map1,
            ChronicleMap<Integer, Integer> map2, final int timeOutMs) {
        int t = 0;
        for (; t < timeOutMs; t++) {
            if (map1.equals(map2))
                break;
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    Set<Thread> threads;

    @Before
    public void sampleThreads() {
        threads = Thread.getAllStackTraces().keySet();
    }

    @After
    public void checkThreadsShutdown() {
        StatelessClientTest.checkThreadsShutdown(threads);
    }
}
