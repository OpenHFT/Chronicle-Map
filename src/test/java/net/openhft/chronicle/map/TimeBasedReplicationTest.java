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

import net.openhft.chronicle.hash.replication.TcpTransportAndNetworkConfig;
import net.openhft.chronicle.hash.replication.TimeProvider;
import net.openhft.chronicle.map.jsr166.JSR166TestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import static net.openhft.chronicle.set.Builder.waitTillEqual;
import static org.junit.Assert.assertEquals;

/**
 * @author Rob Austin.
 */
public class TimeBasedReplicationTest extends JSR166TestCase {

    public static final byte IDENTIFIER = 1;


    Set<Thread> threads;

    @Before
    public void sampleThreads() {
        threads = Thread.getAllStackTraces().keySet();
    }

    @After
    public void checkThreadsShutdown() {
        ChannelReplicationTest.checkThreadsShutdown(threads);
    }

    public void testIgnoreLateAction(BiConsumer<ChronicleMap<CharSequence, CharSequence>,
            ChronicleMap<CharSequence, CharSequence>> action)
            throws IOException, InterruptedException {

        final TimeProvider timeProvider = Mockito.mock(TimeProvider.class);
        ChronicleMapBuilder<CharSequence, CharSequence> builder = ChronicleMapBuilder
                .of(CharSequence.class, CharSequence.class)
                .averageKey("key-1")
                .averageValue("value-1")
                .entries(10)
                .cleanupRemovedEntries(false)
                .timeProvider(timeProvider);
        try (ChronicleMap<CharSequence, CharSequence> map = builder
                .replication((byte) 1, TcpTransportAndNetworkConfig.of(8086)).create()) {
            try (ChronicleMap<CharSequence, CharSequence> map2 = builder
                    .replication((byte) 2, TcpTransportAndNetworkConfig
                            .of(8087, new InetSocketAddress("localhost", 8086))).create()) {
                current(timeProvider);

                // we do a put at the current time
                map.put("key-1", "value-1");
                assertEquals(map.size(), 1);
                assertEquals(map.get("key-1"), "value-1");

                // now test assume that we receive a late update to the map,
                // the following update should be ignored
                late(timeProvider);

                action.accept(map, map2);

                // we'll now flip the time back to the current in order to do the read the result
                current(timeProvider);
                waitTillEqual(map, map2, 5000);
                assertEquals(map.size(), 1);
                // prove that late update to map2 ignored
                assertEquals("value-1", map.get("key-1"));
            }
        }
    }

    @Test
    public void testIgnoreALatePut() throws IOException, InterruptedException {
        testIgnoreLateAction((m1, m2) -> {
            m2.put("key-1", "value-2");
            assertEquals("value-2", m2.get("key-1"));
        });
    }

    @Test
    public void testIgnoreALatePutIfAbsent() throws IOException {

        final TimeProvider timeProvider = Mockito.mock(TimeProvider.class);
        try (ChronicleMap map = ChronicleMapBuilder.of(CharSequence.class, CharSequence.class)
                .averageKey("key-1").averageValue("value-1")
                .entries(10)
                .cleanupRemovedEntries(false)
                .timeProvider(timeProvider)
                .replication((byte) 1)
                .create()) {
            current(timeProvider);

            // we do a put at the current time
            map.put("key-1", "value-1");
            assertEquals(map.size(), 1);
            assertEquals(map.get("key-1"), "value-1");

            // now test assume that we receive a late update to the map, the following update
            // should be ignored
            late(timeProvider);

            // should NOT throw LateUpdateException because the value is present and it is just
            // returned.
            assertEquals("value-1", map.putIfAbsent("key-1", "value-2"));

            // we'll now flip the time back to the current in order to do the read the result
            current(timeProvider);
            assertEquals(1, map.size());
            assertEquals(map.get("key-1"), "value-1");
        }
    }

    @Test
    public void testIgnoreALateReplace() throws IOException, InterruptedException {
        testIgnoreLateAction((m1, m2) -> {
            // in order to be able to call replace() "locally"
            m2.put("key-1", "dummy");
            m2.replace("key-1", "value-2");
            assertEquals("value-2", m2.get("key-1"));
        });
    }

    @Test
    public void testIgnoreALateReplaceWithValue() throws IOException, InterruptedException {
        testIgnoreLateAction((m1, m2) -> {
            // in order to be able to call replace() "locally"
            m2.put("key-1", "dummy");
            m2.replace("key-1", "dummy", "value-2");
            assertEquals("value-2", m2.get("key-1"));
        });
    }

    @Test
    public void testIgnoreALateRemoveWithValue() throws IOException, InterruptedException {
        testIgnoreLateAction((m1, m2) -> {
            // in order to be able to call remove() "locally"
            m2.put("key-1", "dummy");
            m2.remove("key-1", "dummy");
            assertEquals(null, m2.get("key-1"));
        });
    }

    @Test
    public void testIgnoreALateRemove() throws IOException, InterruptedException {
        testIgnoreLateAction((m1, m2) -> {
            // in order to be able to call remove() "locally"
            m2.put("key-1", "dummy");
            m2.remove("key-1");
            assertEquals(null, m2.get("key-1"));
        });
    }

    @Test
    public void testRemoveFollowedByLatePut() throws IOException, InterruptedException {

        final TimeProvider timeProvider = Mockito.mock(TimeProvider.class);
        try (ChronicleMap<CharSequence, CharSequence> map =
                     ChronicleMapBuilder.of(CharSequence.class, CharSequence.class)
                             .averageKey("key-1").averageValue("value-1")
                             .entries(10)
                             .cleanupRemovedEntries(false)
                             .timeProvider(timeProvider)
                             .replication((byte) 1, TcpTransportAndNetworkConfig.of(8086))
                             .create()) {
            try (ChronicleMap<CharSequence, CharSequence> map2 =
                         ChronicleMapBuilder.of(CharSequence.class, CharSequence.class)
                                 .averageKey("key-1").averageValue("value-1")
                                 .entries(10)
                                 .cleanupRemovedEntries(false)
                                 .timeProvider(timeProvider)
                                 .replication((byte) 2, TcpTransportAndNetworkConfig.of(8087,
                                         new InetSocketAddress("localhost", 8086)))
                                 .create()) {
                current(timeProvider);

                // we do a put at the current time
                map.put("key-1", "value-1");
                map.remove("key-1");
                assertEquals(map.size(), 0);
                assertEquals(null, map.get("key-1"));

                // now test assume that we receive a late update to the map,
                // the following update should be ignored
                late(timeProvider);

                map2.put("key-1", "value-2");
                assertEquals("value-2", map2.get("key-1"));

                // we'll now flip the time back to the current in order to do the read the result
                current(timeProvider);
                waitTillEqual(map, map2, 5000);
                assertEquals("map: " + map + ", map2: " + map2, 0, map.size());
                // prove that late put was ignored
                assertEquals(null, map.get("key-1"));
            }
        }
    }

    private void current(TimeProvider timeProvider) {
        Mockito.when(timeProvider.currentTime()).thenReturn(System.currentTimeMillis());
    }

    private void late(TimeProvider timeProvider) {
        Mockito.when(timeProvider.currentTime())
                .thenReturn(System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(10));
    }
}
