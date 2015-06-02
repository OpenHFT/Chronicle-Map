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

import net.openhft.chronicle.hash.replication.TcpTransportAndNetworkConfig;
import net.openhft.chronicle.hash.replication.TimeProvider;
import net.openhft.chronicle.map.jsr166.JSR166TestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import static net.openhft.chronicle.map.Builder.waitTillEqual;
import static org.junit.Assert.assertEquals;

/**
 * @author Rob Austin.
 */
public class TimeBasedReplicationTest extends JSR166TestCase {

    public static final byte IDENTIFIER = 1;

    private static File getPersistenceFile() {
        String TMP = System.getProperty("java.io.tmpdir");
        File file = new File(TMP + "/chm-test" + System.nanoTime());
        file.deleteOnExit();
        return file;
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

    public void testIgnoreLateAction(BiConsumer<ChronicleMap<CharSequence, CharSequence>,
            ChronicleMap<CharSequence, CharSequence>> action)
            throws IOException {

        final TimeProvider timeProvider = Mockito.mock(TimeProvider.class);
        try (ChronicleMap<CharSequence, CharSequence> map =
                     ChronicleMapBuilder.of(CharSequence.class, CharSequence.class)
                             .entries(10)
                             .timeProvider(timeProvider)
                             .replication((byte) 1, TcpTransportAndNetworkConfig.of(8086))
                             .create()) {
            try (ChronicleMap<CharSequence, CharSequence> map2 =
                         ChronicleMapBuilder.of(CharSequence.class, CharSequence.class)
                                 .entries(10)
                                 .timeProvider(timeProvider)
                                 .replication((byte) 2, TcpTransportAndNetworkConfig.of(8087,
                                         new InetSocketAddress("localhost", 8086)))
                                 .create()) {
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
                assertEquals(map.get("key-1"), "value-1");
            }
        }
    }

    @Test
    public void testIgnoreALatePut() throws IOException {
        testIgnoreLateAction((m1, m2) -> {
            m2.put("key-1", "value-2");
            assertEquals("value-2", m2.get("key-1"));
        });
    }

    @Test
    public void testIgnoreALatePutIfAbsent() throws IOException {

        final TimeProvider timeProvider = Mockito.mock(TimeProvider.class);
        try (ChronicleMap map = ChronicleMapBuilder.of(CharSequence.class, CharSequence.class)
                .entries(10)
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
    public void testIgnoreALateReplace() throws IOException {
        testIgnoreLateAction((m1, m2) -> {
            // in order to be able to call replace() "locally"
            m2.put("key-1", "dummy");
            m2.replace("key-1", "value-2");
            assertEquals("value-2", m2.get("key-1"));
        });
    }

    @Test
    public void testIgnoreALateReplaceWithValue() throws IOException {
        testIgnoreLateAction((m1, m2) -> {
            // in order to be able to call replace() "locally"
            m2.put("key-1", "dummy");
            m2.replace("key-1", "dummy", "value-2");
            assertEquals("value-2", m2.get("key-1"));
        });
    }

    @Test
    public void testIgnoreALateRemoveWithValue() throws IOException {
        testIgnoreLateAction((m1, m2) -> {
            // in order to be able to call remove() "locally"
            m2.put("key-1", "dummy");
            m2.remove("key-1", "dummy");
            assertEquals(null, m2.get("key-1"));
        });
    }

    @Test
    public void testIgnoreALateRemove() throws IOException {
        testIgnoreLateAction((m1, m2) -> {
            // in order to be able to call remove() "locally"
            m2.put("key-1", "dummy");
            m2.remove("key-1");
            assertEquals(null, m2.get("key-1"));
        });
    }

    @Test
    public void testRemoveFollowedByLatePut() throws IOException {

        final TimeProvider timeProvider = Mockito.mock(TimeProvider.class);
        try (ChronicleMap<CharSequence, CharSequence> map =
                     ChronicleMapBuilder.of(CharSequence.class, CharSequence.class)
                             .entries(10)
                             .timeProvider(timeProvider)
                             .replication((byte) 1, TcpTransportAndNetworkConfig.of(8086))
                             .create()) {
            try (ChronicleMap<CharSequence, CharSequence> map2 =
                         ChronicleMapBuilder.of(CharSequence.class, CharSequence.class)
                                 .entries(10)
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
                assertEquals(map.size(), 0);
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
                .thenReturn(System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(5));
    }
}
