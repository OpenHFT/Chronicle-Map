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

import net.openhft.chronicle.hash.UdpReplicationConfig;
import org.junit.After;
import org.junit.Before;

import java.io.Closeable;
import java.io.IOException;
import java.net.Inet4Address;

/**
 * Test  ReplicatedChronicleMap where the Replicated is over a TCP Socket
 *
 * @author Rob Austin.
 */

public class UDPSocketReplicationTest {

    private ChronicleMap<Integer, CharSequence> map2;

    static ChronicleMap<Integer, CharSequence> newUdpSocketShmIntString(
            final int identifier,
            final int udpPort) throws IOException {

        UdpReplicationConfig udpConfig = UdpReplicationConfig
                .simple(Inet4Address.getByName("255.255.255.255"), udpPort);

        return ChronicleMapBuilder.of(Integer.class, CharSequence.class)
                .entries(1000)
                .replicators((byte) identifier, udpConfig).create();
    }

    @Before
    public void setup() throws IOException {

        map2 = newUdpSocketShmIntString(1, 1234);
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @After
    public void tearDown() throws InterruptedException {

        for (final Closeable closeable : new Closeable[]{map2}) {
            try {
                closeable.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


  /*  @Test
  //  @Ignore
    public void testBufferOverflow() throws IOException, InterruptedException {

        for (int i = 0; i < 1024; i++) {
            Thread.sleep(5000);
            map2.put(i * 2, "E");
            System.out.println("" + map2);
        }

    }
*/

}



