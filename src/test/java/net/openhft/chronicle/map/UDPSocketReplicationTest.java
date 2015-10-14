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

import net.openhft.chronicle.hash.replication.SingleChronicleHashReplication;
import net.openhft.chronicle.hash.replication.UdpTransportConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

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

        UdpTransportConfig udpConfig = UdpTransportConfig
                .of(Inet4Address.getByName("255.255.255.255"), udpPort);

        return ChronicleMapBuilder.of(Integer.class, CharSequence.class)
                .averageValue("E")
                .entries(1000)
                .replication(SingleChronicleHashReplication.builder().udpTransport(udpConfig)
                                .createWithId((byte) identifier))
                .create();
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
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        System.gc();
    }

    @Test
    public void testBufferOverflow() throws IOException, InterruptedException {

        for (int i = 0; i < 1024; i++) {
            Thread.sleep(5);
            map2.put(i * 2, "E");
            System.out.println("" + map2);
        }
    }

}

