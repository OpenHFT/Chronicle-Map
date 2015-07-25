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

package net.openhft.chronicle.map.example;

import net.openhft.chronicle.hash.replication.TcpTransportAndNetworkConfig;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

/**
 * Created by peter.lawrey on 12/11/14.
 */
public class ServersMapMain {
    static final int port = Integer.getInteger("port", 8989);
    static final int entries = Integer.getInteger("entries", 100000);
    static final int runs = Integer.getInteger("runs", 5);
    static final boolean stateless = Boolean.getBoolean("stateless");

    public static void startServer() throws IOException {
        File file = File.createTempFile("testServersMapMain", ".deleteme");
        file.deleteOnExit();
        final ChronicleMap<byte[], byte[]> serverMap =
                ChronicleMapBuilder.of(byte[].class, byte[].class)
                        .putReturnsNull(true)
                        .constantKeySizeBySample(new byte[8])
                        .constantValueSizeBySample(new byte[32])
                        .replication((byte) 2, TcpTransportAndNetworkConfig.of(port))
                        .createPersistedTo(file);
        System.out.println("Server started");
        System.in.read();
        serverMap.close();
    }

    public static void startRemoteClient(String hostname) throws IOException {
        final ChronicleMap<byte[], byte[]> map;
        if (stateless) {
            map = ChronicleMapBuilder
                    .of(byte[].class, byte[].class, new InetSocketAddress(hostname, port))
                    .putReturnsNull(true)
                    .create();
        } else {
            File file = File.createTempFile("testServersMapMain", ".deleteme");
            file.deleteOnExit();
            TcpTransportAndNetworkConfig tcpConfig =
                    TcpTransportAndNetworkConfig.of(port, new InetSocketAddress(hostname, port));

            map = ChronicleMapBuilder
                    .of(byte[].class, byte[].class)
                    .putReturnsNull(true)
                    .replication((byte) 1, tcpConfig)
                    .createPersistedTo(file);
        }
        ByteBuffer key = ByteBuffer.allocate(8);
        ByteBuffer value = ByteBuffer.allocate(32);
        double lastAveragePut = Double.POSITIVE_INFINITY, lastAverageGet = Double.POSITIVE_INFINITY;
        for (int i = 0; i < runs; i++) {
            long puts = 0, gets = 0;
            for (int j = 0; j < entries; j++) {
                key.putLong(0, j);
                value.putLong(0, j);
                long t1 = System.nanoTime();
                map.put(key.array(), value.array());
                long t2 = System.nanoTime();
                map.get(key.array());
                long t3 = System.nanoTime();
                puts += t2 - t1;
                gets += t3 - t2;
                if (t2 - t1 > lastAveragePut * 100 || t3 - t2 > lastAverageGet * 100)
                    System.out.printf("Took put/get took %.1f/%.1f us%n",
                            (t2 - t1) / 1e3, (t3 - t2) / 1e3);
            }
            lastAveragePut = puts / entries;
            lastAverageGet = gets / entries;
            System.out.printf("Average took put/get took %.1f/%.1f us%n",
                    lastAveragePut / 1e3, lastAverageGet / 1e3);
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length > 0)
            startRemoteClient(args[0]);
        else
            startServer();
    }

}
