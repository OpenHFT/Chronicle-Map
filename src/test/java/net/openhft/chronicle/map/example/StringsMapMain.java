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

package net.openhft.chronicle.map.example;

import net.openhft.chronicle.hash.replication.TcpTransportAndNetworkConfig;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Created by peter.lawrey on 12/11/14.
 */
public class StringsMapMain {
    static final int port = Integer.getInteger("port", 8998);
    static final int entries = Integer.getInteger("entries", 100000);
    static final int runs = Integer.getInteger("runs", 5);
    static final boolean stateless = Boolean.getBoolean("stateless");

    public static void startServer() throws IOException {
        File file = File.createTempFile("testServersMapMain", ".deleteme");
        file.deleteOnExit();
        int keyAndValueSize = (int) Math.round(Math.log10(entries));
        final ChronicleMap<CharSequence, CharSequence> serverMap = ChronicleMapBuilder
                .of(CharSequence.class, CharSequence.class)
                .entries(entries)
                .putReturnsNull(true)
                .averageKeySize(keyAndValueSize)
                .averageValueSize(keyAndValueSize)
                .replication((byte) 2, TcpTransportAndNetworkConfig.of(port))
                .createPersistedTo(file);
        System.out.println("Server started");
        System.in.read();
        serverMap.close();
    }

    public static void startRemoteClient(String hostname) throws IOException {
        final ChronicleMap<CharSequence, CharSequence> map;
        if (stateless) {
            map = ChronicleMapBuilder
                    .of(CharSequence.class, CharSequence.class, new InetSocketAddress(hostname, port))
                    .putReturnsNull(true)
                    .create();
        } else {
            File file = File.createTempFile("testServersMapMain", ".deleteme");
            file.deleteOnExit();
            TcpTransportAndNetworkConfig tcpConfig =
                    TcpTransportAndNetworkConfig.of(port, new InetSocketAddress(hostname, port));

            map = ChronicleMapBuilder
                    .of(CharSequence.class, CharSequence.class)
                    .putReturnsNull(true)
                    .replication((byte) 1, tcpConfig)
                    .createPersistedTo(file);
        }
        StringBuilder key = new StringBuilder();
        StringBuilder value = new StringBuilder();
        StringBuilder value2 = new StringBuilder();
        double lastAveragePut = Double.POSITIVE_INFINITY, lastAverageGet = Double.POSITIVE_INFINITY;
        for (int i = 0; i < runs; i++) {
            long puts = 0, gets = 0;
            for (int j = 0; j < entries; j++) {
                key.setLength(0);
                key.append(j);
                value.setLength(0);
                value.append(j);
                long t1 = System.nanoTime();
                map.put(key, value);
                long t2 = System.nanoTime();
                if (stateless) {
                    value.setLength(0);
                    value.append(map.get(key));
                } else {
                    map.getUsing(key, value);
                }
                long t3 = System.nanoTime();
                puts += t2 - t1;
                gets += t3 - t2;
//                if (t2 - t1 > lastAveragePut * 100 || t3 - t2 > lastAverageGet * 100)
//                    System.out.printf("Took put/get took %.1f/%.1f us%n",
// (t2 - t1) / 1e3, (t3 - t2) / 1e3);
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
