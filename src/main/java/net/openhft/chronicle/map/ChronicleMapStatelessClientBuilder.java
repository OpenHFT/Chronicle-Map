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

import net.openhft.chronicle.hash.ChronicleHashStatelessClientBuilder;
import net.openhft.lang.MemoryUnit;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Rob Austin.
 */
public final class ChronicleMapStatelessClientBuilder<K, V> implements
        ChronicleHashStatelessClientBuilder<ChronicleMapStatelessClientBuilder<K, V>,
                ChronicleMap<K, V>>,
        MapBuilder<ChronicleMapStatelessClientBuilder<K, V>> {

    public static <K, V> ChronicleMapStatelessClientBuilder<K, V> of(
            InetSocketAddress serverAddress) {
        return new ChronicleMapStatelessClientBuilder<>(serverAddress);
    }

    // This method is meaningful, despite you could call of().create(), because Java 7 doesn't
    // infer type parameters if case of chained calls, like this; and then you couldn't omit class
    // name. Then it is
    // ChronicleMapStatelessClientBuilder.<Integer, Integer>.of(addr).create()
    // vs
    // createClientOf(addr) // static import
    // -- the second is really shorter.

    /**
     * Equivalent of {@code ChronicleMapStatelessClientBuilder.<K,V>of(serverAddress).create()}.
     *
     * @param serverAddress address of the server map
     * @param <K> key type of the map
     * @param <V> value type of the map
     * @return stateless client of the ChronicleMap of server
     * @throws IOException if it not possible to connect to the {@code serverAddress}
     */
    public static <K, V> ChronicleMap<K, V> createClientOf(InetSocketAddress serverAddress)
            throws IOException {
        return ChronicleMapStatelessClientBuilder.<K, V>of(serverAddress).create();
    }

    private final InetSocketAddress remoteAddress;
    private boolean putReturnsNull = false;
    private boolean removeReturnsNull = false;
    private long timeoutMs = TimeUnit.SECONDS.toMillis(10);
    private String name;
    private int tcpBufferSize = (int) MemoryUnit.KILOBYTES.toBytes(64);

    private final AtomicBoolean used = new AtomicBoolean(false);

    ChronicleMapStatelessClientBuilder(InetSocketAddress remoteAddress) {
        this.remoteAddress = remoteAddress;
    }

    InetSocketAddress remoteAddress() {
        return remoteAddress;
    }

    @Override
    public ChronicleMapStatelessClientBuilder<K, V> putReturnsNull(boolean putReturnsNull) {
        this.putReturnsNull = putReturnsNull;
        return this;
    }

    boolean putReturnsNull() {
        return putReturnsNull;
    }

    @Override
    public ChronicleMapStatelessClientBuilder<K, V> removeReturnsNull(boolean removeReturnsNull) {
        this.removeReturnsNull = removeReturnsNull;
        return this;
    }

    boolean removeReturnsNull() {
        return removeReturnsNull;
    }

    @Override
    public ChronicleMapStatelessClientBuilder<K, V> timeout(long timeout, TimeUnit units) {
        this.timeoutMs = units.toMillis(timeout);
        return this;
    }

    long timeoutMs() {
        return timeoutMs;
    }

    @Override
    public ChronicleMapStatelessClientBuilder<K, V> name(String name) {
        this.name = name;
        return this;
    }

    String name() {
        return name;
    }

    @Override
    public ChronicleMapStatelessClientBuilder<K, V> tcpBufferSize(int tcpBufferSize) {
        this.tcpBufferSize = tcpBufferSize;
        return this;
    }

    int tcpBufferSize() {
        return tcpBufferSize;
    }

    @Override
    public ChronicleMap<K, V> create() throws IOException {
        if (!used.getAndSet(true)) {
            return new StatelessChronicleMap<>(this);
        } else {
            throw new IllegalStateException(
                    "A stateless client has already been created using this builder. " +
                            "Create a new ChronicleMapStatelessClientBuilder " +
                            "to create a new stateless client");
        }
    }
}
