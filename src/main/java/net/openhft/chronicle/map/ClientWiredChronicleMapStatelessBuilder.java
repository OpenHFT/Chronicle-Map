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
public final class ClientWiredChronicleMapStatelessBuilder<K, V> implements
        ChronicleHashStatelessClientBuilder<ClientWiredChronicleMapStatelessBuilder<K, V>,
                ChronicleMap<K, V>>,
        MapBuilder<ClientWiredChronicleMapStatelessBuilder<K, V>> {

    ClientWiredStatelessTcpConnectionHub hub;
    private Class keyClass;
    private Class valueClass;
    private byte localIdentifier;
    private short channelID;
    private boolean doHandShaking;

    public ClientWiredChronicleMapStatelessBuilder(
            InetSocketAddress remoteAddress, Class keyClass, Class valueClass, short channelID) {
        this.keyClass = keyClass;
        this.valueClass = valueClass;
        this.remoteAddress = remoteAddress;
        this.channelID = channelID;
    }

    public ClientWiredChronicleMapStatelessBuilder(
            ClientWiredStatelessTcpConnectionHub hub, Class keyClass, Class valueClass,
            short channelID) {
        this.keyClass = keyClass;
        this.valueClass = valueClass;
        this.hub = hub;
        this.channelID = channelID;
    }


    private InetSocketAddress remoteAddress;
    private boolean putReturnsNull = false;
    private boolean removeReturnsNull = false;
    private long timeoutMs = TimeUnit.SECONDS.toMillis(10);
    private String name;
    private int tcpBufferSize = (int) MemoryUnit.KILOBYTES.toBytes(64);

    private final AtomicBoolean used = new AtomicBoolean(false);


    InetSocketAddress remoteAddress() {
        return remoteAddress;
    }

    @Override
    public ClientWiredChronicleMapStatelessBuilder<K, V> putReturnsNull(boolean putReturnsNull) {
        this.putReturnsNull = putReturnsNull;
        return this;
    }

    boolean putReturnsNull() {
        return putReturnsNull;
    }

    @Override
    public ClientWiredChronicleMapStatelessBuilder<K, V> removeReturnsNull(
            boolean removeReturnsNull) {
        this.removeReturnsNull = removeReturnsNull;
        return this;
    }

    boolean removeReturnsNull() {
        return removeReturnsNull;
    }

    @Override
    public ClientWiredChronicleMapStatelessBuilder<K, V> timeout(long timeout, TimeUnit units) {
        this.timeoutMs = units.toMillis(timeout);
        return this;
    }

    long timeoutMs() {
        return timeoutMs;
    }

    @Override
    public ClientWiredChronicleMapStatelessBuilder<K, V> name(String name) {
        this.name = name;
        return this;
    }

    String name() {
        return name;
    }

    @Override
    public ClientWiredChronicleMapStatelessBuilder<K, V> tcpBufferSize(int tcpBufferSize) {
        this.tcpBufferSize = tcpBufferSize;
        return this;
    }

    int tcpBufferSize() {
        return tcpBufferSize;
    }

    @Override
    public ChronicleMap<K, V> create() throws IOException {

        // todo clean this up
        if (hub == null)
            hub = new ClientWiredStatelessTcpConnectionHub(this, localIdentifier, doHandShaking);

        if (!used.getAndSet(true)) {
            return new ClientWiredStatelessChronicleMap<K, V>(
                    this, keyClass, valueClass, channelID);

        } else {
            throw new IllegalStateException(
                    "A stateless client has already been created using this builder. " +
                            "Create a new ChronicleMapStatelessClientBuilder " +
                            "to create a new stateless client");
        }
    }

    public void identifier(byte identifier) {
        this.localIdentifier = identifier;
    }

    public short channelID() {
        return channelID;
    }


    public ClientWiredStatelessTcpConnectionHub hub() {
        return hub;
    }
}
