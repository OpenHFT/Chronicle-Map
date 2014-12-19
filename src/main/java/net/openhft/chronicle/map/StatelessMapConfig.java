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

import net.openhft.chronicle.hash.StatelessClientConfig;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Rob Austin.
 */
class StatelessMapConfig<K, V> implements StatelessClientConfig<ChronicleMap<K, V>> {

    private final ChronicleMapBuilder<K, V> mapBuilder;
    private final InetSocketAddress remoteAddress;
    private final long timeoutMs;
    private final String name;

    private int tcpBufferSize = 1024 * VanillaChronicleMap.MAX_ENTRY_OVERSIZE_FACTOR;

    private final AtomicBoolean used;

    StatelessMapConfig(ChronicleMapBuilder<K, V> mapBuilder,
                       InetSocketAddress remoteAddress, long timeoutMs,
                       String name, AtomicBoolean used) {
        this.mapBuilder = mapBuilder;
        this.remoteAddress = remoteAddress;
        this.timeoutMs = timeoutMs;
        this.name = name;
        this.used = used;
    }

    public long timeoutMs() {
        return timeoutMs;
    }

    public InetSocketAddress remoteAddress() {
        return remoteAddress;
    }

    @Override
    public StatelessMapConfig<K, V> timeout(long timeout, TimeUnit units) {
        return new StatelessMapConfig<>(mapBuilder, remoteAddress, units.toMillis(timeout),
                name, used);
    }

    @Override
    public StatelessClientConfig<ChronicleMap<K, V>> name(String name) {
        return new StatelessMapConfig<>(mapBuilder, remoteAddress, timeoutMs, name, used);
    }

    String name() {
        return name;
    }

    @Override
    public ChronicleMap<K, V> create() throws IOException {
        if (!used.getAndSet(true)) {
            return mapBuilder.createStatelessClient(this);
        } else {
            throw new IllegalStateException(
                    "A stateless client has already been created using this config chain. " +
                            "Create a new StatelessClientConfig (builder.statelessClient()) " +
                            "to create a new stateless client");
        }
    }

    public int tcpBufferSize() {
        return tcpBufferSize;
    }

    public StatelessMapConfig tcpBufferSize(int packetSize) {
        this.tcpBufferSize = packetSize;
        return this;
    }
}
