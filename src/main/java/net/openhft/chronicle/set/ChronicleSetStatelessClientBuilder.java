/*
 * Copyright 2014 Higher Frequency Trading http://www.higherfrequencytrading.com
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

package net.openhft.chronicle.set;

import net.openhft.chronicle.hash.ChronicleHashStatelessClientBuilder;
import net.openhft.chronicle.hash.serialization.internal.DummyValue;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import net.openhft.chronicle.map.ChronicleMapStatelessClientBuilder;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

public final class ChronicleSetStatelessClientBuilder<E> implements
        ChronicleHashStatelessClientBuilder<
                ChronicleSetStatelessClientBuilder<E>, ChronicleSet<E>> {

    public static <E> ChronicleSetStatelessClientBuilder<E> of(InetSocketAddress serverAddress) {
        return new ChronicleSetStatelessClientBuilder<>(serverAddress);
    }

    private final ChronicleMapStatelessClientBuilder<E, DummyValue> mapClientBuilder;

    ChronicleSetStatelessClientBuilder(InetSocketAddress serverAddress) {
        Class<E> eClass = null;
        this.mapClientBuilder = ChronicleMapBuilder.of(eClass, DummyValue.class, serverAddress);
    }

    @Override
    public ChronicleSetStatelessClientBuilder<E> timeout(long timeout, TimeUnit units) {
        mapClientBuilder.timeout(timeout, units);
        return this;
    }

    @Override
    public ChronicleSetStatelessClientBuilder<E> name(String name) {
        mapClientBuilder.name(name);
        return this;
    }

    @Override
    public ChronicleSetStatelessClientBuilder<E> tcpBufferSize(int tcpBufferSize) {
        mapClientBuilder.tcpBufferSize(tcpBufferSize);
        return this;
    }

    @Override
    public ChronicleSet<E> create() throws IOException {
        return new SetFromMap<>(mapClientBuilder.create());
    }
}
