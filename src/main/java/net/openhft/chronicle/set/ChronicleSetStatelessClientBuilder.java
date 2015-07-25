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
