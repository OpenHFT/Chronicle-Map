/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.hash;

import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.set.ChronicleSet;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Builder of stateless {@link ChronicleHash} implementation, this stateless implementation will be
 * referred to as a stateless client as it
 * will not hold any of its own data locally, the stateless client will perform Remote Procedure
 * Calls ( RPC ) to another {@link ChronicleMap} or {@link ChronicleSet} which we will refer to
 * as the server. The server will hold all your data, the server can not it’s self be a
 * stateless client. Your stateless client must be connected to the server via TCP/IP. The
 * stateless client will delegate all your method calls to the remote sever. The stateless
 * client operations will block, in other words the stateless client will wait for the server to
 * send a response before continuing to the next operation. The stateless client could be
 * consider to be a ClientProxy to  {@link ChronicleMap} or {@link ChronicleSet}  running on
 * another host
 *
 * @param <H> the type of {@code ChronicleHash} accessed remotely, {@code ChronicleMap} or
 *           {@code ChronicleSet}
 */
public interface ChronicleHashStatelessClientBuilder<
        C extends ChronicleHashStatelessClientBuilder<C, H>, H extends ChronicleHash> {
    C timeout(long timeout, TimeUnit units);

    C name(String name);

    C tcpBufferSize(int tcpBufferSize);

    H create() throws IOException;
}
