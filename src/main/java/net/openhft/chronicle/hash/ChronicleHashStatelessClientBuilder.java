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
