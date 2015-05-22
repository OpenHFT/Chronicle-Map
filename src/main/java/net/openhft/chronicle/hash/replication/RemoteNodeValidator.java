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

package net.openhft.chronicle.hash.replication;

import java.net.SocketAddress;

/**
 * @author Rob Austin.
 */
public interface RemoteNodeValidator {

    /**
     * checks the identifier that is unique and we haven't seen it before, unless it comes from the same port
     * and host.
     *
     * @param remoteIdentifier remoteIdentifier
     * @param remoteAddress        remoteAddress
     * @return               true if unique
     */
    boolean validate(byte remoteIdentifier, SocketAddress remoteAddress);
}
