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

package net.openhft.chronicle.hash;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * @author Rob Austin.
 */
public class StatelessBuilder {

    private long timeoutMs = TimeUnit.SECONDS.toMillis(10);

    private InetSocketAddress remoteAddress;

    public long timeoutMs() {
        return timeoutMs;
    }

    public void timeout(long timeout, TimeUnit units) {
        this.timeoutMs = units.toMillis(timeout);
    }

    private StatelessBuilder(InetSocketAddress remoteAddress) {
        this.remoteAddress = remoteAddress;
    }

    public static StatelessBuilder remoteAddress(InetSocketAddress inetSocketAddress) {
        return new StatelessBuilder(inetSocketAddress);
    }

    public InetSocketAddress remoteAddress() {
        return remoteAddress;
    }
}
