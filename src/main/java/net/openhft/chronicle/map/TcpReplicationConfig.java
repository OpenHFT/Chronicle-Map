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

package net.openhft.chronicle.map;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableSet;
import static java.util.concurrent.TimeUnit.SECONDS;


public abstract class TcpReplicationConfig {

    private static final int DEFAULT_PACKET_SIZE = 1024 * 8;
    private static final long DEFAULT_HEART_BEAT_INTERVAL = 20;
    private static final TimeUnit DEFAULT_HEART_BEAT_INTERVAL_UNIT = SECONDS;

    /**
     * Package-private constructor forbids subclassing from outside of the package
     */
    TcpReplicationConfig() {
        // nothing to do
    }

    public static TcpReplicationConfig of(int serverPort, InetSocketAddress... endpoints) {
        return of(serverPort, asList(endpoints));
    }

    public static TcpReplicationConfig of(int serverPort, Collection<InetSocketAddress> endpoints) {
        // at least in tests, we configure "receive-only" replication without endpoints.
        // TODO decide what to do with this case
//        if (endpoints.isEmpty())
//            throw new IllegalArgumentException("There should be some endpoints");
        for (final InetSocketAddress endpoint : endpoints) {
            if (endpoint.getPort() == serverPort && "localhost".equals(endpoint.getHostName()))
                throw new IllegalArgumentException("endpoint=" + endpoint
                        + " can not point to the same port as the server");
        }
        return create(serverPort,
                unmodifiableSet(new HashSet<InetSocketAddress>(endpoints)),
                DEFAULT_PACKET_SIZE, ThrottlingConfig.noThrottling(),
                DEFAULT_HEART_BEAT_INTERVAL, DEFAULT_HEART_BEAT_INTERVAL_UNIT);
    }

    static TcpReplicationConfig create(int serverPort, Set<InetSocketAddress> endpoints,
                                       int packetSize, ThrottlingConfig throttlingConfig,
                                       long heartBeatInterval, TimeUnit heartBeatIntervalUnit) {
        return new TcpReplicationConfigBean(serverPort, endpoints, packetSize,
                throttlingConfig, heartBeatInterval, heartBeatIntervalUnit);
    }

    public abstract int serverPort();

    public abstract Set<InetSocketAddress> endpoints();

    public abstract int packetSize();

    public TcpReplicationConfig packetSize(int packetSize) {
        if (packetSize <= 0)
            throw new IllegalArgumentException();
        return create(serverPort(), endpoints(), packetSize, throttlingConfig(),
                heartBeatInterval(), heartBeatIntervalUnit());
    }

    public abstract ThrottlingConfig throttlingConfig();

    public TcpReplicationConfig throttlingConfig(ThrottlingConfig throttlingConfig) {
        ThrottlingConfig.checkMillisecondBucketInterval(throttlingConfig, "TCP");
        return create(serverPort(), endpoints(), packetSize(), throttlingConfig,
                heartBeatInterval(), heartBeatIntervalUnit());
    }

    abstract long heartBeatInterval();

    abstract TimeUnit heartBeatIntervalUnit();

    public long heartBeatInterval(TimeUnit unit) {
        return unit.convert(heartBeatInterval(), heartBeatIntervalUnit());
    }

    /**
     * @param heartBeatInterval heart beat interval
     * @param unit              the time unit of the interval
     * @return this builder back
     * @throws IllegalArgumentException if the given heart beat interval is unrecognisably small
     *                                  for the current TCP replicator implementation or negative. Current minimum interval
     *                                  is 1 millisecond.
     */
    public TcpReplicationConfig heartBeatInterval(long heartBeatInterval, TimeUnit unit) {
        if (unit.toMillis(heartBeatInterval) < 1) {
            throw new IllegalArgumentException(
                    "Minimum heart beat interval is 1 millisecond, " +
                            heartBeatInterval + " " + unit + " given");
        }
        return create(serverPort(), endpoints(), packetSize(), throttlingConfig(),
                heartBeatInterval, unit);
    }
}
