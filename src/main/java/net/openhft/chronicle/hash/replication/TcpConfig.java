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

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.unmodifiableSet;
import static java.util.concurrent.TimeUnit.SECONDS;


public final class TcpConfig {

    private static final int DEFAULT_PACKET_SIZE = 1024 * 8;
    private static final long DEFAULT_HEART_BEAT_INTERVAL = 20;
    private static final TimeUnit DEFAULT_HEART_BEAT_INTERVAL_UNIT = SECONDS;

    private final int serverPort;
    private final Set<InetSocketAddress> endpoints;
    private final int packetSize;
    private final boolean autoReconnectedUponDroppedConnection;
    private final ThrottlingConfig throttlingConfig;
    private final long heartBeatInterval;
    private final TimeUnit heartBeatIntervalUnit;
    private final IdentifierListener identifierListener;

    private TcpConfig(int serverPort, Set<InetSocketAddress> endpoints, int packetSize,
                      boolean autoReconnectedUponDroppedConnection,
                      ThrottlingConfig throttlingConfig, long heartBeatInterval,
                      TimeUnit heartBeatIntervalUnit, IdentifierListener identifierListener) {
        this.serverPort = serverPort;
        this.endpoints = endpoints;
        this.packetSize = packetSize;
        this.autoReconnectedUponDroppedConnection = autoReconnectedUponDroppedConnection;
        this.throttlingConfig = throttlingConfig;
        this.heartBeatInterval = heartBeatInterval;
        this.heartBeatIntervalUnit = heartBeatIntervalUnit;
        this.identifierListener = identifierListener;
    }


    public static TcpConfig forSendingNode(int serverPort, InetSocketAddress... endpoints) {
        return forSendingNode(serverPort, Arrays.asList(endpoints));
    }

    public static TcpConfig forSendingNode(int serverPort,
                                           Collection<InetSocketAddress> endpoints) {
        if (endpoints.isEmpty())
            throw new IllegalArgumentException("There should be some endpoints");
        return unknownTopology(serverPort, endpoints);
    }

    public static TcpConfig forReceivingOnlyNode(int serverPort) {
        return unknownTopology(serverPort, Collections.<InetSocketAddress>emptyList());
    }

    public static TcpConfig unknownTopology(int serverPort,
                                            Collection<InetSocketAddress> endpoints) {
        for (final InetSocketAddress endpoint : endpoints) {
            if (endpoint.getPort() == serverPort && "localhost".equals(endpoint.getHostName()))
                throw new IllegalArgumentException("endpoint=" + endpoint
                        + " can not point to the same port as the server");
        }
        return new TcpConfig(
                serverPort, unmodifiableSet(new HashSet<InetSocketAddress>(endpoints)),
                DEFAULT_PACKET_SIZE,
                true, // autoReconnectedUponDroppedConnection
                ThrottlingConfig.noThrottling(),
                DEFAULT_HEART_BEAT_INTERVAL,
                DEFAULT_HEART_BEAT_INTERVAL_UNIT,
                null); // identifierListener
    }

    public IdentifierListener nonUniqueIdentifierListener() {
        return identifierListener;
    }

    public TcpConfig nonUniqueIdentifierListener(final IdentifierListener identifierListener) {
        return new TcpConfig(serverPort, endpoints, packetSize,
                autoReconnectedUponDroppedConnection, throttlingConfig, heartBeatInterval,
                heartBeatIntervalUnit, identifierListener);
    }

    public boolean autoReconnectedUponDroppedConnection() {
        return autoReconnectedUponDroppedConnection;
    }

    public TcpConfig autoReconnectedUponDroppedConnection(boolean autoReconnectedUponDroppedConnection) {
        return new TcpConfig(serverPort, endpoints, packetSize,
                autoReconnectedUponDroppedConnection, throttlingConfig, heartBeatInterval,
                heartBeatIntervalUnit, identifierListener);
    }

    public ThrottlingConfig throttlingConfig() {
        return throttlingConfig;
    }

    public TcpConfig throttlingConfig(ThrottlingConfig throttlingConfig) {
        ThrottlingConfig.checkMillisecondBucketInterval(throttlingConfig, "TCP");
        return new TcpConfig(serverPort, endpoints, packetSize,
                autoReconnectedUponDroppedConnection, throttlingConfig, heartBeatInterval,
                heartBeatIntervalUnit, identifierListener);
    }

    public long heartBeatInterval(TimeUnit unit) {
        return unit.convert(heartBeatInterval, heartBeatIntervalUnit);
    }

    public int serverPort() {
        return serverPort;
    }

    public TcpConfig serverPort(int serverPort) {
        return new TcpConfig(serverPort, endpoints, packetSize,
                autoReconnectedUponDroppedConnection, throttlingConfig, heartBeatInterval,
                heartBeatIntervalUnit, identifierListener);
    }

    public Set<InetSocketAddress> endpoints() {
        return endpoints;
    }

    public TcpConfig endpoints(Set<InetSocketAddress> endpoints) {
        for (final InetSocketAddress endpoint : endpoints) {
            if (endpoint.getPort() == serverPort && "localhost".equals(endpoint.getHostName()))
                throw new IllegalArgumentException("endpoint=" + endpoint
                        + " can not point to the same port as the server");
        }
        return new TcpConfig(serverPort, endpoints, packetSize,
                autoReconnectedUponDroppedConnection, throttlingConfig, heartBeatInterval,
                heartBeatIntervalUnit, identifierListener);
    }

    public int packetSize() {
        return packetSize;
    }

    public TcpConfig packetSize(int packetSize) {
        if (packetSize <= 0)
            throw new IllegalArgumentException();
        return new TcpConfig(serverPort, endpoints, packetSize,
                autoReconnectedUponDroppedConnection, throttlingConfig, heartBeatInterval,
                heartBeatIntervalUnit, identifierListener);
    }

    public TcpConfig heartBeatInterval(long heartBeatInterval, TimeUnit heartBeatIntervalUnit) {
        return new TcpConfig(serverPort, endpoints, packetSize,
                autoReconnectedUponDroppedConnection, throttlingConfig, heartBeatInterval,
                heartBeatIntervalUnit, identifierListener);
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TcpConfig that = (TcpConfig) o;

        if (autoReconnectedUponDroppedConnection != that.autoReconnectedUponDroppedConnection) return false;
        if (heartBeatInterval != that.heartBeatInterval) return false;
        if (packetSize != that.packetSize) return false;
        if (serverPort != that.serverPort) return false;
        if (endpoints != null ? !endpoints.equals(that.endpoints) : that.endpoints != null) return false;
        if (heartBeatIntervalUnit != that.heartBeatIntervalUnit) return false;
        if (throttlingConfig != null ? !throttlingConfig.equals(that.throttlingConfig) : that.throttlingConfig != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = serverPort;
        result = 31 * result + (endpoints != null ? endpoints.hashCode() : 0);
        result = 31 * result + packetSize;
        result = 31 * result + (autoReconnectedUponDroppedConnection ? 1 : 0);
        result = 31 * result + (throttlingConfig != null ? throttlingConfig.hashCode() : 0);
        result = 31 * result + (int) (heartBeatInterval ^ (heartBeatInterval >>> 32));
        result = 31 * result + (heartBeatIntervalUnit != null ? heartBeatIntervalUnit.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "TcpReplicationConfig{" +
                "serverPort=" + serverPort +
                ", endpoints=" + endpoints +
                ", packetSize=" + packetSize +
                ", autoReconnectedUponDroppedConnection=" + autoReconnectedUponDroppedConnection +
                ", throttlingConfig=" + throttlingConfig +
                ", heartBeatInterval=" + heartBeatInterval +
                ", heartBeatIntervalUnit=" + heartBeatIntervalUnit +
                '}';
    }
}
