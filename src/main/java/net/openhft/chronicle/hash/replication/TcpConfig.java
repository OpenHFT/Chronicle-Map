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

import net.openhft.chronicle.hash.ReplicationConfig;

import org.jetbrains.annotations.NotNull;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.unmodifiableSet;
import static java.util.concurrent.TimeUnit.SECONDS;


public class TcpConfig implements ReplicationConfig {

    private static final int DEFAULT_PACKET_SIZE = 1024 * 8;
    private static final long DEFAULT_HEART_BEAT_INTERVAL = 20;
    private static final TimeUnit DEFAULT_HEART_BEAT_INTERVAL_UNIT = SECONDS;

    private int serverPort = DEFAULT_PACKET_SIZE;
    private Set<InetSocketAddress> endpoints;
    private int packetSize = DEFAULT_PACKET_SIZE;
    private boolean autoReconnectedUponDroppedConnection = true;
    private ThrottlingConfig throttlingConfig = ThrottlingConfig.noThrottling();
    private long heartBeatInterval = DEFAULT_HEART_BEAT_INTERVAL;
    private TimeUnit heartBeatIntervalUnit = DEFAULT_HEART_BEAT_INTERVAL_UNIT;
    private IdentifierListener identifierListener;

    TcpConfig() {

    }

    public IdentifierListener nonUniqueIdentifierListener() {
        return identifierListener;
    }

    public TcpConfig nonUniqueIdentifierListener(@NotNull final IdentifierListener
                                                                    identifierListener) {
        this.identifierListener = identifierListener;
        return this;
    }

    public boolean autoReconnectedUponDroppedConnection() {
        return autoReconnectedUponDroppedConnection;
    }

    public TcpConfig autoReconnectedUponDroppedConnection(boolean autoReconnectedUponDroppedConnection) {
        this.autoReconnectedUponDroppedConnection = autoReconnectedUponDroppedConnection;
        return this;
    }

      public ThrottlingConfig throttlingConfig() {
        return throttlingConfig;
    }

    public TcpConfig throttlingConfig(ThrottlingConfig throttlingConfig) {
        this.throttlingConfig = throttlingConfig;
        return this;
    }

    public long heartBeatInterval(TimeUnit unit) {
        return unit.convert(heartBeatInterval, heartBeatIntervalUnit);
    }

    public int serverPort() {
        return serverPort;
    }

    public TcpConfig serverPort(int serverPort) {
        this.serverPort = serverPort;
        return this;
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

        this.endpoints = endpoints;
        return this;
    }

    public int packetSize() {
        return packetSize;
    }

    public static TcpConfig of(int serverPort, InetSocketAddress... endpoints) {
        return of(serverPort, Arrays.asList(endpoints));
    }

    public static TcpConfig of(int serverPort, Collection<InetSocketAddress> endpoints) {
        // at least in tests, we configure "receive-only" replication without endpoints.
        // TODO decide what to do with this case
//        if (endpoints.isEmpty())
//            throw new IllegalArgumentException("There should be some endpoints");
        for (final InetSocketAddress endpoint : endpoints) {
            if (endpoint.getPort() == serverPort && "localhost".equals(endpoint.getHostName()))
                throw new IllegalArgumentException("endpoint=" + endpoint
                        + " can not point to the same port as the server");
        }

        TcpConfig tcpConfig = new TcpConfig();

        tcpConfig.serverPort(serverPort);
        tcpConfig.endpoints = unmodifiableSet(new HashSet<InetSocketAddress>(endpoints));

        return tcpConfig;


    }


    public TcpConfig packetSize(int packetSize) {
        if (packetSize <= 0)
            throw new IllegalArgumentException();
        this.packetSize = packetSize;
        return this;
    }

    public TcpConfig throttlingConfig(long heartBeatInterval, TimeUnit heartBeatIntervalUnit) {
        this.heartBeatInterval = heartBeatInterval;
        this.heartBeatIntervalUnit = heartBeatIntervalUnit;
        return this;
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

    public TcpConfig heartBeatInterval(long time, TimeUnit unit) {
        this.heartBeatInterval = time;
        this.heartBeatIntervalUnit = unit;
        return this;
    }


}
