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

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.InetAddress;
import java.net.NetworkInterface;


public final class UdpTransportConfig {
    private final InetAddress address;
    private final int port;
    private final @Nullable NetworkInterface networkInterface;
    private final ThrottlingConfig throttlingConfig;

    private UdpTransportConfig(
            InetAddress address,
            int port,
            @Nullable NetworkInterface networkInterface,
            ThrottlingConfig throttlingConfig) {
        if (address == null) {
            throw new NullPointerException("Null address");
        }
        this.address = address;
        this.port = port;

        this.networkInterface = networkInterface;
        if (throttlingConfig == null) {
            throw new NullPointerException("Null throttlingConfig");
        }
        this.throttlingConfig = throttlingConfig;
    }


    public static UdpTransportConfig multiCast(@NotNull InetAddress address, int port,
                                                 @NotNull NetworkInterface networkInterface) {
        if (!address.isMulticastAddress() || networkInterface == null)
            throw new IllegalArgumentException();
        return create(address, port, networkInterface, ThrottlingConfig.noThrottling());
    }

    public static UdpTransportConfig of(@NotNull InetAddress address, int port) {
        if (address.isMulticastAddress())
            throw new IllegalArgumentException();
        return create(address, port, null, ThrottlingConfig.noThrottling());
    }

    static UdpTransportConfig create(InetAddress address, int port,
                                     @Nullable NetworkInterface networkInterface,
                                     ThrottlingConfig throttlingConfig) {
        return new UdpTransportConfig(address, port, networkInterface, throttlingConfig);
    }

    @NotNull
    public InetAddress address() {
        return address;
    }

    public int port() {
        return port;
    }

    @Nullable
    public NetworkInterface networkInterface() {
        return networkInterface;
    }

    @NotNull
    public ThrottlingConfig throttlingConfig() {
        return throttlingConfig;
    }

    @Override
    public String toString() {
        return "UdpReplicationConfig{"
                + "address=" + address
                + ", port=" + port
                + ", networkInterface=" + networkInterface
                + ", throttlingConfig=" + throttlingConfig
                + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof UdpTransportConfig) {
            UdpTransportConfig that = (UdpTransportConfig) o;
            return (this.address.equals(that.address()))
                    && (this.port == that.port())
                    && (this.networkInterface.equals(that.networkInterface()))
                    && (this.throttlingConfig.equals(that.throttlingConfig()));
        }
        return false;
    }

    @Override
    public int hashCode() {
        int h = 1;
        h *= 1000003;
        h ^= address.hashCode();
        h *= 1000003;
        h ^= port;
        h *= 1000003;
        h ^= networkInterface.hashCode();
        h *= 1000003;
        h ^= throttlingConfig.hashCode();
        return h;
    }

    public UdpTransportConfig networkInterfaceForMulticast(
            @Nullable NetworkInterface networkInterface) {
        if (!address().isMulticastAddress())
            throw new IllegalArgumentException();
        return create(address(), port(), networkInterface, throttlingConfig());
    }

    public UdpTransportConfig throttlingConfig(@NotNull ThrottlingConfig throttlingConfig) {
        ThrottlingConfig.checkMillisecondBucketInterval(throttlingConfig, "UDP");
        return create(address(), port(), networkInterface(), throttlingConfig);
    }
}
