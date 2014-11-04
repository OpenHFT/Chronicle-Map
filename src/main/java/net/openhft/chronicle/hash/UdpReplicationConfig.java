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
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.InetAddress;
import java.net.NetworkInterface;


public abstract class UdpReplicationConfig implements ReplicationConfig {

    /**
     * Package-private constructor forbids subclassing from outside of the package
     */
    UdpReplicationConfig() {
        // nothing to do
    }

    public static UdpReplicationConfig simple(@NotNull InetAddress address, int port) {
        if (address.isMulticastAddress())
            throw new IllegalArgumentException();


        return create(address, port, null, ThrottlingConfig.noThrottling());
    }

    public static UdpReplicationConfig multiCast(@NotNull InetAddress address, int port,
                                                 @NotNull NetworkInterface networkInterface) {
        if (!address.isMulticastAddress() || networkInterface == null)
            throw new IllegalArgumentException();
        return create(address, port, networkInterface, ThrottlingConfig.noThrottling());
    }

    static UdpReplicationConfig create(InetAddress address, int port,
                                       NetworkInterface networkInterface,
                                       ThrottlingConfig throttlingConfig) {
        return new UdpReplicationConfigBean(address, port, networkInterface,
                throttlingConfig);
    }

    public abstract InetAddress address();

    public abstract int port();

    @Nullable
    public abstract NetworkInterface networkInterface();

    public abstract ThrottlingConfig throttlingConfig();

    public UdpReplicationConfig throttlingConfig(ThrottlingConfig throttlingConfig) {
        ThrottlingConfig.checkMillisecondBucketInterval(throttlingConfig, "UDP");
        return create(address(), port(), networkInterface(), throttlingConfig);
    }
}



final class UdpReplicationConfigBean extends UdpReplicationConfig {
    private final InetAddress address;
    private final int port;
    private final NetworkInterface networkInterface;
    private final ThrottlingConfig throttlingConfig;

    UdpReplicationConfigBean(
            InetAddress address,
            int port,
            NetworkInterface networkInterface,
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

    @Override
    public InetAddress address() {
        return address;
    }

    @Override
    public int port() {
        return port;
    }

    @Override
    public NetworkInterface networkInterface() {
        return networkInterface;
    }

    @Override
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
        if (o instanceof UdpReplicationConfig) {
            UdpReplicationConfig that = (UdpReplicationConfig) o;
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
}
