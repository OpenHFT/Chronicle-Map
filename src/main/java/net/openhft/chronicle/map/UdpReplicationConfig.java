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

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.InetAddress;
import java.net.NetworkInterface;


public abstract class UdpReplicationConfig {

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
