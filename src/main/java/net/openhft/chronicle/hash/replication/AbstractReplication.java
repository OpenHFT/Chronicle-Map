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

package net.openhft.chronicle.hash.replication;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Serializable;

/**
 * Common configurations of {@link SingleChronicleHashReplication} and {@link ReplicationHub}.
 */
public abstract class AbstractReplication implements Serializable {
    private final byte localIdentifier;

    @Nullable
    private final TcpTransportAndNetworkConfig tcpConfig;

    @Nullable
    private final UdpTransportConfig udpConfig;

    @Nullable
    private final transient RemoteNodeValidator remoteNodeValidator;

    private final boolean bootstrapOnlyLocalEntries;
    private final String name;

    @Nullable
    private final transient ConnectionListener connectionListener;

    // package-private to forbid subclassing from outside of the package
    AbstractReplication(byte localIdentifier, Builder builder) {
        this.localIdentifier = localIdentifier;
        tcpConfig = builder.tcpConfig;
        udpConfig = builder.udpConfig;
        remoteNodeValidator = builder.remoteNodeValidator;
        bootstrapOnlyLocalEntries = builder.bootstrapOnlyLocalEntries;
        name = builder.name;
        connectionListener = builder.connectionListener;
    }

    @Override
    public String toString() {
        return "localIdentifier=" + localIdentifier +
                ", tcpConfig=" + tcpConfig +
                ", udpConfig=" + udpConfig +
                ", remoteNodeValidator=" + remoteNodeValidator +
                ", bootstrapOnlyLocalEntries=" + bootstrapOnlyLocalEntries +
                ", name=" + name;
    }

    public byte identifier() {
        return localIdentifier;
    }

    @Nullable
    public TcpTransportAndNetworkConfig tcpTransportAndNetwork() {
        return tcpConfig;
    }

    @Nullable
    public UdpTransportConfig udpTransport() {
        return udpConfig;
    }

    @Nullable
    public RemoteNodeValidator remoteNodeValidator() {
        return remoteNodeValidator;
    }

    public boolean bootstrapOnlyLocalEntries() {
        return bootstrapOnlyLocalEntries;
    }
    
    public String name() {
        return name;
    }

    public ConnectionListener connectionListener() {
        return this.connectionListener;
    }

    /**
     * Builder of {@link AbstractReplication} configurations. This class and it's subclasses are
     * mutable, configuration methods mutate the builder and return it back for convenient
     * chaining.
     *
     * @param <R> the concrete {@link AbstractReplication} subclass: {@link
     *            SingleChronicleHashReplication} or {@link ReplicationHub}
     * @param <B> the concrete builder subclass: {@link SingleChronicleHashReplication.Builder} or
     *            {@link ReplicationHub.Builder}
     */
    public static abstract class Builder<R extends AbstractReplication, B extends Builder<R, B>> {
        private TcpTransportAndNetworkConfig tcpConfig = null;
        private UdpTransportConfig udpConfig = null;
        private RemoteNodeValidator remoteNodeValidator = null;
        private ConnectionListener connectionListener = null;

        private boolean bootstrapOnlyLocalEntries = false;
        private String name = "(unknown)";

        // package-private to forbid subclassing from outside of the package
        Builder() {
        }

        @NotNull
        public B tcpTransportAndNetwork(@Nullable TcpTransportAndNetworkConfig tcpConfig) {
            this.tcpConfig = tcpConfig;
            return (B) this;
        }

        /**
         * Configures UDP transport settings, used by Replications, created by this builder. {@code
         * null} means that UDP transport shouldn't being used.
         *
         * @param udpConfig the new UDP transport config for replications, created by this builder.
         * @return this builder back, for chaining
         * @see AbstractReplication#udpTransport()
         */
        @NotNull
        public B udpTransport(@Nullable UdpTransportConfig udpConfig) {
            this.udpConfig = udpConfig;
            return (B) this;
        }

        @NotNull
        public B remoteNodeValidator(@Nullable RemoteNodeValidator remoteNodeValidator) {
            this.remoteNodeValidator = remoteNodeValidator;
            return (B) this;
        }


        @NotNull
        public B connectionListener(@Nullable ConnectionListener connectionListener) {
            this.connectionListener = connectionListener;
            return (B) this;
        }


        /**
         * Configures if the node, provided with replication, created by this builder, should
         * replicate only local data, last updated with own identifier, or all data currently
         * present on the node, when a new node joins the replication grid.
         *
         * <p>Default configuration is {@code false}, potentially swamping new nodes with
         * duplicates. However, this does guarantee that all the data is replicated over to the new
         * node and is useful especially in the case that the originating node of some data is not
         * currently running.
         *
         * @param bootstrapOnlyLocalEntries if a node provided with this replication should
         *                                  replicate only local data to the new nodes joining the
         *                                  replication network
         * @return this builder back
         */
        @NotNull
        public B bootstrapOnlyLocalEntries(boolean bootstrapOnlyLocalEntries) {
            this.bootstrapOnlyLocalEntries = bootstrapOnlyLocalEntries;
            return (B) this;
        }

        /**
         * Configures replication "name", which could appear in log messages and replication
         * thread names. Default is {@code "(unknown)"}.
         *
         * @param name human-readable replication name
         * @return this builder back
         */
        public B name(String name) {
            this.name = name;
            return (B) this;
        }

        /**
         * Creates a Replication instance with the given node (server) identifier.
         *
         * @param identifier the node (server) identifier of the returned replication
         * @return a new Replication instance with the specified node (server) identifier
         * @throws IllegalArgumentException if the given identifier is non-positive
         * @throws IllegalStateException if neither {@link #tcpTransportAndNetwork(
         *         TcpTransportAndNetworkConfig)} nor {@link #udpTransport(UdpTransportConfig)} are
         *         configured to non-{@code null}. At least one of the transport-level configs
         *         should be specified.
         */
        @NotNull
        public abstract R createWithId(byte identifier);

        void check(byte identifier) {
            if (identifier <= 0)
                throw new IllegalArgumentException("Identifier must be positive, " + identifier +
                        " given");
        }
    }
}
