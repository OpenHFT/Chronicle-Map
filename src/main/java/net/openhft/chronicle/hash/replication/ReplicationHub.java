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

import net.openhft.chronicle.hash.ChronicleHash;
import net.openhft.chronicle.hash.ChronicleHashBuilder;
import net.openhft.chronicle.hash.ChronicleHashInstanceConfig;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.set.ChronicleSet;
import org.jetbrains.annotations.NotNull;

/**
 * This class holds all configurations of
 * <a href="https://github.com/OpenHFT/Chronicle-Map#multiple-chronicle-maps---network-distributed">
 * multicontainer replication</a>, which is usable, when you want to replicate several {@linkplain
 * ChronicleHash ChronicleHashes} ({@linkplain ChronicleMap maps}, {@linkplain ChronicleSet sets},
 * etc.) between same servers. {@code ReplicationHub} allows to share TCP/UDP connection, sockets,
 * buffers, worker threads for that, considerably reducing resources usage and increasing cumulative
 * (for all replicated containers) replication throughput. See
 * <a href="https://github.com/OpenHFT/Chronicle-Map#multiple-chronicle-maps---network-distributed">
 * the corresponding section in ChronicleMap manual</a> for more information.
 *
 * <p>Create instances of this class using this pattern: <pre>{@code
 * ReplicationHub hub = ReplicationHub.builder()
 *     .tcpTransportAndNetwork(tcpConfig)
 *     // more configurations...
 *     .createWithId(identifierOfThisServerWithinTheChronicleReplicationNetwork);}</pre>
 * Then, given you prepared a {@link ChronicleHashBuilder builder} to create a {@link ChronicleHash
 * }, configure it's replication via channel of this hub like this: <pre>{@code
 * ChronicleMap myMap = builder.instance()
 *     .replicatedViaChannel(hub.createChannel(myMapChannelId))
 *     .persistedTo(myMapFile) // optional, for this example
 *     .create();}</pre>
 *
 * @see ChronicleHashInstanceConfig#replicatedViaChannel(ReplicationChannel)
 * @see ReplicationChannel
 */
public final class ReplicationHub extends AbstractReplication {

    /**
     * Creates and returns a new {@link ReplicationHub.Builder}.
     *
     * @return a new {@link ReplicationHub.Builder}
     */
    @NotNull
    public static Builder builder() {
        return new Builder();
    }

    private final int maxEntrySize;
    private final ReplicationChannel[] channels;

    private ReplicationHub(byte localIdentifier, Builder builder) {
        super(localIdentifier, builder);
        maxEntrySize = builder.maxEntrySize;
        channels = new ReplicationChannel[builder.maxNumberOfChronicles];
    }

    /**
     * Returns the maximum {@linkplain ChronicleHashBuilder#entrySize(int) entry size} of {@link
     * ChronicleHash}es, replicated by this {@code ReplicationHub}, could have. {@code
     * RuntimeException} is thrown on attempt to replicate with this {@code ReplicationHub}
     * a {@code ChronicleHash} with larger entry size.
     *
     * @return the maximum entry size
     * @see Builder#maxEntrySize(int)
     */
    public int maxEntrySize() {
        return maxEntrySize;
    }

    /**
     * Returns the maximum number of channels could be {@linkplain #createChannel(short) created}
     * for this {@code ReplicationHub}.
     *
     * @return the maximum number of channels
     * @see Builder#maxNumberOfChannels()
     * @see #createChannel(short)
     */
    public int maxNumberOfChannels() {
        return channels.length;
    }

    /**
     * Creates a new {@link ReplicationChannel} in this {@code ReplicationHub} with the given
     * identifier. Identifier shouldn't be lesser than zero and greater or equal to
     * {@code maxNumberOfChannels() - 1}. On a {@code ReplicationHub} instance,
     * {@code ReplicationChannel} could be created only once for each possible {@code channelId}
     * value.
     *
     * @param channelId the identifier of the channel. Should be equal for replicated containers
     *                  on different nodes (servers)
     * @return a new {@code ReplicationChannel} instance, in fact just incapsulating the given
     *         {@code channelId} for this {@code ReplicationHub}
     * @throws IllegalArgumentException if the specified {@code channelId} is out of<br>
     *         <code>[0, {@link #maxNumberOfChannels()})</code> range
     * @throws IllegalStateException if {@code ReplicationChannel} with the specified
     *         {@code channelId} has already been acquired on this {@code ReplicationHub}
     * @see ReplicationChannel
     */
    public synchronized ReplicationChannel createChannel(short channelId) {
        if (channelId < 0)
            throw new IllegalArgumentException("channelId should be positive");
        if (channelId >= maxNumberOfChannels())
            throw new IllegalArgumentException("maxNumberOfChannels is configured (or defaulted) " +
                    "to " + maxNumberOfChannels() + ", channelId=" + channelId + " is requested");
        if (channels[channelId] != null)
            throw new IllegalStateException("The requested channelId=" + channelId +
                    " is already in use");
        ReplicationChannel channel = new ReplicationChannel(this, channelId);
        channels[channelId] = channel;
        return channel;
    }

    /**
     * Builder of {@link ReplicationHub}s.
     */
    public static final class Builder extends AbstractReplication.Builder<ReplicationHub, Builder> {
        private int maxEntrySize = 1024;
        private int maxNumberOfChronicles = 128;

        private Builder() {}

        /**
         * Configures the maximum {@linkplain ChronicleHashBuilder#entrySize(int) entry size} of
         * {@link ChronicleHash}es, replicated via {@code ReplicationHub}s, created by this builder.
         *
         * <p>Default value is {@code 1024}.
         *
         * @param maxEntrySize {@link ReplicationHub#maxEntrySize()} of {@link ReplicationHub}s,
         *                     created by this builder
         * @return this builder object back, for chaining
         * @see ReplicationHub#maxEntrySize()
         */
        @NotNull
        public Builder maxEntrySize(int maxEntrySize) {
            this.maxEntrySize = maxEntrySize;
            return this;
        }

        /**
         * Configures the maximum number of channels could be {@linkplain #createChannel(short)
         * created for {@code ReplicationHub}s, created by this builder.
         *
         * <p>Default value is {@code 128}.
         *
         * @param maxNumberOfChannels {@link ReplicationHub#maxNumberOfChannels()} of
         *                            {@link ReplicationHub}s, created by this builder
         * @return this builder object back, for chaining
         * @see ReplicationHub#maxNumberOfChannels()
         */
        @NotNull
        public Builder maxNumberOfChannels(int maxNumberOfChannels) {
            this.maxNumberOfChronicles = maxNumberOfChannels;
            return this;
        }

        @NotNull
        @Override
        public ReplicationHub createWithId(byte identifier) {
            check(identifier);
            return new ReplicationHub(identifier, this);
        }
    }
}
