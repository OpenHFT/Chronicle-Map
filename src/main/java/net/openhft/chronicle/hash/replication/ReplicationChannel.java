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
import net.openhft.chronicle.hash.ChronicleHashInstanceConfig;
import org.jetbrains.annotations.NotNull;

/**
 * A token which should be created via {@link ReplicationHub#createChannel(int)} call and passed to {@link
 * ChronicleHashInstanceConfig#replicatedViaChannel(ReplicationChannel)} method, to establish a channel
 * replication. See <a href="https://github.com/OpenHFT/Chronicle-Map#multiple-chronicle-maps---network-distributed">
 * the corresponding section in ChronicleMap manual</a> for more information.
 *
 * <p>A {@code ReplicationChannel} could be used to replicate only one {@link ChronicleHash} instance.
 *
 * @see ReplicationHub
 * @see ReplicationHub#createChannel(int)
 */
public final class ReplicationChannel {
    private ReplicationHub hub;
    private final int channelId;

    ReplicationChannel(ReplicationHub hub, int channelId) {
        this.hub = hub;
        this.channelId = channelId;
    }

    /**
     * Returns the {@link ReplicationHub} on which this {@code ReplicationChannel} was {@linkplain
     * ReplicationHub#createChannel(int) created}.
     *
     * @return the {@code ReplicationHub} to which this {@code ReplicationChannel} belongs
     * @see ReplicationHub#createChannel(int)
     */
    @NotNull
    public ReplicationHub hub() {
        return hub;
    }

    /**
     * Returns the identifier of this channel, with which was {@linkplain ReplicationHub#createChannel(int)
     * created}.
     *
     * @return the identifier of this channel
     * @see ReplicationHub#createChannel(int)
     */
    public int channelId() {
        return channelId;
    }
}
