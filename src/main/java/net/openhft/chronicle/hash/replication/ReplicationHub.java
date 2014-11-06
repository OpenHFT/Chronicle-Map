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

import net.openhft.chronicle.hash.ReplicationConfig;

import java.io.IOException;

public final class ReplicationHub extends AbstractReplication {

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

    public int maxEntrySize() {
        return maxEntrySize;
    }

    public int maxNumberOfChannels() {
        return channels.length;
    }

    public synchronized ReplicationChannel createChannel(short channelId) {
        if (channelId < 0)
            throw new IllegalArgumentException("channelId should be positive");
        if (channelId >= maxNumberOfChannels())
            throw new IllegalArgumentException("maxNumberOfChannels is configured (or defaulted) " +
                    "to " + maxNumberOfChannels() + ", channelId=" + channelId + " is requested");
        if (channels[channelId] != null)
            throw new IllegalArgumentException("The requested channelId=" + channelId +
                    " is already in use");
        ReplicationChannel channel = new ReplicationChannel(this, channelId);
        channels[channelId] = channel;
        return channel;
    }

    public static final class Builder extends AbstractReplication.Builder<Builder> {

        private int maxEntrySize = 1024;
        private int maxNumberOfChronicles = 128;

        private Builder() {}

        public Builder maxEntrySize(int maxEntrySize) {
            this.maxEntrySize = maxEntrySize;
            return this;
        }

        public Builder maxNumberOfChannels(int maxNumberOfChannels) {
            this.maxNumberOfChronicles = maxNumberOfChannels;
            return this;
        }

        public ReplicationHub create(byte identifier) throws IOException {
            check();
            return new ReplicationHub(identifier, this);
        }
    }
}
