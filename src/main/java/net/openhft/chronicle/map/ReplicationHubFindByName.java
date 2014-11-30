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

import net.openhft.chronicle.hash.ChronicleHash;
import net.openhft.chronicle.hash.ChronicleHashBuilder;
import net.openhft.chronicle.hash.ChronicleHashInstanceConfig;
import net.openhft.chronicle.hash.FindByName;
import net.openhft.chronicle.hash.replication.ReplicationChannel;
import net.openhft.chronicle.hash.replication.ReplicationHub;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static net.openhft.chronicle.map.ChronicleMapBuilder.of;

/**
 * @author Rob Austin.
 */
class ReplicationHubFindByName<K> implements FindByName {
    public static final Logger LOG = LoggerFactory.getLogger(ReplicationHubFindByName.class.getName());

    public static final int MAP_BY_NAME_CHANNEL = 1;

    private final AtomicInteger nextFreeChannel = new AtomicInteger(2);
    private final Map<String, ChronicleMapBuilderWithChannelId> map;
    private final ReplicationHub replicationHub;

    public static class ChronicleMapBuilderWithChannelId<K, V> implements Serializable {
        ChronicleHashBuilder<K, ChronicleMap<K, V>, ?> chronicleMapBuilder;
        int channelId;
    }

    /**
     * @throws IOException
     */
    public ReplicationHubFindByName(ReplicationHub replicationHub) throws IOException {

        LOG.info("connecting to replicationHub=" + replicationHub);

        this.replicationHub = replicationHub;
        ReplicationChannel channel = replicationHub.createChannel((short) MAP_BY_NAME_CHANNEL);

        final MapEventListener<CharSequence, ChronicleMapBuilderWithChannelId> listener = new
                MapEventListener<CharSequence, ChronicleMapBuilderWithChannelId>() {

                    // creates a map based on the details that are sent to the map of builders
                    @Override
                    public void onPut(CharSequence key, ChronicleMapBuilderWithChannelId value,
                               ChronicleMapBuilderWithChannelId replacedValue) {
                        super.onPut(key, value, replacedValue);
                        boolean added = replacedValue == null;
                        if (!added || value == null)
                            return;

                        // establish the new map based on this channel ID
                        LOG.info("create new map for name=" + value.chronicleMapBuilder.name()
                                + ",channelId=" + value.channelId);

                        try {
                            toReplicatedViaChannel(value.chronicleMapBuilder, value.channelId).create();
                        } catch (IllegalStateException e) {
                            // channel is already created
                            LOG.debug("while creating channel for name=" + value.chronicleMapBuilder.name()
                                    + ",channelId=" + value.channelId, e);
                        } catch (IOException e) {
                            LOG.error("", e);
                        }

                    }

                };

        this.map = (Map) of(CharSequence.class, ChronicleMapBuilderWithChannelId.class)
                .entrySize(3000)
                .entries(128)
                .eventListener(listener)
                .instance()
                .replicatedViaChannel(channel)
                .create();
        if (LOG.isDebugEnabled())
            LOG.debug("map=" + map);
    }

    public <T extends ChronicleHash> T create(ChronicleMapBuilder<CharSequence, CharSequence> builder) throws IllegalArgumentException,
            IOException, TimeoutException, InterruptedException {

        int withChannelId = nextFreeChannel.incrementAndGet();
        ChronicleMapBuilderWithChannelId builderWithChannelId = new
                ChronicleMapBuilderWithChannelId();
        builderWithChannelId.channelId = withChannelId;
        builderWithChannelId.chronicleMapBuilder = (ChronicleHashBuilder)builder;

        map.put(builder.name(), builderWithChannelId);

        return (T) get(builder.name()).chronicleMapBuilder.create();

    }

    public <T extends ChronicleHash> T from(String name) throws IllegalArgumentException,
            IOException, TimeoutException, InterruptedException {
        return (T) replicatedViaChannel(name).create();
    }

    ChronicleMapBuilderWithChannelId get(String name) throws IllegalArgumentException,
            TimeoutException,
            InterruptedException {

        ChronicleMapBuilderWithChannelId chronicleMapBuilder = waitTillEntryReceived(5000, name);
        if (chronicleMapBuilder == null)
            throw new IllegalArgumentException("A map name=" + name + " can not be found.");

        return chronicleMapBuilder;
    }

    /**
     * * waits until map1 and map2 show the same value
     *
     * @param timeOutMs timeout in milliseconds
     * @throws InterruptedException
     */
    private ChronicleMapBuilderWithChannelId waitTillEntryReceived(final int timeOutMs, String name) throws TimeoutException, InterruptedException {
        int t = 0;
        for (; t < timeOutMs; t++) {
            ChronicleMapBuilderWithChannelId builder = map.get(name);

            if (builder != null)
                return builder;
            Thread.sleep(1);
        }
        throw new TimeoutException("timed out wait for map name=" + name);

    }

    private ChronicleHashInstanceConfig replicatedViaChannel(String name) throws TimeoutException, InterruptedException {
        ChronicleMapBuilderWithChannelId builder = get(name);
        return toReplicatedViaChannel(builder.chronicleMapBuilder, builder.channelId);
    }

    /**
     * similar to {@link  ChronicleMapBuilder#createPersistedTo(File file)  }
     *
     * @param name the name of the map to be created
     * @param file the file which will be used as shared off heap storage
     * @param <T>
     * @return a new instance of ChronicleMap
     * @throws IllegalArgumentException
     * @throws IOException
     * @see ChronicleMapBuilder#createPersistedTo(File file)
     */
    public <T extends ChronicleHash> T createPersistedTo(String name, File file) throws IllegalArgumentException,
            IOException, TimeoutException, InterruptedException {
        return (T) replicatedViaChannel(name).persistedTo(file).create();
    }

    private ChronicleHashInstanceConfig toReplicatedViaChannel(ChronicleMapBuilder builder) {
        int channelId = nextFreeChannel.incrementAndGet();
        if (channelId > replicationHub.maxNumberOfChannels())
            throw new IllegalStateException("There are no more free channels, you can increase the number " +
                    "of changes in the replicationHub by calling replicationHub.maxNumberOfChannels(..);");
        return builder.instance().replicatedViaChannel(replicationHub.createChannel((short) channelId));
    }

    private ChronicleHashInstanceConfig toReplicatedViaChannel(ChronicleHashBuilder<K, ?, ?> builder, int
            withChannelId) {

        return builder.instance().replicatedViaChannel(replicationHub.createChannel((short) withChannelId));
    }

}
