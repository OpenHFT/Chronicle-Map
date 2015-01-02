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
import net.openhft.chronicle.hash.ChronicleHashInstanceBuilder;
import net.openhft.chronicle.hash.replication.ReplicationChannel;
import net.openhft.chronicle.hash.replication.ReplicationHub;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Rob Austin.
 */
class ReplicationHubFindByName<K> implements FindByName {
    public static final Logger LOG = LoggerFactory.getLogger(ReplicationHubFindByName.class.getName());

    public static final int MAP_BY_NAME_CHANNEL = 1;

    private final AtomicInteger nextFreeChannel = new AtomicInteger(2);
    private final Map<String, MapInstanceBuilder> map;
    private final ReplicationHub replicationHub;

    /**
     * @throws IOException
     */
    public ReplicationHubFindByName(ReplicationHub replicationHub) throws IOException {

        LOG.info("connecting to replicationHub=" + replicationHub);

        this.replicationHub = replicationHub;
        ReplicationChannel channel = replicationHub.createChannel((short) MAP_BY_NAME_CHANNEL);

        final MapEventListener<CharSequence, MapInstanceBuilder> listener =
                new MapEventListener<CharSequence, MapInstanceBuilder>() {
                    // creates a map based on the details that are sent to the map of builders
                    @Override
                    public void onPut(CharSequence key, MapInstanceBuilder value,
                                      MapInstanceBuilder replacedValue) {
                        super.onPut(key, value, replacedValue);
                        boolean added = replacedValue == null;
                        if (!added || value == null)
                            return;

                        // establish the new map based on this channel ID
                        LOG.info("create new map for name=" + value.name
                                + ",channelId=" + value.channel.channelId());

                        try {
                            toReplicatedViaChannel(value.mapBuilder, value.channel.channelId()).create();
                        } catch (IllegalStateException e) {
                            // channel is already created
                            LOG.debug("while creating channel for name=" + value.name
                                    + ",channelId=" + value.channel.channelId(), e);
                        } catch (IOException e) {
                            LOG.error("", e);
                        }
                    }
                };

        this.map = (Map) ChronicleMapBuilder
                .of(CharSequence.class, MapInstanceBuilder.class)
                .averageKeySize(10)
                .averageValueSize(4000)
                .entries(128)
                .eventListener(listener)
                .instance()
                .replicatedViaChannel(channel)
                .create();
        if (LOG.isDebugEnabled())
            LOG.debug("map=" + map);
    }

    public <T extends ChronicleHash> T create(
            MapInstanceBuilder<CharSequence, CharSequence> config)
            throws IOException, TimeoutException, InterruptedException {

        int withChannelId = nextFreeChannel.incrementAndGet();

        map.put(config.name,
                config.replicatedViaChannel(replicationHub.createChannel(withChannelId)));

        return (T) get(config.name).mapBuilder.create();
    }

    public <T extends ChronicleHash> T from(String name) throws IllegalArgumentException,
            IOException, TimeoutException, InterruptedException {
        return (T) get(name).create();
    }

    MapInstanceBuilder get(String name) throws IllegalArgumentException,
            TimeoutException,
            InterruptedException {

        MapInstanceBuilder config = waitTillEntryReceived(5000, name);
        if (config == null)
            throw new IllegalArgumentException("A map name=" + name + " can not be found.");

        return config;
    }

    /**
     * * waits until map1 and map2 show the same value
     *
     * @param timeOutMs timeout in milliseconds
     * @throws InterruptedException
     */
    private MapInstanceBuilder waitTillEntryReceived(final int timeOutMs, String name)
            throws TimeoutException, InterruptedException {
        int t = 0;
        for (; t < timeOutMs; t++) {
            MapInstanceBuilder config = map.get(name);

            if (config != null)
                return config;
            Thread.sleep(1);
        }
        throw new TimeoutException("timed out wait for map name=" + name);
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
    public <T extends ChronicleHash> T createPersistedTo(String name, File file)
            throws IllegalArgumentException,
            IOException, TimeoutException, InterruptedException {
        return (T) get(name).persistedTo(file).create();
    }

    private ChronicleHashInstanceBuilder toReplicatedViaChannel(ChronicleMapBuilder builder) {
        int channelId = nextFreeChannel.incrementAndGet();
        if (channelId > replicationHub.maxNumberOfChannels())
            throw new IllegalStateException("There are no more free channels, you can increase the number " +
                    "of changes in the replicationHub by calling replicationHub.maxNumberOfChannels(..);");
        return builder.instance().replicatedViaChannel(replicationHub.createChannel((short) channelId));
    }

    private ChronicleHashInstanceBuilder toReplicatedViaChannel(ChronicleHashBuilder<K, ?, ?> builder, int
            withChannelId) {
        return builder.instance().replicatedViaChannel(replicationHub.createChannel((short) withChannelId));
    }

}
