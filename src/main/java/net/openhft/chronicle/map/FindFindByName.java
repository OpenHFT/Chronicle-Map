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
import net.openhft.chronicle.hash.ChronicleHashInstanceConfig;
import net.openhft.chronicle.hash.FindByName;
import net.openhft.chronicle.hash.replication.ReplicationHub;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static net.openhft.chronicle.map.ChronicleMapBuilder.of;

/**
 * @author Rob Austin.
 */
class FindFindByName implements FindByName {

    public static final int MAP_BY_NAME_CHANNEL = 1;
    private final ReplicationHub replicationHub;
    int maxEntrySize = 10 * 1024;

    private final AtomicInteger nextFreeChannel = new AtomicInteger(1);
    private final Map<String, ChronicleMapBuilder> map;

    /**
     * @throws IOException
     */
    public FindFindByName(ReplicationHub replicationHub) throws IOException {
        this.replicationHub = replicationHub;
        this.map = (Map) of(CharSequence.class, ChronicleMapBuilder.class)
                .entrySize(300)
                .instance()
                .replicatedViaChannel(replicationHub.createChannel((short) MAP_BY_NAME_CHANNEL)).create();
    }

    ChronicleMapBuilder get(String name) throws IllegalArgumentException {

        ChronicleMapBuilder chronicleMapBuilder = map.get(name);
        if (chronicleMapBuilder == null)
            throw new IllegalArgumentException("A map name=" + name + " can not be found.");

        return chronicleMapBuilder;
    }


    public <T extends ChronicleHash> T create(String name) throws IllegalArgumentException,
            IOException {
        return (T) replicatedViaChannel(name).create();
    }

    private ChronicleHashInstanceConfig replicatedViaChannel(String name) {
        return toReplicatedViaChannel(get(name));
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
            IOException {
        return (T) replicatedViaChannel(name).persistedTo(file).create();
    }

    private ChronicleHashInstanceConfig toReplicatedViaChannel(ChronicleMapBuilder builder) {
        int channelId = nextFreeChannel.incrementAndGet();
        if (channelId > replicationHub.maxNumberOfChannels())
            throw new IllegalStateException("There are no more free channels, you can increase the number " +
                    "of changes in the replicationHub by calling replicationHub.maxNumberOfChannels(..);");
        return builder.instance().replicatedViaChannel(replicationHub.createChannel((short) channelId));
    }

    public void add(ChronicleMapBuilder builder) {
        this.map.put(builder.name(), builder);
    }
}
