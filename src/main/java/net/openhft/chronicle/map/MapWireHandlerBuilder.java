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

package net.openhft.chronicle.map;

import net.openhft.chronicle.hash.ChronicleHashInstanceBuilder;
import net.openhft.chronicle.hash.replication.ReplicationHub;
import net.openhft.chronicle.network2.WireHandler;
import net.openhft.chronicle.wire.WireKey;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.function.Supplier;

/**
 * Created by Rob Austin
 */
public class MapWireHandlerBuilder {

    public static <K, V> WireHandler of(
            @NotNull final Supplier<ChronicleHashInstanceBuilder<ChronicleMap<K, V>>> chronicleHashInstanceBuilder,
            @NotNull final ReplicationHub hub,
            byte localIdentifier,
            @NotNull final List<Replica> channelList) {
        return new MapWireHandler<K, V>(chronicleHashInstanceBuilder, hub, localIdentifier, channelList);
    }

    public static enum Fields implements WireKey {
        HAS_NEXT,
        TIME_STAMP,
        CHANNEL_ID,
        METHOD_NAME,
        TYPE,
        TRANSACTION_ID,
        RESULT,
        RESULT_KEY,
        RESULT_VALUE,
        ARG_1,
        ARG_2,
        ARG_3,
        IS_EXCEPTION,
        EXCEPTION,
        RESULT_IS_NULL
    }
}

