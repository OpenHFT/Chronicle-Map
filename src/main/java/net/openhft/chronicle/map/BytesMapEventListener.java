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

import net.openhft.chronicle.hash.serialization.SizeMarshaller;
import net.openhft.lang.io.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * <ul>
 *     <li>Methods must not update {@code entry} state ({@linkplain Bytes#position(long) position},
 *     {@linkplain Bytes#limit() limit}).</li>
 *     <li>Methods must not update entry bytes, except in metadata area</li>
 *     <li>From {@code metaDataPos} offset metadata area starts in the given {@code entry}, listener
 *     should be itself aware of it's length (see {@link ChronicleMapBuilder#metaDataBytes()}).</li>
 *     <li>From {@code keyPos} offset key area in the given {@code entry}, serialized key size
 *     (using {@linkplain ChronicleMapBuilder#keySizeMarshaller(SizeMarshaller)}), directly followed
 *     by the serialized key itself.</li>
 *     <li>From {@code valuePos} offset value area in the given {@code entry}, serialized value size
 *     (using {@linkplain ChronicleMapBuilder#keySizeMarshaller(SizeMarshaller)}), then (optionally)
 *     alignment (see {@link OffHeapUpdatableChronicleMapBuilder#entryAndValueAlignment(Alignment)}
 *     ), followed by the serialized value itself.</li>
 * </ul>
 */
public abstract class BytesMapEventListener implements Serializable {
    private static final long serialVersionUID = 0L;

    public void onGetFound(Bytes entry, long metaDataPos, long keyPos, long valuePos) {
        // do nothing
    }

    public void onPut(Bytes entry, long metaDataPos, long keyPos, long valuePos, boolean added) {
        // do nothing
    }

    public void onRemove(Bytes entry, long metaDataPos, long keyPos, long valuePos) {
        // do nothing
    }
}
