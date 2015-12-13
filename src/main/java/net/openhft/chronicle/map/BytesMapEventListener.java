/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.map;

import net.openhft.chronicle.hash.serialization.SizeMarshaller;
import net.openhft.lang.io.Bytes;

import java.io.Serializable;

/**
 * <ul>
 *     <li>Methods must not update {@code entry} state ({@linkplain Bytes#position(long) position},
 *     {@linkplain Bytes#limit() limit}).</li>
 *     <li>Methods must not update entry bytes, except in metadata area</li>
 *     <li>From {@code metaDataPos} offset metadata area starts in the given {@code entry}, listener
 *     should be itself aware of it's length (see {@link ChronicleMapBuilder#metaDataBytes(int)}).</li>
 *     <li>From {@code keyPos} offset key area in the given {@code entry}, serialized key size
 *     (using {@linkplain ChronicleMapBuilder#keySizeMarshaller(SizeMarshaller)}), directly followed
 *     by the serialized key itself.</li>
 *     <li>From {@code valuePos} offset value area in the given {@code entry}, serialized value size
 *     (using {@linkplain ChronicleMapBuilder#keySizeMarshaller(SizeMarshaller)}), then (optionally)
 *     alignment (see {@link ChronicleMapBuilder#entryAndValueAlignment(Alignment)}
 *     ), followed by the serialized value itself.</li>
 * </ul>
 *
 * <p>There are helper methods {@link ChronicleMap#readKey(Bytes, long)} and
 * {@link ChronicleMap#readValue(Bytes, long)} that help to deal with this.
 */
public abstract class BytesMapEventListener implements Serializable {
    private static final long serialVersionUID = 0L;

    public void onGetFound(Bytes entry, long metaDataPos, long keyPos, long valuePos) {
        // do nothing
    }

    public void onPut(Bytes entry, long metaDataPos, long keyPos, long valuePos, boolean added,
                      boolean replicationEvent, boolean hasValueChanged) {
        // do nothing
    }

    public void onRemove(Bytes entry, long metaDataPos, long keyPos, long valuePos,
                         boolean replicationEvent) {
        // do nothing
    }
}
