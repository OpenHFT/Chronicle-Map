/*
 *      Copyright (C) 2015  higherfrequencytrading.com
 *
 *      This program is free software: you can redistribute it and/or modify
 *      it under the terms of the GNU Lesser General Public License as published by
 *      the Free Software Foundation, either version 3 of the License.
 *
 *      This program is distributed in the hope that it will be useful,
 *      but WITHOUT ANY WARRANTY; without even the implied warranty of
 *      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *      GNU Lesser General Public License for more details.
 *
 *      You should have received a copy of the GNU Lesser General Public License
 *      along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.hash.impl;


import net.openhft.chronicle.hash.ChronicleHash;
import net.openhft.chronicle.hash.ChronicleHashBuilder;
import net.openhft.chronicle.hash.serialization.internal.SerializationBuilder;

//TODO remove this temporary interface
public interface ChronicleHashBuilderImpl<K, H extends ChronicleHash<K, ?, ?, ?>,
        B extends ChronicleHashBuilder<K, H, B>> extends ChronicleHashBuilder<K, H, B> {

    SerializationBuilder<K> keyBuilder();

    int actualSegments(boolean replicated);

    long entriesPerSegment(boolean replicated);

    long chunkSize(boolean replicated);

    int segmentHeaderSize(boolean replicated);

    long actualChunksPerSegment(boolean replicated);

    int maxChunksPerEntry();

    int segmentEntrySpaceInnerOffset(boolean replicated);
}
