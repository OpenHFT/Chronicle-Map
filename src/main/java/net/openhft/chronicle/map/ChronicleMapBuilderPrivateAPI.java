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

package net.openhft.chronicle.map;

import net.openhft.chronicle.hash.ChronicleHashBuilderPrivateAPI;
import net.openhft.chronicle.hash.serialization.impl.SerializationBuilder;

class ChronicleMapBuilderPrivateAPI<K> implements ChronicleHashBuilderPrivateAPI<K> {

    private ChronicleMapBuilder<K, ?> b;

    public ChronicleMapBuilderPrivateAPI(ChronicleMapBuilder<K, ?> b) {
        this.b = b;
    }

    @Override
    public SerializationBuilder<K> keyBuilder() {
        return b.keyBuilder();
    }

    @Override
    public int segmentEntrySpaceInnerOffset() {
        return b.segmentEntrySpaceInnerOffset();
    }

    @Override
    public long chunkSize() {
        return b.chunkSize();
    }

    @Override
    public int maxChunksPerEntry() {
        return b.maxChunksPerEntry();
    }

    @Override
    public long entriesPerSegment() {
        return b.entriesPerSegment();
    }

    @Override
    public long actualChunksPerSegmentTier() {
        return b.actualChunksPerSegmentTier();
    }

    @Override
    public int segmentHeaderSize() {
        return b.segmentHeaderSize();
    }

    @Override
    public int actualSegments() {
        return b.actualSegments();
    }

    @Override
    public long maxExtraTiers() {
        return b.maxExtraTiers();
    }

    @Override
    public boolean aligned64BitMemoryOperationsAtomic() {
        return b.aligned64BitMemoryOperationsAtomic();
    }

    @Override
    public boolean checksumEntries() {
        return b.checksumEntries();
    }
}
