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
import net.openhft.chronicle.hash.serialization.internal.SerializationBuilder;

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
    public int segmentEntrySpaceInnerOffset(boolean replicated) {
        return b.segmentEntrySpaceInnerOffset(replicated);
    }

    @Override
    public long chunkSize(boolean replicated) {
        return b.chunkSize(replicated);
    }

    @Override
    public int maxChunksPerEntry(boolean replicated) {
        return b.maxChunksPerEntry(replicated);
    }

    @Override
    public long entriesPerSegment(boolean replicated) {
        return b.entriesPerSegment(replicated);
    }

    @Override
    public long actualChunksPerSegment(boolean replicated) {
        return b.actualChunksPerSegment(replicated);
    }

    @Override
    public int segmentHeaderSize(boolean replicated) {
        return b.segmentHeaderSize(replicated);
    }

    @Override
    public int actualSegments(boolean replicated) {
        return b.actualSegments(replicated);
    }

    @Override
    public long maxExtraTiers(boolean replicated) {
        return b.maxExtraTiers(replicated);
    }
}
