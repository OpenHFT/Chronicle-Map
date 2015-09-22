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

package net.openhft.chronicle.hash;

import net.openhft.chronicle.hash.serialization.internal.SerializationBuilder;

public interface ChronicleHashBuilderPrivateAPI<K> {

    SerializationBuilder<K> keyBuilder();

    int segmentEntrySpaceInnerOffset(boolean replicated);

    long chunkSize(boolean replicated);

    int maxChunksPerEntry(boolean replicated);

    long entriesPerSegment(boolean replicated);

    long actualChunksPerSegment(boolean replicated);

    int segmentHeaderSize(boolean replicated);

    int actualSegments(boolean replicated);

    long maxExtraTiers(boolean replicated);

    boolean aligned64BitMemoryOperationsAtomic();
}
