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

package net.openhft.chronicle.hash.impl;


import net.openhft.chronicle.hash.ChronicleHash;
import net.openhft.chronicle.hash.ChronicleHashBuilder;
import net.openhft.chronicle.hash.serialization.internal.SerializationBuilder;

//TODO remove this temporary interface
public interface ChronicleHashBuilderImpl<K, H extends ChronicleHash<K, ?, ?>,
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
