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

package net.openhft.chronicle.map.fromdocs;

import net.openhft.chronicle.hash.serialization.BytesWriter;
import net.openhft.lang.io.Bytes;

enum LongPairArrayWriter implements BytesWriter<LongPair[]> {
    INSTANCE;

    @Override
    public long size(LongPair[] longPairs) {
        return longPairs.length * 16L;
    }

    @Override
    public void write(Bytes bytes, LongPair[] longPairs) {
        for (LongPair pair : longPairs) {
            bytes.writeLong(pair.first);
            bytes.writeLong(pair.second);
        }
    }
}
