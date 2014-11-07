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

import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.lang.io.Bytes;

import java.util.Arrays;

enum LongPairArrayReader implements BytesReader<LongPair[]> {
    INSTANCE;

    @Override
    public LongPair[] read(Bytes bytes, long size) {
        return read(bytes, size, null);
    }

    @Override
    public LongPair[] read(Bytes bytes, long size, LongPair[] toReuse) {
        int resLen = (int) (size / 16L);
        LongPair[] res;
        if (toReuse != null) {
            if (toReuse.length == resLen) {
                res = toReuse;
            } else {
                res = Arrays.copyOf(toReuse, resLen);
            }
        } else {
            res = new LongPair[resLen];
        }
        for (int i = 0; i < resLen; i++) {
            LongPair pair = res[i];
            if (pair == null)
                res[i] = pair = new LongPair();
            pair.first = bytes.readLong();
            pair.second = bytes.readLong();
        }
        return res;
    }
}
