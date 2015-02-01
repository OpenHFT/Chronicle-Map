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

package net.openhft.chronicle.hash.serialization.internal;

import net.openhft.chronicle.hash.hashing.Hasher;
import net.openhft.chronicle.hash.serialization.BytesInterop;
import net.openhft.lang.io.Bytes;

public enum BytesBytesInterop implements BytesInterop<Bytes> {
    INSTANCE;

    @Override
    public boolean startsWith(Bytes bytes, Bytes e) {
        long limit = bytes.limit();
        bytes.limit(bytes.capacity());
        boolean result = bytes.startsWith(e);
        bytes.limit(limit);
        return result;
    }

    @Override
    public long hash(Bytes e) {
        return Hasher.hash(e);
    }

    @Override
    public long size(Bytes e) {
        return e.remaining();
    }

    @Override
    public void write(Bytes bytes, Bytes e) {
        bytes.write(e, e.position(), e.remaining());
    }
}
