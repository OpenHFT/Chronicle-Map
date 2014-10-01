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

package net.openhft.chronicle.map.serialization;

import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.DirectBytes;
import net.openhft.lang.io.DirectStore;
import net.openhft.lang.io.serialization.JDKObjectSerializer;
import net.openhft.chronicle.map.threadlocal.StatefulCopyable;

final class DirectBytesBuffer implements StatefulCopyable<DirectBytesBuffer> {

    private final Object identity;
    private DirectBytes buffer;

    DirectBytesBuffer(Object identity) {
        this.identity = identity;
    }

    private Bytes buffer(long maxSize) {
        DirectBytes buf;
        if ((buf = buffer) != null) {
            if (maxSize <= buf.capacity()) {
                return buf.clear();
            } else {
                DirectStore store = (DirectStore) buf.store();
                store.resize(maxSize, false);
                return buffer = store.bytes();
            }
        } else {
            return buffer = new DirectStore(JDKObjectSerializer.INSTANCE, maxSize, false).bytes();
        }
    }

    @Override
    public Object stateIdentity() {
        return identity;
    }

    @Override
    public DirectBytesBuffer copy() {
        return new DirectBytesBuffer(identity);
    }
}
