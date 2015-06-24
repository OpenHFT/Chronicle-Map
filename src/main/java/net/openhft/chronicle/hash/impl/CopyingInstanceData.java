/*
 * Copyright 2015 Higher Frequency Trading
 *
 *  http://www.higherfrequencytrading.com
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package net.openhft.chronicle.hash.impl;

import net.openhft.chronicle.bytes.RandomDataInput;
import net.openhft.chronicle.hash.AbstractData;
import net.openhft.lang.io.DirectBytes;
import net.openhft.lang.io.DirectStore;
import net.openhft.lang.io.serialization.JDKObjectSerializer;

public abstract class CopyingInstanceData<I> extends AbstractData<I> {

    private final JavaLangBytesReusableBytesStore bytesStore =
            new JavaLangBytesReusableBytesStore();

    public DirectBytes getBuffer(DirectBytes b, long capacity) {
        if (b != null) {
            if (b.capacity() >= capacity) {
                return (DirectBytes) b.clear();
            } else {
                DirectStore store = (DirectStore) b.store();
                store.resize(capacity, false);
                DirectBytes bytes = store.bytes();
                bytesStore.setBytes(bytes);
                return bytes;
            }
        } else {
            DirectBytes bytes = new DirectStore(JDKObjectSerializer.INSTANCE,
                    Math.max(1, capacity), true).bytes();
            bytesStore.setBytes(bytes);
            return bytes;
        }
    }

    @Override
    public RandomDataInput bytes() {
        return bytesStore;
    }

    public abstract I instance();

    public abstract DirectBytes buffer();

    @Override
    public long offset() {
        return 0;
    }

    @Override
    public long size() {
        return buffer().limit();
    }

    @Override
    public I get() {
        return instance();
    }
}
