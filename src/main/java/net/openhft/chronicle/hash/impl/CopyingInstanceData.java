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

import net.openhft.chronicle.bytes.ReadAccess;
import net.openhft.chronicle.hash.AbstractData;
import net.openhft.lang.io.DirectBytes;
import net.openhft.lang.io.DirectStore;
import net.openhft.lang.io.serialization.JDKObjectSerializer;

import static net.openhft.chronicle.hash.impl.JavaLangBytesAccessors.uncheckedBytesAccessor;

public abstract class CopyingInstanceData<I, T> extends AbstractData<I, T> {

    public static DirectBytes getBuffer(DirectBytes b, long capacity) {
        if (b != null) {
            if (b.capacity() >= capacity) {
                return (DirectBytes) b.clear();
            } else {
                DirectStore store = (DirectStore) b.store();
                store.resize(capacity, false);
                return store.bytes();
            }
        } else {
            return new DirectStore(JDKObjectSerializer.INSTANCE, Math.max(1, capacity), true)
                    .bytes();
        }
    }

    public abstract I instance();

    public abstract DirectBytes buffer();

    @Override
    public ReadAccess<T> access() {
        //noinspection unchecked
        return (ReadAccess<T>) uncheckedBytesAccessor(buffer()).access(buffer());
    }

    @Override
    public T handle() {
        //noinspection unchecked
        return (T) uncheckedBytesAccessor(buffer()).handle(buffer());
    }

    @Override
    public long offset() {
        return uncheckedBytesAccessor(buffer()).offset(buffer(), 0);
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
