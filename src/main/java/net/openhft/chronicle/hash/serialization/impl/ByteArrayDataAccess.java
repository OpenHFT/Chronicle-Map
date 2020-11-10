/*
 * Copyright 2012-2018 Chronicle Map Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.hash.serialization.impl;

import net.openhft.chronicle.bytes.HeapBytesStore;
import net.openhft.chronicle.bytes.RandomDataInput;
import net.openhft.chronicle.hash.AbstractData;
import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.serialization.DataAccess;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class ByteArrayDataAccess extends AbstractData<byte[]> implements DataAccess<byte[]> {

    /**
     * Cache field
     */
    private transient HeapBytesStore<byte[]> bs;

    /**
     * State field
     */
    private transient byte[] array;

    public ByteArrayDataAccess() {
        initTransients();
    }

    private void initTransients() {
        bs = null;
    }

    @Override
    public RandomDataInput bytes() {
        return bs;
    }

    @Override
    public long offset() {
        return bs.start();
    }

    @Override
    public long size() {
        return bs.capacity();
    }

    @Override
    public byte[] get() {
        return array;
    }

    @Override
    public byte[] getUsing(@Nullable byte[] using) {
        if (using == null || using.length != array.length)
            using = new byte[array.length];
        System.arraycopy(array, 0, using, 0, array.length);
        return using;
    }

    @Override
    public Data<byte[]> getData(@NotNull byte[] instance) {
        array = instance;
        bs = HeapBytesStore.wrap(array);
        return this;
    }

    @Override
    public void uninit() {
        array = null;
        bs = null;
    }

    @Override
    public DataAccess<byte[]> copy() {
        return new ByteArrayDataAccess();
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wireOut) {
        // no fields to write
    }

    @Override
    public void readMarshallable(@NotNull WireIn wireIn) {
        // no fields to read
        initTransients();
    }

    @Override
    public String toString() {
        return new String(array, 0, 0, array.length);
    }
}
