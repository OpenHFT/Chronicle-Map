/*
 *      Copyright (C) 2012, 2016  higherfrequencytrading.com
 *      Copyright (C) 2016 Roman Leventov
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
        bs = HeapBytesStore.uninitialized();
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
        bs.init(instance);
        return this;
    }

    @Override
    public void uninit() {
        array = null;
        bs.uninit();
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
}
