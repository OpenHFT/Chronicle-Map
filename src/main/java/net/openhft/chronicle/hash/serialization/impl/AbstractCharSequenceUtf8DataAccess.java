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

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesUtil;
import net.openhft.chronicle.bytes.RandomDataInput;
import net.openhft.chronicle.hash.AbstractData;
import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.serialization.DataAccess;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;

import static net.openhft.chronicle.hash.serialization.impl.DefaultElasticBytes.DEFAULT_BYTES_CAPACITY;

abstract class AbstractCharSequenceUtf8DataAccess<T extends CharSequence> extends AbstractData<T>
        implements DataAccess<T>, Data<T> {

    /**
     * State field
     */
    transient T cs;
    /**
     * Cache field
     */
    private transient Bytes bytes;

    AbstractCharSequenceUtf8DataAccess(long bytesCapacity) {
        initTransients(bytesCapacity);
    }

    private void initTransients(long bytesCapacity) {
        bytes = DefaultElasticBytes.allocateDefaultElasticBytes(bytesCapacity);
    }

    @Override
    public Data<T> getData(@NotNull T cs) {
        this.cs = cs;
        bytes.clear();
        BytesUtil.appendUtf8(bytes, cs);
        return this;
    }

    @Override
    public void uninit() {
        cs = null;
    }

    @Override
    public void readMarshallable(@NotNull WireIn wireIn) {
        // no config fields to read
        initTransients(DEFAULT_BYTES_CAPACITY);
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wireOut) {
        // no config fields to write
    }

    @Override
    public RandomDataInput bytes() {
        return bytes.bytesStore();
    }

    @Override
    public long offset() {
        return 0;
    }

    @Override
    public long size() {
        return bytes.readRemaining();
    }

    @Override
    public T get() {
        return cs;
    }
}
