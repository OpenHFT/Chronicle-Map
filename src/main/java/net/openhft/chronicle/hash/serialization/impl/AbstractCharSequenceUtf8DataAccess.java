/*
 *      Copyright (C) 2015  higherfrequencytrading.com
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

abstract class AbstractCharSequenceUtf8DataAccess<T extends CharSequence> extends AbstractData<T>
        implements DataAccess<T>, Data<T> {

    /** Cache field */
    private transient Bytes bytes;

    /** State field */
    transient T cs;

    AbstractCharSequenceUtf8DataAccess() {
        initTransients();
    }

    private void initTransients() {
        bytes = Bytes.allocateElasticDirect(1);
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
        initTransients();
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
