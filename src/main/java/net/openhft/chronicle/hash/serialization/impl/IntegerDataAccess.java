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

import net.openhft.chronicle.algo.hashing.LongHashFunction;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.bytes.RandomDataInput;
import net.openhft.chronicle.bytes.RandomDataOutput;
import net.openhft.chronicle.hash.AbstractData;
import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.serialization.DataAccess;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * @deprecated use {@link IntegerDataAccess_3_13} instead. This class is not removed only because
 * users who created a Chronicle Map with older version of the library with this class as the key or
 * value data access should be able to read and access the map with the present version of the
 * Chronicle Map library.
 */
@SuppressWarnings("DeprecatedIsStillUsed")
@Deprecated
public final class IntegerDataAccess extends AbstractData<Integer>
        implements DataAccess<Integer>, Data<Integer> {

    // Cache fields
    private transient boolean bsInit;
    private transient BytesStore bs;

    /**
     * State field
     */
    private transient Integer instance;

    public IntegerDataAccess() {
        initTransients();
    }

    private void initTransients() {
        bs = BytesStore.wrap(new byte[4]);
    }

    @Override
    public RandomDataInput bytes() {
        if (!bsInit) {
            bs.writeInt(0, instance);
            bsInit = true;
        }
        return bs;
    }

    @Override
    public long offset() {
        return 0;
    }

    @Override
    public long size() {
        return 4;
    }

    @Override
    public Integer get() {
        return instance;
    }

    @Override
    public Integer getUsing(@Nullable Integer using) {
        return instance;
    }

    @Override
    public long hash(LongHashFunction f) {
        if (f == LongHashFunction.xx_r39()) {
            return WrongXxHash.hashInt(instance);
        } else {
            return f.hashInt(instance);
        }
    }

    @Override
    public boolean equivalent(RandomDataInput source, long sourceOffset) {
        return source.readInt(sourceOffset) == instance;
    }

    @Override
    public void writeTo(RandomDataOutput target, long targetOffset) {
        target.writeInt(targetOffset, instance);
    }

    @Override
    public Data<Integer> getData(@NotNull Integer instance) {
        this.instance = instance;
        bsInit = false;
        return this;
    }

    @Override
    public void uninit() {
        instance = null;
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
    @SuppressWarnings("deprecation")
    public DataAccess<Integer> copy() {
        return new IntegerDataAccess();
    }
}
