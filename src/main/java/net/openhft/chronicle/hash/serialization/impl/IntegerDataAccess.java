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
