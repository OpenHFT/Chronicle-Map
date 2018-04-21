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

package net.openhft.chronicle.set;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.serialization.DataAccess;
import net.openhft.chronicle.hash.serialization.SizedReader;
import net.openhft.chronicle.hash.serialization.impl.EnumMarshallable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

final class DummyValueMarshaller implements DataAccess<DummyValue>, SizedReader<DummyValue>,
        EnumMarshallable<DummyValueMarshaller> {
    public static final DummyValueMarshaller INSTANCE = new DummyValueMarshaller();

    private DummyValueMarshaller() {
    }

    @Override
    public Data<DummyValue> getData(@NotNull DummyValue instance) {
        return DummyValueData.INSTANCE;
    }

    @Override
    public void uninit() {
        // no op
    }

    @Override
    public DataAccess<DummyValue> copy() {
        return this;
    }

    @NotNull
    @Override
    public DummyValue read(@NotNull Bytes in, long size, @Nullable DummyValue using) {
        return DummyValue.DUMMY_VALUE;
    }

    @Override
    public DummyValueMarshaller readResolve() {
        return INSTANCE;
    }
}

