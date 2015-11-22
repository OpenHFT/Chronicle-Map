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

package net.openhft.chronicle.set;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.serialization.DataAccess;
import net.openhft.chronicle.hash.serialization.SizedReader;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

enum DummyValueMarshaller implements DataAccess<DummyValue>, SizedReader<DummyValue> {
    INSTANCE;

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
}

