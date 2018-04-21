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

import net.openhft.chronicle.bytes.RandomDataInput;
import net.openhft.chronicle.hash.AbstractData;
import net.openhft.chronicle.map.impl.stage.data.ZeroBytesStore;
import org.jetbrains.annotations.Nullable;

public class DummyValueData extends AbstractData<DummyValue> {

    public static final DummyValueData INSTANCE = new DummyValueData();

    private DummyValueData() {
    }

    @Override
    public RandomDataInput bytes() {
        return ZeroBytesStore.INSTANCE;
    }

    @Override
    public long offset() {
        return 0;
    }

    @Override
    public long size() {
        return 0;
    }

    @Override
    public DummyValue get() {
        return DummyValue.DUMMY_VALUE;
    }

    @Override
    public DummyValue getUsing(@Nullable DummyValue using) {
        return DummyValue.DUMMY_VALUE;
    }
}
