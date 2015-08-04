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

package net.openhft.chronicle.map.impl.stage.data;

import net.openhft.chronicle.bytes.RandomDataInput;
import net.openhft.chronicle.hash.AbstractData;
import net.openhft.chronicle.map.impl.VanillaChronicleMapHolder;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

@Staged
public class DummyValueZeroData<V> extends AbstractData<V> {
    @StageRef
    VanillaChronicleMapHolder<?, ?, ?, ?, ?, ?, ?> mh;

    @Override
    public RandomDataInput bytes() {
        return ZeroRandomDataInput.INSTANCE;
    }

    @Override
    public long offset() {
        return 0;
    }

    @Override
    public long size() {
        return mh.m().valueSizeMarshaller.minEncodableSize();
    }

    @Override
    public V get() {
        throw new UnsupportedOperationException();
    }

    @Override
    public V getUsing(V usingInstance) {
        throw new UnsupportedOperationException();
    }
}
