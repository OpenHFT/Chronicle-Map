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

package net.openhft.chronicle.map.impl.stage.data;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.RandomDataInput;
import net.openhft.chronicle.hash.AbstractData;
import net.openhft.chronicle.hash.impl.stage.hash.CheckOnEachPublicOperation;
import net.openhft.chronicle.map.impl.VanillaChronicleMapHolder;
import net.openhft.chronicle.map.impl.stage.map.ValueBytesInterop;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

@Staged
public class DummyValueZeroData<V> extends AbstractData<V> {

    private final Bytes zeroBytes = ZeroBytesStore.INSTANCE.bytesForRead();
    @StageRef
    VanillaChronicleMapHolder<?, ?, ?> mh;
    @StageRef
    ValueBytesInterop<V> vi;
    @StageRef
    CheckOnEachPublicOperation checkOnEachPublicOperation;

    @Override
    public RandomDataInput bytes() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return ZeroBytesStore.INSTANCE;
    }

    @Override
    public long offset() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return 0;
    }

    @Override
    public long size() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return Math.max(0, mh.m().valueSizeMarshaller.minStorableSize());
    }

    @Override
    public V get() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        // Not optimized and creates garbage, because this isn't the primary
        // use case. Zero data should only be used in bytes form
        return getUsing(null);
    }

    @Override
    public V getUsing(V using) {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        zeroBytes.readPosition(0);
        try {
            return vi.valueReader.read(zeroBytes, size(), using);
        } catch (Exception e) {
            throw zeroReadException(e);
        }
    }

    private IllegalStateException zeroReadException(Exception cause) {
        return new IllegalStateException(mh.h().toIdentityString() +
                ": Most probable cause of this exception - zero bytes of\n" +
                "the minimum positive encoding length, supported by the specified or default\n" +
                "valueSizeMarshaller() is not correct serialized form of any value. You should\n" +
                "configure defaultValueProvider() in ChronicleMapBuilder", cause);
    }
}
