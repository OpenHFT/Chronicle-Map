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
