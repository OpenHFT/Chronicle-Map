/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
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
package net.openhft.chronicle.map.impl.stage.data.bytes;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.RandomDataInput;
import net.openhft.chronicle.hash.AbstractData;
import net.openhft.chronicle.hash.impl.stage.entry.SegmentStages;
import net.openhft.chronicle.hash.impl.stage.hash.CheckOnEachPublicOperation;
import net.openhft.chronicle.map.impl.VanillaChronicleMapHolder;
import net.openhft.chronicle.map.impl.stage.entry.MapEntryStages;
import net.openhft.chronicle.map.impl.stage.map.ValueBytesInterop;
import net.openhft.sg.Stage;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

/**
 * {@link net.openhft.chronicle.hash.Data} view over the value bytes of the
 * current map entry.
 * <p>
 * The stage keeps track of the value offset and size within the segment and
 * exposes a cached decoded value for repeated access during a single
 * operation. All reads go through the configured {@link ValueBytesInterop}
 * so that user-defined value marshallers are honoured.
 */
@Staged
@SuppressWarnings({"rawtypes", "unchecked"})
public class EntryValueBytesData<V> extends AbstractData<V> {

    @StageRef
    VanillaChronicleMapHolder<?, V, ?> mh;
    @StageRef
    ValueBytesInterop<V> vi;
    @StageRef
    SegmentStages s;
    @StageRef
    MapEntryStages<?, V> entry;
    @StageRef
    CheckOnEachPublicOperation checkOnEachPublicOperation;

    @Stage("CachedEntryValue")
    private V cachedEntryValue =
            mh.m().valueType() == CharSequence.class ? (V) new StringBuilder() : null;
    @Stage("CachedEntryValue")
    private boolean cachedEntryValueRead = false;

    private void initCachedEntryValue() {
        cachedEntryValue = innerGetUsing(cachedEntryValue);
        cachedEntryValueRead = true;
    }

    public boolean cachedEntryValueInit() {
        return cachedEntryValueRead;
    }

    public void closeCachedEntryValue() {
        cachedEntryValueRead = false;
    }

    @Override
    public RandomDataInput bytes() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return s.segmentBS;
    }

    @Override
    public long offset() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return entry.valueOffset;
    }

    @Override
    public long size() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return entry.valueSize;
    }

    @Override
    public V get() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return cachedEntryValue;
    }

    @Override
    public V getUsing(V using) {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return innerGetUsing(using);
    }

    private V innerGetUsing(V usingValue) {
        Bytes segmentBytes = s.segmentBytesForRead();
        segmentBytes.readPosition(entry.valueOffset);
        return vi.valueReader.read(segmentBytes, size(), usingValue);
    }
}
