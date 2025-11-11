/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
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
