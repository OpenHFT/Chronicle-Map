/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash.impl.stage.data.bytes;

import net.openhft.chronicle.algo.hashing.LongHashFunction;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.RandomDataInput;
import net.openhft.chronicle.hash.AbstractData;
import net.openhft.chronicle.hash.impl.VanillaChronicleHashHolder;
import net.openhft.chronicle.hash.impl.stage.entry.HashEntryStages;
import net.openhft.chronicle.hash.impl.stage.entry.SegmentStages;
import net.openhft.chronicle.hash.impl.stage.hash.CheckOnEachPublicOperation;
import net.openhft.chronicle.hash.impl.stage.hash.KeyBytesInterop;
import net.openhft.sg.Stage;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

@Staged
@SuppressWarnings({"rawtypes", "unchecked"})
public class EntryKeyBytesData<K> extends AbstractData<K> {

    @StageRef
    VanillaChronicleHashHolder<K> hh;
    @StageRef
    KeyBytesInterop<K> ki;
    @StageRef
    SegmentStages s;
    @StageRef
    HashEntryStages<K> entry;
    @StageRef
    CheckOnEachPublicOperation checkOnEachPublicOperation;

    @Stage("CachedEntryKey")
    private K cachedEntryKey;
    @Stage("CachedEntryKey")
    private boolean cachedEntryKeyRead = false;

    private void initCachedEntryKey() {
        cachedEntryKey = innerGetUsing(cachedEntryKey);
        cachedEntryKeyRead = true;
    }

    @Override
    public RandomDataInput bytes() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return s.segmentBS;
    }

    @Override
    public long offset() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return entry.keyOffset;
    }

    @Override
    public long size() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return entry.keySize;
    }

    @Override
    public long hash(LongHashFunction f) {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return super.hash(f);
    }

    @Override
    public K get() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return cachedEntryKey;
    }

    @Override
    public K getUsing(K using) {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return innerGetUsing(using);
    }

    private K innerGetUsing(K usingKey) {
        Bytes bytes = s.segmentBytesForRead();
        bytes.readPosition(entry.keyOffset);
        return ki.keyReader.read(bytes, size(), usingKey);
    }
}
