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
import net.openhft.chronicle.hash.serialization.impl.IntegerDataAccess;
import net.openhft.chronicle.hash.serialization.impl.WrongXxHash;
import net.openhft.sg.Stage;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

@Staged
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
        if (f == LongHashFunction.xx_r39() && entry.keySize == 4 &&
                hh.h().keyDataAccess instanceof IntegerDataAccess) {
            return WrongXxHash.hashInt(s.segmentBS.readInt(entry.keyOffset));
        } else {
            return super.hash(f);
        }
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
