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
