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

package net.openhft.chronicle.hash.impl.stage.data.bytes;

import net.openhft.chronicle.bytes.RandomDataInput;
import net.openhft.chronicle.hash.AbstractData;
import net.openhft.chronicle.hash.impl.stage.entry.HashEntryStages;
import net.openhft.chronicle.hash.impl.stage.entry.SegmentStages;
import net.openhft.chronicle.hash.impl.stage.hash.CheckOnEachPublicOperation;
import net.openhft.chronicle.hash.impl.stage.hash.KeyBytesInterop;
import net.openhft.sg.Stage;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

@Staged
public class EntryKeyBytesData<K> extends AbstractData<K> {
    
    @StageRef KeyBytesInterop<K, ?, ?> ki;
    @StageRef SegmentStages s;
    @StageRef HashEntryStages<K> entry;
    @StageRef CheckOnEachPublicOperation checkOnEachPublicOperation;


    @Stage("CachedEntryKey") private K cachedEntryKey;
    @Stage("CachedEntryKey") private boolean cachedEntryKeyRead = false;
    
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
    public K get() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return cachedEntryKey;
    }

    @Override
    public K getUsing(K usingKey) {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return innerGetUsing(usingKey);
    }
    
    private K innerGetUsing(K usingKey) {
        s.segmentBytes.position(entry.keyOffset);
        return ki.keyReader.read(s.segmentBytes, size(), usingKey);
    }
}
