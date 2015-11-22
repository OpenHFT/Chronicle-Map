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

package net.openhft.chronicle.map.impl.stage.query;

import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.impl.stage.entry.HashLookupSearch;
import net.openhft.chronicle.map.VanillaChronicleMap;
import net.openhft.chronicle.map.impl.stage.entry.MapEntryStages;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

import static net.openhft.chronicle.map.VanillaChronicleMap.alignAddr;

@Staged
public abstract class QueryMapEntryStages<K, V> extends MapEntryStages<K, V> {

    @StageRef HashLookupSearch hls;

    @Override
    public void putValueDeletedEntry(Data<V> newValue) {
        assert s.innerUpdateLock.isHeldByCurrentThread();

        int newSizeInChunks;
        long entryStartOffset = keySizeOffset;
        long newSizeOfEverythingBeforeValue = -1;
        boolean newValueSizeIsDifferent = newValue.size() != valueSize;
        if (newValueSizeIsDifferent) {
            newSizeOfEverythingBeforeValue = newSizeOfEverythingBeforeValue(newValue);
            long newValueOffset =
                    alignAddr(entryStartOffset + newSizeOfEverythingBeforeValue, mh.m().alignment);
            long newEntrySize = newValueOffset + newValue.size() - entryStartOffset;
            newSizeInChunks = mh.m().inChunks(newEntrySize);
        } else {
            newSizeInChunks = entrySizeInChunks;
        }
        if (pos + newSizeInChunks < s.freeList.logicalSize() &&
                s.freeList.isRangeClear(pos, pos + newSizeInChunks)) {
            s.freeList.setRange(pos, pos + newSizeInChunks);
            s.innerWriteLock.lock();
            allocatedChunks.incrementSegmentEntriesIfNeeded();
            if (newValueSizeIsDifferent) {
                initValue(newValue);
            } else {
                writeValue(newValue);
            }
        } else {
            if (newValueSizeIsDifferent) {
                assert newSizeOfEverythingBeforeValue >= 0;
            } else {
                newSizeOfEverythingBeforeValue = newSizeOfEverythingBeforeValue(newValue);
            }
            long entrySize = innerEntrySize(newSizeOfEverythingBeforeValue, newValue.size());
            if (newValueSizeIsDifferent) {
                allocatedChunks.initEntryAndKeyCopying(
                        entrySize, valueSizeOffset - entryStartOffset);
                initValue(newValue);
            } else {
                long oldValueSizeOffset = valueSizeOffset;
                long oldValueSize = valueSize;
                long oldValueOffset = valueOffset;
                allocatedChunks.initEntryAndKeyCopying(entrySize, valueOffset - entryStartOffset);
                initValue_WithoutSize(newValue, oldValueSizeOffset, oldValueSize, oldValueOffset);
            }
            freeExtraAllocatedChunks();
        }
        hls.putNewVolatile(pos);
    }
}
