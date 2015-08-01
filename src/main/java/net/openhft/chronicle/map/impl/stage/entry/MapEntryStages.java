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

package net.openhft.chronicle.map.impl.stage.entry;

import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.impl.stage.entry.AllocatedChunks;
import net.openhft.chronicle.hash.impl.stage.entry.HashEntryStages;
import net.openhft.chronicle.hash.impl.stage.entry.HashLookup;
import net.openhft.chronicle.map.MapEntry;
import net.openhft.chronicle.map.VanillaChronicleMap;
import net.openhft.chronicle.map.impl.VanillaChronicleMapHolder;
import net.openhft.chronicle.map.impl.stage.data.bytes.EntryValueBytesData;
import net.openhft.sg.Stage;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;
import org.jetbrains.annotations.NotNull;

@Staged
public abstract class MapEntryStages<K, V> extends HashEntryStages<K>
        implements MapEntry<K, V> {

    @StageRef public VanillaChronicleMapHolder<?, ?, ?, ?, ?, ?, ?> mh;
    @StageRef public AllocatedChunks allocatedChunks;
    @StageRef HashLookup hashLookup;


    long countValueSizeOffset() {
        return keyEnd();
    }
    
    public long valueSizeOffset = -1;

    void initValueSizeOffset() {
        valueSizeOffset = countValueSizeOffset();
    }

    @Stage("ValSize") public long valueSize = -1;
    @Stage("ValSize") public long valueOffset;

    @Stage("ValSize")
    private void countValueOffset() {
        mh.m().alignment.alignPositionAddr(entryBytes);
        valueOffset = entryBytes.position();
    }

    void initValSize(long valueSize) {
        this.valueSize = valueSize;
        entryBytes.position(valueSizeOffset);
        mh.m().valueSizeMarshaller.writeSize(entryBytes, valueSize);
        countValueOffset();
    }

    void initValSize() {
        entryBytes.position(valueSizeOffset);
        valueSize = mh.m().readValueSize(entryBytes);
        countValueOffset();
    }

    void initValSizeEqualToOld(long oldValueSizeOffset, long oldValueSize, long oldValueOffset) {
        valueSize = oldValueSize;
        valueOffset = valueSizeOffset + (oldValueOffset - oldValueSizeOffset);
    }
    
    public void initValue(Data<?> value) {
        entryBytes.position(valueSizeOffset);
        initValSize(value.size());
        writeValue(value);
    }

    public void writeValue(Data<?> value) {
        value.writeTo(entryBS, valueOffset);
    }

    public void initValueWithoutSize(
            Data<?> value, long oldValueSizeOffset, long oldValueSize, long oldValueOffset) {
        assert oldValueSize == value.size();
        initValSizeEqualToOld(oldValueSizeOffset, oldValueSize, oldValueOffset);
        writeValue(value);
    }

    @Override
    protected long entryEnd() {
        return valueOffset + valueSize;
    }
    
    @StageRef public EntryValueBytesData<V> entryValue;

    @NotNull
    @Override
    public Data<V> value() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return entryValue;
    }

    public void putValueDeletedEntry(Data<V> newValue) {
        throw new AssertionError("putValueDeletedEntry() might be called only from " +
                "non-Replicated Map query context");
    }

    public void writeValueAndPutPos(Data<V> value) {
        initValue(value);
        freeExtraAllocatedChunks();
        hashLookup.putValueVolatile(hlp.hashLookupPos, pos);
    }
    
    public long newSizeOfEverythingBeforeValue(Data<V> newValue) {
        return valueSizeOffset + mh.m().valueSizeMarshaller.sizeEncodingSize(newValue.size()) -
                keySizeOffset;
    }
    
    public void innerDefaultReplaceValue(Data<V> newValue) {
        assert s.innerUpdateLock.isHeldByCurrentThread();

        boolean newValueSizeIsDifferent = newValue.size() != this.valueSize;
        if (newValueSizeIsDifferent) {
            long newSizeOfEverythingBeforeValue = newSizeOfEverythingBeforeValue(newValue);
            long entryStartOffset = keySizeOffset;
            VanillaChronicleMap<?, ?, ?, ?, ?, ?, ?> m = mh.m();
            long newValueOffset = m.alignment.alignAddr(
                    entryStartOffset + newSizeOfEverythingBeforeValue);
            long newEntrySize = newValueOffset + newValue.size() - entryStartOffset;
            int newSizeInChunks = m.inChunks(newEntrySize);
            newValueDoesNotFit:
            if (newSizeInChunks > entrySizeInChunks) {
                if (newSizeInChunks > m.maxChunksPerEntry) {
                    throw new IllegalArgumentException("Value too large: " +
                            "entry takes " + newSizeInChunks + " chunks, " +
                            m.maxChunksPerEntry + " is maximum.");
                }
                if (s.freeList.allClear(pos + entrySizeInChunks, pos + newSizeInChunks)) {
                    s.freeList.set(pos + entrySizeInChunks, pos + newSizeInChunks);
                    break newValueDoesNotFit;
                }
                relocation(newValue, newSizeOfEverythingBeforeValue);
                return;
            } else if (newSizeInChunks < entrySizeInChunks) {
                // Freeing extra chunks
                s.freeList.clear(pos + newSizeInChunks, pos + entrySizeInChunks);
                // Do NOT reset nextPosToSearchFrom, because if value
                // once was larger it could easily became larger again,
                // But if these chunks will be taken by that time,
                // this entry will need to be relocated.
            }
            // new size != old size => size is not constant => size is actually written =>
            // to prevent (at least) this execution:
            // 1. concurrent reader thread reads the size
            // 2. this thread updates the size and the value
            // 3. concurrent reader reads the value
            // We MUST upgrade to exclusive lock
        } else {
            // TODO to turn the following block on, JLANG-46 is required. Also unclear what happens
            // if the value is DataValue generated with 2, 4 or 8 distinct bytes, putting on-heap
            // implementation of such value is also not atomic currently, however there is a way
            // to make it atomic, we should identify such cases and make a single write:
            // state = UNSAFE.getLong(onHeapValueObject, offsetToTheFirstField);
            // bytes.writeLong(state);
//            boolean newValueSizeIsPowerOf2 = ((newValueSize - 1L) & newValueSize) != 0;
//            if (!newValueSizeIsPowerOf2 || newValueSize > 8L) {
//                // if the new value size is 1, 2, 4, or 8, it is written not atomically only if
//                // the user provided own marshaller and writes value byte-by-byte, that is very
//                // unlikely. in this case the user should update acquire write lock before write
//                // updates himself
//                upgradeToWriteLock();
//            }
        }
        s.innerWriteLock.lock();

        if (newValueSizeIsDifferent) {
            initValue(newValue);
        } else {
            writeValue(newValue);
        }
        hashLookup.putValueVolatile(hlp.hashLookupPos, pos);
    }

    protected void relocation(Data<V> newValue, long newSizeOfEverythingBeforeValue) {
        s.innerWriteLock.lock();
        s.free(pos, entrySizeInChunks);
        long entrySize = innerEntrySize(newSizeOfEverythingBeforeValue, newValue.size());
        allocatedChunks.initEntryAndKeyCopying(entrySize, valueSizeOffset - keySizeOffset);
        writeValueAndPutPos(newValue);
    }

    public final long entrySize(long keySize, long valueSize) {
        long sizeOfEverythingBeforeValue = sizeOfEverythingBeforeValue(keySize, valueSize);
        return innerEntrySize(sizeOfEverythingBeforeValue, valueSize);
    }

    public long innerEntrySize(long sizeOfEverythingBeforeValue, long valueSize) {
        if (mh.m().constantlySizedEntry) {
            return mh.m().alignment.alignAddr(sizeOfEverythingBeforeValue + valueSize);
        } else if (mh.m().couldNotDetermineAlignmentBeforeAllocation) {
            return sizeOfEverythingBeforeValue + mh.m().worstAlignment + valueSize;
        } else {
            return mh.m().alignment.alignAddr(sizeOfEverythingBeforeValue) + valueSize;
        }
    }

    long sizeOfEverythingBeforeValue(long keySize, long valueSize) {
        return mh.m().metaDataBytes +
                mh.m().keySizeMarshaller.sizeEncodingSize(keySize) + keySize +
                mh.m().valueSizeMarshaller.sizeEncodingSize(valueSize);
    }

    public final void freeExtraAllocatedChunks() {
        // fast path
        if (!mh.m().constantlySizedEntry && mh.m().couldNotDetermineAlignmentBeforeAllocation &&
                entrySizeInChunks < allocatedChunks.allocatedChunks)  {
            s.free(pos + entrySizeInChunks, allocatedChunks.allocatedChunks - entrySizeInChunks);
        } else {
            initTheEntrySizeInChunks(allocatedChunks.allocatedChunks);
        }
    }
}
