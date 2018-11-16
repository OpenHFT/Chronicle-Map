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

package net.openhft.chronicle.map.impl.stage.entry;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.impl.CompactOffHeapLinearHashTable;
import net.openhft.chronicle.hash.impl.stage.entry.AllocatedChunks;
import net.openhft.chronicle.hash.impl.stage.entry.HashEntryStages;
import net.openhft.chronicle.hash.impl.stage.query.KeySearch;
import net.openhft.chronicle.map.MapEntry;
import net.openhft.chronicle.map.VanillaChronicleMap;
import net.openhft.chronicle.map.impl.VanillaChronicleMapHolder;
import net.openhft.chronicle.map.impl.stage.data.bytes.EntryValueBytesData;
import net.openhft.sg.Stage;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;
import org.jetbrains.annotations.NotNull;

import static net.openhft.chronicle.map.VanillaChronicleMap.alignAddr;

@Staged
public abstract class MapEntryStages<K, V> extends HashEntryStages<K>
        implements MapEntry<K, V> {

    @StageRef
    public VanillaChronicleMapHolder<?, ?, ?> mh;
    @StageRef
    public AllocatedChunks allocatedChunks;
    public long valueSizeOffset = -1;
    @Stage("ValueSize")
    public long valueSize = -1;
    @Stage("ValueSize")
    public long valueOffset;
    @StageRef
    public EntryValueBytesData<V> entryValue;
    @StageRef
    KeySearch<K> ks;

    long countValueSizeOffset() {
        return keyEnd();
    }

    @SuppressWarnings("unused")
    void initValueSizeOffset() {
        valueSizeOffset = countValueSizeOffset();
    }

    void initValueSize(long valueSize) {
        this.valueSize = valueSize;
        Bytes segmentBytes = s.segmentBytesForWrite();
        segmentBytes.writePosition(valueSizeOffset);
        mh.m().valueSizeMarshaller.writeSize(segmentBytes, valueSize);
        long currentPosition = segmentBytes.writePosition();
        long currentAddr = segmentBytes.addressForRead(currentPosition);
        long skip = alignAddr(currentAddr, mh.m().alignment) - currentAddr;
        if (skip > 0)
            segmentBytes.writeSkip(skip);
        valueOffset = segmentBytes.writePosition();
    }

    @SuppressWarnings("unused")
    void initValueSize() {
        Bytes segmentBytes = s.segmentBytesForRead();
        segmentBytes.readPosition(valueSizeOffset);
        valueSize = mh.m().readValueSize(segmentBytes);
        valueOffset = segmentBytes.readPosition();
    }

    public void initValue(Data<?> value) {
        initValueSize(value.size());
        writeValue(value);
    }

    public void writeValue(Data<?> value) {
        initDelayedUpdateChecksum(true);
        // In acquireContext(), replaceValue() is called for
        // 1) executing custom replaceValue() logic, if defined, from configured MapEntryOperations
        // 2) Update replication status (bits, timestamp)
        // 3) update entry checksum
        // but the actual entry replacement is not needed, if the value object is a flyweight over
        // the off-heap bytes. This condition avoids in-place data copy.
        // TODO would be nice to reduce scope of this check, i. e. check only when it could be
        // true, and avoid when it surely false (fresh value put, relocating put etc.)
        // TODO this optimization is now disabled, because it calls value.bytes() that forces double
        // data copy, if sizedReader/Writer configured for the value.
//        RandomDataInput valueBytes = value.bytes();
//        if (valueBytes instanceof NativeBytesStore &&
//                valueBytes.address(value.offset()) == s.segmentBS.address(valueOffset)) {
//            return;
//        }
        value.writeTo(s.segmentBS, valueOffset);
    }

    @Override
    public long entryEnd() {
        return valueOffset + valueSize;
    }

    @NotNull
    @Override
    public Data<V> value() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return entryValue;
    }

    public long newSizeOfEverythingBeforeValue(Data<V> newValue) {
        return valueSizeOffset + mh.m().valueSizeMarshaller.storingLength(newValue.size()) -
                keySizeOffset;
    }

    public void innerDefaultReplaceValue(Data<V> newValue) {
        assert s.innerUpdateLock.isHeldByCurrentThread();

        boolean newValueSizeIsDifferent = newValue.size() != this.valueSize;
        if (newValueSizeIsDifferent) {
            long newSizeOfEverythingBeforeValue = newSizeOfEverythingBeforeValue(newValue);
            long entryStartOffset = keySizeOffset;
            VanillaChronicleMap<?, ?, ?> m = mh.m();
            long newValueOffset =
                    alignAddr(entryStartOffset + newSizeOfEverythingBeforeValue, mh.m().alignment);
            long newEntrySize = newEntrySize(newValue, entryStartOffset, newValueOffset);
            int newSizeInChunks = m.inChunks(newEntrySize);
            newValueDoesNotFit:
            if (newSizeInChunks > entrySizeInChunks) {
                if (newSizeInChunks > m.maxChunksPerEntry) {
                    throw new IllegalArgumentException(m.toIdentityString() +
                            ": Value too large: entry takes " + newSizeInChunks + " chunks, " +
                            m.maxChunksPerEntry + " is maximum.");
                }
                if (s.realloc(pos, entrySizeInChunks, newSizeInChunks)) {
                    break newValueDoesNotFit;
                }
                relocation(newValue, newEntrySize);
                return;
            } else if (newSizeInChunks < entrySizeInChunks) {
                s.freeExtra(pos, entrySizeInChunks, newSizeInChunks);
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
    }

    public long newEntrySize(Data<V> newValue, long entryStartOffset, long newValueOffset) {
        return checksumStrategy.extraEntryBytes() +
                newValueOffset + newValue.size() - entryStartOffset;
    }

    protected void relocation(Data<V> newValue, long newEntrySize) {
        // need to copy, because in initEntryAndKeyCopying(), in alloc(), nextTier() called ->
        // hashLookupPos cleared, as a dependant
        long oldHashLookupPos = hlp.hashLookupPos;
        long oldHashLookupAddr = s.tierBaseAddr;

        boolean tierHasChanged = allocatedChunks.initEntryAndKeyCopying(
                newEntrySize, valueSizeOffset - keySizeOffset, pos, entrySizeInChunks);

        if (tierHasChanged) {
            // implicitly inits key search, locating hashLookupPos on the empty slot
            if (!ks.searchStateAbsent())
                throw new AssertionError();
        }

        initValue(newValue);

        freeExtraAllocatedChunks();

        CompactOffHeapLinearHashTable hl = hh.h().hashLookup;
        long hashLookupKey = hl.key(hl.readEntry(oldHashLookupAddr, oldHashLookupPos));
        hl.checkValueForPut(pos);
        hl.writeEntryVolatile(s.tierBaseAddr, hlp.hashLookupPos, hashLookupKey, pos);
        // write lock is needed anyway (see testPutShouldBeWriteLocked()) but the scope is reduced
        // as much as possible
        s.innerWriteLock.lock();
        if (tierHasChanged)
            hl.remove(oldHashLookupAddr, oldHashLookupPos);
    }

    public final long entrySize(long keySize, long valueSize) {
        long sizeOfEverythingBeforeValue = sizeOfEverythingBeforeValue(keySize, valueSize);
        return innerEntrySize(sizeOfEverythingBeforeValue, valueSize);
    }

    public long innerEntrySize(long sizeOfEverythingBeforeValue, long valueSize) {
        if (!mh.m().constantlySizedEntry && mh.m().couldNotDetermineAlignmentBeforeAllocation)
            sizeOfEverythingBeforeValue += mh.m().worstAlignment;
        int alignment = mh.m().alignment;
        return alignAddr(sizeOfEverythingBeforeValue, alignment) +
                alignAddr(valueSize, alignment);
    }

    long sizeOfEverythingBeforeValue(long keySize, long valueSize) {
        return mh.m().keySizeMarshaller.storingLength(keySize) + keySize +
                checksumStrategy.extraEntryBytes() +
                mh.m().valueSizeMarshaller.storingLength(valueSize);
    }

    public final void freeExtraAllocatedChunks() {
        // fast path
        if (!mh.m().constantlySizedEntry && mh.m().couldNotDetermineAlignmentBeforeAllocation &&
                entrySizeInChunks < allocatedChunks.allocatedChunks) {
            s.freeExtra(pos, allocatedChunks.allocatedChunks, entrySizeInChunks);
        } else {
            initEntrySizeInChunks(allocatedChunks.allocatedChunks);
        }
    }

    public boolean entryDeleted() {
        return false;
    }
}
