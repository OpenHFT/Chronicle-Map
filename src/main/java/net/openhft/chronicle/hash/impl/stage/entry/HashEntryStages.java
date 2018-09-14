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

package net.openhft.chronicle.hash.impl.stage.entry;

import net.openhft.chronicle.algo.bytes.Access;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.hash.ChecksumEntry;
import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.HashEntry;
import net.openhft.chronicle.hash.impl.LocalLockState;
import net.openhft.chronicle.hash.impl.VanillaChronicleHashHolder;
import net.openhft.chronicle.hash.impl.stage.data.bytes.EntryKeyBytesData;
import net.openhft.chronicle.hash.impl.stage.hash.CheckOnEachPublicOperation;
import net.openhft.sg.Stage;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;
import org.jetbrains.annotations.NotNull;

import static net.openhft.chronicle.algo.bytes.Access.checkedBytesStoreAccess;
import static net.openhft.chronicle.algo.bytes.Access.nativeAccess;

@Staged
public abstract class HashEntryStages<K> implements HashEntry<K>, ChecksumEntry {

    @StageRef
    public VanillaChronicleHashHolder<?> hh;
    @StageRef
    public SegmentStages s;
    @StageRef
    public CheckOnEachPublicOperation checkOnEachPublicOperation;
    @StageRef
    public HashLookupPos hlp;

    public long pos = -1;
    @Stage("EntryOffset")
    public long keySizeOffset = -1;
    public long keySize = -1;
    public long keyOffset = -1;
    public boolean delayedUpdateChecksum = false;
    @StageRef
    public EntryKeyBytesData<K> entryKey;
    @Stage("EntrySizeInChunks")
    public int entrySizeInChunks = 0;
    @StageRef
    HashEntryChecksumStrategy hashEntryChecksumStrategy;
    public final ChecksumStrategy checksumStrategy = hh.h().checksumEntries ?
            hashEntryChecksumStrategy : NoChecksumStrategy.INSTANCE;

    public void initPos(long pos) {
        this.pos = pos;
    }

    public abstract void closePos();

    public abstract boolean entryOffsetInit();

    public void initEntryOffset(long keySizeOffset) {
        this.keySizeOffset = keySizeOffset;
    }

    public void initEntryOffset() {
        keySizeOffset = s.entrySpaceOffset + pos * hh.h().chunkSize;
    }

    public abstract void closeEntryOffset();

    public void initKeySize(long keySize) {
        this.keySize = keySize;
    }

    public abstract void closeKeySize();

    public void initKeyOffset(long keyOffset) {
        this.keyOffset = keyOffset;
    }

    public abstract void closeKeyOffset();

    public void readExistingEntry(long pos) {
        initPos(pos);
        Bytes segmentBytes = s.segmentBytesForRead();
        segmentBytes.readPosition(keySizeOffset);
        initKeySize(hh.h().keySizeMarshaller.readSize(segmentBytes));
        initKeyOffset(segmentBytes.readPosition());
    }

    public void readFoundEntry(long pos, long keySizeOffset, long keySize, long keyOffset) {
        initPos(pos);
        initEntryOffset(keySizeOffset);
        initKeySize(keySize);
        initKeyOffset(keyOffset);
    }

    public void closeEntry() {
        closePos();
        closeEntryOffset();
        closeKeySize();
        closeKeyOffset();
    }

    public void writeNewEntry(long pos, Data<?> key) {
        initPos(pos);
        initKeySize(key.size());
        Bytes segmentBytes = s.segmentBytesForWrite();
        segmentBytes.writePosition(keySizeOffset);
        hh.h().keySizeMarshaller.writeSize(segmentBytes, keySize);
        initKeyOffset(segmentBytes.writePosition());
        key.writeTo(s.segmentBS, keyOffset);
    }

    public void copyExistingEntry(
            long newPos, long bytesToCopy, long oldKeyAddr, long oldKeySizeAddr) {
        initPos(newPos);
        initKeyOffset(keySizeOffset + (oldKeyAddr - oldKeySizeAddr));
        // Calling Access.copy() which is probably slower because not of abstractions,
        // because there is no BytesStore.write(off, addr, len) method. Alternative is
        // to make a final BytesStore rawMemoryStore = new PointerBytesStore().set(0, Long.MAX_V)
        // and here: s.segmentBS.write(keySizeOffset, rawMemoryStore, keySizeAddr, bytesToCopy)
        Access.copy(
                nativeAccess(), null, oldKeySizeAddr,
                checkedBytesStoreAccess(), s.segmentBS, keySizeOffset,
                bytesToCopy);
    }

    public long keyEnd() {
        return keyOffset + keySize;
    }

    public long entryEnd() {
        return keyEnd();
    }

    public void initDelayedUpdateChecksum(boolean delayedUpdateChecksum) {
        // makes delayedUpdateChecksum dependent on keySizeOffset and Locks stages, to trigger
        // delayedUpdateChecksum close on these stages' close
        assert entryOffsetInit() && keySizeOffset >= 0;
        assert s.locksInit() && s.localLockState != LocalLockState.UNLOCKED;
        assert delayedUpdateChecksum; // doesn't make sense to init to "uninit" false value
        this.delayedUpdateChecksum = true;
    }

    abstract boolean delayedUpdateChecksumInit();

    public void closeDelayedUpdateChecksum() {
        if (hh.h().checksumEntries)
            hashEntryChecksumStrategy.computeAndStoreChecksum();
        delayedUpdateChecksum = false;
    }

    @Override
    public void updateChecksum() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        if (!hh.h().checksumEntries) {
            throw new UnsupportedOperationException(hh.h().toIdentityString() +
                    ": Checksum is not stored in this Chronicle Hash");
        }
        s.innerUpdateLock.lock();
        initDelayedUpdateChecksum(true);
    }

    @Override
    public boolean checkSum() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        if (!hh.h().checksumEntries) {
            throw new UnsupportedOperationException(hh.h().toIdentityString() +
                    ": Checksum is not stored in this Chronicle Hash");
        }
        // This is needed, because a concurrent update lock holder might perform an entry update,
        // but not yet written a checksum (because checksum write is delayed to the update unlock).
        // So checkSum() on read lock level might result to false negative results.
        s.innerUpdateLock.lock();
        return delayedUpdateChecksumInit() || checksumStrategy.innerCheckSum();
    }

    long entrySize() {
        return checksumStrategy.extraEntryBytes() + entryEnd() - keySizeOffset;
    }

    @NotNull
    @Override
    public Data<K> key() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return entryKey;
    }

    void initEntrySizeInChunks() {
        entrySizeInChunks = hh.h().inChunks(entrySize());
    }

    public void initEntrySizeInChunks(int actuallyUsedChunks) {
        entrySizeInChunks = actuallyUsedChunks;
    }

    public void innerRemoveEntryExceptHashLookupUpdate() {
        s.free(pos, entrySizeInChunks);
        s.incrementModCount();
    }
}
