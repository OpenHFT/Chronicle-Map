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

package net.openhft.chronicle.hash.impl.stage.entry;

import net.openhft.chronicle.algo.bytes.Access;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.hash.ChecksumEntry;
import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.HashEntry;
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

    @StageRef public VanillaChronicleHashHolder<?> hh;
    @StageRef public SegmentStages s;
    @StageRef public CheckOnEachPublicOperation checkOnEachPublicOperation;
    @StageRef public HashLookupPos hlp;

    public long pos = -1;

    public void initPos(long pos) {
        this.pos = pos;
    }

    public abstract void closePos();

    @Stage("EntryOffset") public long keySizeOffset = -1;

    public void initEntryOffset() {
        keySizeOffset = s.entrySpaceOffset + pos * hh.h().chunkSize;
    }

    public long keySize = -1;

    public void initKeySize(long keySize) {
        this.keySize = keySize;
    }

    public long keyOffset = -1;

    public void initKeyOffset(long keyOffset) {
        this.keyOffset = keyOffset;
    }

    public void readExistingEntry(long pos) {
        initPos(pos);
        Bytes segmentBytes = s.segmentBytesForRead();
        segmentBytes.readPosition(keySizeOffset);
        initKeySize(hh.h().keySizeMarshaller.readSize(segmentBytes));
        initKeyOffset(segmentBytes.readPosition());
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

    protected long entryEnd() {
        return keyEnd();
    }

    @StageRef HashEntryChecksumStrategy hashEntryChecksumStrategy;
    public final ChecksumStrategy checksumStrategy = hh.h().checksumEntries ?
            hashEntryChecksumStrategy : NoChecksumStrategy.INSTANCE;

    @Override
    public void updateChecksum() {
        checksumStrategy.updateChecksum();
    }

    @Override
    public boolean checkSum() {
        return checksumStrategy.checkSum();
    }

    long entrySize() {
        return checksumStrategy.extraEntryBytes() + entryEnd() - keySizeOffset;
    }

    @StageRef EntryKeyBytesData<K> entryKey;

    @NotNull
    @Override
    public Data<K> key() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return entryKey;
    }

    @Stage("EntrySizeInChunks") public int entrySizeInChunks = 0;

    void initEntrySizeInChunks() {
        entrySizeInChunks = hh.h().inChunks(entrySize());
    }

    public void initEntrySizeInChunks(int actuallyUsedChunks) {
        entrySizeInChunks = actuallyUsedChunks;
    }
    
    public void innerRemoveEntryExceptHashLookupUpdate() {
        s.free(pos, entrySizeInChunks);
        s.entries(s.entries() - 1L);
        s.incrementModCount();
    }
}
