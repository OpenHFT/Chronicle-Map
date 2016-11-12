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

package net.openhft.chronicle.hash.impl.stage.iter;

import net.openhft.chronicle.bytes.BytesUtil;
import net.openhft.chronicle.hash.ChronicleHashRecoveryFailedException;
import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.impl.CompactOffHeapLinearHashTable;
import net.openhft.chronicle.hash.impl.VanillaChronicleHash;
import net.openhft.chronicle.hash.impl.stage.entry.SegmentStages;
import net.openhft.chronicle.hash.impl.stage.hash.LogHolder;
import net.openhft.chronicle.map.ExternalMapQueryContext;
import net.openhft.chronicle.map.MapEntry;
import net.openhft.chronicle.map.VanillaChronicleMap;
import net.openhft.chronicle.map.impl.VanillaChronicleMapHolder;
import net.openhft.chronicle.map.impl.stage.entry.MapEntryStages;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;
import org.slf4j.Logger;

@Staged
public class TierRecovery {

    @StageRef LogHolder lh;
    @StageRef VanillaChronicleMapHolder<?, ?, ?> mh;
    @StageRef SegmentStages s;
    @StageRef MapEntryStages<?, ?> e;
    @StageRef IterationKeyHashCode khc;

    public int recoverTier(int segmentIndex) {
        s.freeList.clearAll();

        Logger log = lh.LOG;
        VanillaChronicleHash<?, ?, ?, ?> h = mh.h();
        CompactOffHeapLinearHashTable hl = h.hashLookup;
        long hlAddr = s.tierBaseAddr;

        long validEntries = 0;
        long hlPos = 0;
        do {
            long hlEntry = hl.readEntry(hlAddr, hlPos);
            nextHlPos:
            if (!hl.empty(hlEntry)) {
                // (*)
                hl.clearEntry(hlAddr, hlPos);
                if (validEntries >= h.maxEntriesPerHashLookup) {
                    log.error("Too many entries in tier with index {}, max is {}",
                            s.tierIndex, h.maxEntriesPerHashLookup);
                    break nextHlPos;
                }

                long searchKey = hl.key(hlEntry);
                long entryPos = hl.value(hlEntry);
                int si = checkEntry(searchKey, entryPos, segmentIndex);
                if (si < 0) {
                    break nextHlPos;
                } else {
                    s.freeList.setRange(entryPos, entryPos + e.entrySizeInChunks);
                    segmentIndex = si;
                }

                // The entry has passed all checks, re-insert:
                long startInsertPos = hl.hlPos(searchKey);
                long insertPos = startInsertPos;
                do {
                    long hlInsertEntry = hl.readEntry(hlAddr, insertPos);
                    if (hl.empty(hlInsertEntry)) {
                        hl.writeEntry(hlAddr, insertPos, hl.entry(searchKey, entryPos));
                        validEntries++;
                        break nextHlPos;
                    }
                    if (insertPos == hlPos) {
                        // means we made a whole loop, without finding a hole to re-insert entry,
                        // even if hashLookup was corrupted and all slots are dirty now, at least
                        // the slot cleared at (*) should be clear, if it is dirty, only
                        // a concurrent modification thread could occupy it
                        throw new ChronicleHashRecoveryFailedException(
                                "Concurrent modification of " + h.toIdentityString() +
                                        " while recovery procedure is in progress");
                    }
                    checkDuplicateKeys:
                    if (hl.key(hlInsertEntry) == searchKey) {
                        long anotherEntryPos = hl.value(hlInsertEntry);
                        if (anotherEntryPos == entryPos) {
                            validEntries++;
                            break nextHlPos;
                        }
                        long currentKeyOffset = e.keyOffset;
                        long currentKeySize = e.keySize;
                        int currentEntrySizeInChunks = e.entrySizeInChunks;
                        if (insertPos >= 0 && insertPos < hlPos) {
                            // insertPos already checked
                            e.readExistingEntry(anotherEntryPos);
                        } else if (checkEntry(searchKey, anotherEntryPos, segmentIndex) < 0) {
                            break checkDuplicateKeys;
                        }
                        if (e.keySize == currentKeySize &&
                                BytesUtil.bytesEqual(s.segmentBS, currentKeyOffset,
                                        s.segmentBS, e.keyOffset, currentKeySize)) {
                            log.error("Entries with duplicate keys within a tier: " +
                                            "at pos {} and {} with key {}, first value is {}",
                                    entryPos, anotherEntryPos, e.key(), e.value());
                            s.freeList.clearRange(
                                    entryPos, entryPos + currentEntrySizeInChunks);
                            break nextHlPos;
                        }
                    }
                    insertPos = hl.step(insertPos);
                } while (insertPos != startInsertPos);
                throw new ChronicleHashRecoveryFailedException(
                        "HashLookup overflow should never occur. " +
                                "It might also be concurrent access to " + h.toIdentityString() +
                                " while recovery procedure is in progress");
            }
            hlPos = hl.step(hlPos);
        } while (hlPos != 0);
        shiftHashLookupEntries();
        return segmentIndex;
    }

    private void shiftHashLookupEntries() {
        VanillaChronicleHash<?, ?, ?, ?> h = mh.h();
        CompactOffHeapLinearHashTable hl = h.hashLookup;
        long hlAddr = s.tierBaseAddr;

        long hlPos = 0;
        long steps = 0;
        do {
            long hlEntry = hl.readEntry(hlAddr, hlPos);
            if (!hl.empty(hlEntry)) {
                long searchKey = hl.key(hlEntry);
                long hlHolePos = hl.hlPos(searchKey);
                while (hlHolePos != hlPos) {
                    long hlHoleEntry = hl.readEntry(hlAddr, hlHolePos);
                    if (hl.empty(hlHoleEntry)) {
                        hl.writeEntry(hlAddr, hlHolePos, hlEntry);
                        if (hl.remove(hlAddr, hlPos) != hlPos) {
                            hlPos = hl.stepBack(hlPos);
                            steps--;
                        }
                        break;
                    }
                    hlHolePos = hl.step(hlHolePos);
                }
            }
            hlPos = hl.step(hlPos);
            steps++;
        } while (hlPos != 0 || steps == 0);
    }

    public void removeDuplicatesInSegment() {
        long startHlPos = 0L;
        VanillaChronicleMap<?, ?, ?> m = mh.m();
        CompactOffHeapLinearHashTable hashLookup = m.hashLookup;
        long currentTierBaseAddr = s.tierBaseAddr;
        while (!hashLookup.empty(hashLookup.readEntry(currentTierBaseAddr, startHlPos))) {
            startHlPos = hashLookup.step(startHlPos);
        }
        long hlPos = startHlPos;
        int steps = 0;
        long entries = 0;
        tierIteration:
        do {
            hlPos = hashLookup.step(hlPos);
            steps++;
            long entry = hashLookup.readEntry(currentTierBaseAddr, hlPos);
            if (!hashLookup.empty(entry)) {
                e.readExistingEntry(hashLookup.value(entry));
                Data key = (Data) e.key();
                try (ExternalMapQueryContext<?, ?, ?> c = m.queryContext(key)) {
                    MapEntry<?, ?> entry2 = c.entry();
                    Data<?> key2 = ((MapEntry) c).key();
                    if (key2.bytes().address(key2.offset()) != key.bytes().address(key.offset())) {
                        lh.LOG.error("entries with duplicate key {} in segment {}: " +
                                "with values {} and {}, removing the latter",
                                key, c.segmentIndex(),
                                entry2 != null ? ((MapEntry) c).value() : "<deleted>",
                                !e.entryDeleted() ? e.value() : "<deleted>");
                        if (hashLookup.remove(currentTierBaseAddr, hlPos) != hlPos) {
                            hlPos = hashLookup.stepBack(hlPos);
                            steps--;
                        }
                        continue tierIteration;
                    }
                }
                entries++;
            }
            // the `steps == 0` condition and this variable updates in the loop fix the bug, when
            // shift deletion occurs on the first entry of the tier, and the hlPos
            // becomes equal to start pos without making the whole loop, but only visiting a single
            // entry
        } while (hlPos != startHlPos || steps == 0);

        recoverTierEntriesCounter(entries);
        recoverLowestPossibleFreeChunkTiered();
    }

    private void recoverTierEntriesCounter(long entries) {
        if (s.tierEntries() != entries) {
            lh.LOG.error("Wrong number of entries counter for tier with index {}, " +
                    "stored: {}, should be: {}", s.tierIndex, s.tierEntries(), entries);
            s.tierEntries(entries);
        }
    }

    private void recoverLowestPossibleFreeChunkTiered() {
        long lowestFreeChunk = s.freeList.nextClearBit(0);
        if (lowestFreeChunk == -1)
            lowestFreeChunk = mh.m().actualChunksPerSegmentTier;
        if (s.lowestPossiblyFreeChunk() != lowestFreeChunk) {
            lh.LOG.error("wrong lowest free chunk for tier with index {}, " +
                    "stored: {}, should be: {}",
                    s.tierIndex, s.lowestPossiblyFreeChunk(), lowestFreeChunk);
            s.lowestPossiblyFreeChunk(lowestFreeChunk);
        }
    }

    private int checkEntry(long searchKey, long entryPos, int segmentIndex) {
        Logger log = lh.LOG;
        VanillaChronicleHash<?, ?, ?, ?> h = mh.h();
        if (entryPos < 0 || entryPos >= h.actualChunksPerSegmentTier) {
            log.error("Entry pos is out of range: {}, should be 0-{}",
                    entryPos, h.actualChunksPerSegmentTier - 1);
            return -1;
        }
        try {
            e.readExistingEntry(entryPos);
        } catch (Exception e) {
            log.error("Exception while reading entry key size: {}", e);
            return -1;
        }
        if (e.keyEnd() > s.segmentBytes.capacity()) {
            log.error("Wrong key size: {}", e.keySize);
            return -1;
        }

        long keyHashCode = khc.keyHashCode();
        int segmentIndexFromKey = h.hashSplitting.segmentIndex(keyHashCode);
        if (segmentIndexFromKey < 0 || segmentIndexFromKey >= h.actualSegments) {
            log.error("Segment index from the entry key hash code is out of range: {}, " +
                    "should be 0-{}, entry key: {}",
                    segmentIndexFromKey, h.actualSegments - 1, e.key());
            return -1;
        }

        long segmentHashFromKey = h.hashSplitting.segmentHash(keyHashCode);
        long searchKeyFromKey = h.hashLookup.maskUnsetKey(segmentHashFromKey);
        if (searchKey != searchKeyFromKey) {
            log.error("HashLookup searchKey: {}, HashLookup searchKey " +
                            "from the entry key hash code: {}, entry key: {}",
                    searchKey, searchKeyFromKey, e.key());
            return -1;
        }

        try {
            // e.entryEnd() implicitly reads the value size, to be computed
            long entryAndChecksumEnd = e.entryEnd() + e.checksumStrategy.extraEntryBytes();
            if (entryAndChecksumEnd > s.segmentBytes.capacity()) {
                log.error("Wrong value size: {}, key: {}", e.valueSize, e.key());
                return -1;
            }
        } catch (Exception ex) {
            log.error("Exception while reading entry value size: {}, key: {}", ex, e.key());
            return -1;
        }

        int storedChecksum = e.checksumStrategy.storedChecksum();
        int checksumFromEntry = e.checksumStrategy.computeChecksum();
        if (storedChecksum != checksumFromEntry) {
            log.error("Checksum doesn't match, stored: {}, should be from " +
                            "the entry bytes: {}, key: {}, value: {}",
                    storedChecksum, checksumFromEntry, e.key(), e.value());
            return -1;
        }

        if (!s.freeList.isRangeClear(entryPos, entryPos + e.entrySizeInChunks)) {
            log.error("Overlapping entry: positions {}-{}, key: {}, value: {}",
                    entryPos, entryPos + e.entrySizeInChunks - 1, e.key(), e.value());
            return -1;
        }

        if (segmentIndex < 0) {
            return segmentIndexFromKey;
        } else {
            if (segmentIndex != segmentIndexFromKey) {
                log.error("Expected segment index: {}, segment index from the entry key: {}, " +
                                "key: {}, value: {}", segmentIndex, searchKeyFromKey, e.key(),
                        e.value());
                return -1;
            } else {
                return segmentIndex;
            }
        }
    }
}
