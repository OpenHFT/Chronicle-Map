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

package net.openhft.chronicle.hash.impl.stage.iter;

import net.openhft.chronicle.bytes.BytesUtil;
import net.openhft.chronicle.hash.ChronicleHashCorruption;
import net.openhft.chronicle.hash.ChronicleHashRecoveryFailedException;
import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.impl.CompactOffHeapLinearHashTable;
import net.openhft.chronicle.hash.impl.VanillaChronicleHash;
import net.openhft.chronicle.hash.impl.stage.entry.SegmentStages;
import net.openhft.chronicle.map.ChronicleHashCorruptionImpl;
import net.openhft.chronicle.map.ExternalMapQueryContext;
import net.openhft.chronicle.map.MapEntry;
import net.openhft.chronicle.map.VanillaChronicleMap;
import net.openhft.chronicle.map.impl.VanillaChronicleMapHolder;
import net.openhft.chronicle.map.impl.stage.entry.MapEntryStages;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

import static net.openhft.chronicle.map.ChronicleHashCorruptionImpl.*;

@Staged
public class TierRecovery {

    @StageRef
    VanillaChronicleMapHolder<?, ?, ?> mh;
    @StageRef
    SegmentStages s;
    @StageRef
    MapEntryStages<?, ?> e;
    @StageRef
    IterationKeyHashCode khc;

    public int recoverTier(
            int segmentIndex, ChronicleHashCorruption.Listener corruptionListener,
            ChronicleHashCorruptionImpl corruption) {
        s.freeList.clearAll();

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
                    report(corruptionListener, corruption, segmentIndex, () ->
                            format("Too many entries in tier with index {}, max is {}",
                                    s.tierIndex, h.maxEntriesPerHashLookup)
                    );
                    break nextHlPos;
                }

                long searchKey = hl.key(hlEntry);
                long entryPos = hl.value(hlEntry);
                int si = checkEntry(searchKey, entryPos, segmentIndex,
                        corruptionListener, corruption);
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
                        } else if (checkEntry(searchKey, anotherEntryPos, segmentIndex,
                                corruptionListener, corruption) < 0) {
                            break checkDuplicateKeys;
                        }
                        if (e.keySize == currentKeySize &&
                                BytesUtil.bytesEqual(s.segmentBS, currentKeyOffset,
                                        s.segmentBS, e.keyOffset, currentKeySize)) {
                            report(corruptionListener, corruption, segmentIndex, () ->
                                    format("Entries with duplicate keys within a tier: " +
                                                    "at pos {} and {} with key {}, first value is {}",
                                            entryPos, anotherEntryPos, e.key(), e.value())
                            );
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

    public void removeDuplicatesInSegment(
            ChronicleHashCorruption.Listener corruptionListener,
            ChronicleHashCorruptionImpl corruption) {
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
                Data key = e.key();
                try (ExternalMapQueryContext<?, ?, ?> c = m.queryContext(key)) {
                    MapEntry<?, ?> entry2 = c.entry();
                    Data<?> key2 = ((MapEntry) c).key();
                    long keyAddress = key.bytes().addressForRead(key.offset());
                    long key2Address = key2.bytes().addressForRead(key2.offset());
                    if (key2Address != keyAddress) {
                        report(corruptionListener, corruption, s.segmentIndex, () ->
                                format("entries with duplicate key {} in segment {}: " +
                                                "with values {} and {}, removing the latter",
                                        key, c.segmentIndex(),
                                        entry2 != null ? ((MapEntry) c).value() : "<deleted>",
                                        !e.entryDeleted() ? e.value() : "<deleted>")
                        );
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

        recoverTierEntriesCounter(entries, corruptionListener, corruption);
        recoverLowestPossibleFreeChunkTiered(corruptionListener, corruption);
    }

    private void recoverTierEntriesCounter(
            long entries, ChronicleHashCorruption.Listener corruptionListener,
            ChronicleHashCorruptionImpl corruption) {
        if (s.tierEntries() != entries) {
            report(corruptionListener, corruption, s.segmentIndex, () ->
                    format("Wrong number of entries counter for tier with index {}, " +
                            "stored: {}, should be: {}", s.tierIndex, s.tierEntries(), entries)
            );
            s.tierEntries(entries);
        }
    }

    private void recoverLowestPossibleFreeChunkTiered(
            ChronicleHashCorruption.Listener corruptionListener,
            ChronicleHashCorruptionImpl corruption) {
        long lowestFreeChunk = s.freeList.nextClearBit(0);
        if (lowestFreeChunk == -1)
            lowestFreeChunk = mh.m().actualChunksPerSegmentTier;
        if (s.lowestPossiblyFreeChunk() != lowestFreeChunk) {
            long finalLowestFreeChunk = lowestFreeChunk;
            report(corruptionListener, corruption, s.segmentIndex, () ->
                    format("wrong lowest free chunk for tier with index {}, " +
                                    "stored: {}, should be: {}",
                            s.tierIndex, s.lowestPossiblyFreeChunk(), finalLowestFreeChunk)
            );
            s.lowestPossiblyFreeChunk(lowestFreeChunk);
        }
    }

    private int checkEntry(
            long searchKey, long entryPos, int segmentIndex,
            ChronicleHashCorruption.Listener corruptionListener,
            ChronicleHashCorruptionImpl corruption) {
        VanillaChronicleHash<?, ?, ?, ?> h = mh.h();
        if (entryPos < 0 || entryPos >= h.actualChunksPerSegmentTier) {
            report(corruptionListener, corruption, segmentIndex, () ->
                    format("Entry pos is out of range: {}, should be 0-{}",
                            entryPos, h.actualChunksPerSegmentTier - 1)
            );
            return -1;
        }
        try {
            e.readExistingEntry(entryPos);
        } catch (Exception e) {
            reportException(corruptionListener, corruption, segmentIndex,
                    () -> "Exception while reading entry key size", e);
            return -1;
        }
        if (e.keyEnd() > s.segmentBytes.capacity()) {
            report(corruptionListener, corruption, segmentIndex, () ->
                    format("Wrong key size: {}", e.keySize)
            );
            return -1;
        }

        long keyHashCode = khc.keyHashCode();
        int segmentIndexFromKey = h.hashSplitting.segmentIndex(keyHashCode);
        if (segmentIndexFromKey < 0 || segmentIndexFromKey >= h.actualSegments) {
            report(corruptionListener, corruption, segmentIndex, () ->
                    format("Segment index from the entry key hash code is out of range: {}, " +
                                    "should be 0-{}, entry key: {}",
                            segmentIndexFromKey, h.actualSegments - 1, e.key())
            );
            return -1;
        }

        long segmentHashFromKey = h.hashSplitting.segmentHash(keyHashCode);
        long searchKeyFromKey = h.hashLookup.maskUnsetKey(segmentHashFromKey);
        if (searchKey != searchKeyFromKey) {
            report(corruptionListener, corruption, segmentIndex, () ->
                    format("HashLookup searchKey: {}, HashLookup searchKey " +
                                    "from the entry key hash code: {}, entry key: {}, entry pos: {}",
                            searchKey, searchKeyFromKey, e.key(), entryPos)
            );
            return -1;
        }

        try {
            // e.entryEnd() implicitly reads the value size, to be computed
            long entryAndChecksumEnd = e.entryEnd() + e.checksumStrategy.extraEntryBytes();
            if (entryAndChecksumEnd > s.segmentBytes.capacity()) {
                report(corruptionListener, corruption, segmentIndex, () ->
                        format("Wrong value size: {}, key: {}", e.valueSize, e.key())
                );
                return -1;
            }
        } catch (Exception ex) {
            reportException(corruptionListener, corruption, segmentIndex, () ->
                    "Exception while reading entry value size, key: " + e.key(), ex);
            return -1;
        }

        int storedChecksum = e.checksumStrategy.storedChecksum();
        int checksumFromEntry = e.checksumStrategy.computeChecksum();
        if (storedChecksum != checksumFromEntry) {
            report(corruptionListener, corruption, segmentIndex, () ->
                    format("Checksum doesn't match, stored: {}, should be from " +
                                    "the entry bytes: {}, key: {}, value: {}",
                            storedChecksum, checksumFromEntry, e.key(), e.value())
            );
            return -1;
        }

        if (!s.freeList.isRangeClear(entryPos, entryPos + e.entrySizeInChunks)) {
            report(corruptionListener, corruption, segmentIndex, () ->
                    format("Overlapping entry: positions {}-{}, key: {}, value: {}",
                            entryPos, entryPos + e.entrySizeInChunks - 1, e.key(), e.value())
            );
            return -1;
        }

        if (segmentIndex < 0) {
            return segmentIndexFromKey;
        } else {
            if (segmentIndex != segmentIndexFromKey) {
                report(corruptionListener, corruption, segmentIndex, () ->
                        format("Expected segment index: {}, segment index from the entry key: {}, " +
                                        "key: {}, value: {}",
                                segmentIndex, searchKeyFromKey, e.key(), e.value())
                );
                return -1;
            } else {
                return segmentIndex;
            }
        }
    }
}
