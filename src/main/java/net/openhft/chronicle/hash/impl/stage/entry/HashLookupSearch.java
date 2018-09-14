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

import net.openhft.chronicle.hash.impl.CompactOffHeapLinearHashTable;
import net.openhft.chronicle.hash.impl.VanillaChronicleHashHolder;
import net.openhft.chronicle.hash.impl.stage.query.KeySearch;
import net.openhft.chronicle.map.impl.stage.entry.MapEntryStages;
import net.openhft.sg.Stage;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

import static net.openhft.chronicle.hash.impl.CompactOffHeapLinearHashTable.UNSET_KEY;

@Staged
public abstract class HashLookupSearch {

    @StageRef
    public VanillaChronicleHashHolder<?> hh;
    @Stage("SearchKey")
    public long searchStartPos;
    @StageRef
    SegmentStages s;
    @StageRef
    HashLookupPos hlp;
    @StageRef
    KeySearch<?> ks;
    @StageRef
    MapEntryStages<?, ?> e;
    @Stage("SearchKey")
    long searchKey = UNSET_KEY;

    public CompactOffHeapLinearHashTable hl() {
        return hh.h().hashLookup;
    }

    public void initSearchKey(long searchKey) {
        this.searchKey = searchKey;
        searchStartPos = hl().hlPos(searchKey);
    }

    private long addr() {
        return s.tierBaseAddr;
    }

    public long nextPos() {
        long pos = hlp.hashLookupPos;
        CompactOffHeapLinearHashTable hl = hl();
        while (true) {
            // read volatile to make a happens-before edge between entry insertion from concurrent
            // thread under update lock and this thread (reading the entry)
            long entry = hl.readEntryVolatile(addr(), pos);
            if (hl.empty(entry)) {
                hlp.setHashLookupPos(pos);
                return -1L;
            }
            pos = hl.step(pos);
            if (pos == searchStartPos)
                break;
            if (hl.key(entry) == searchKey) {
                hlp.setHashLookupPos(pos);
                return hl.value(entry);
            }
        }
        throw new IllegalStateException(hh.h().toIdentityString() +
                ": HashLookup overflow should never occur");
    }

    public void found() {
        hlp.setHashLookupPos(hl().stepBack(hlp.hashLookupPos));
    }

    public void remove() {
        hlp.setHashLookupPos(hl().remove(addr(), hlp.hashLookupPos));
    }

    public void putNewVolatile(long entryPos) {
        // Correctness check + make putNewVolatile() dependant on keySearch, this, in turn,
        // is needed for hlp.hashLookupPos re-initialization after nextTier().
        // Not an assert statement, because ks.searchStatePresent() should run regardless assertions
        // enabled or not.
        boolean keySearchReInit = !ks.keySearchInit();
        if (ks.searchStatePresent())
            throw new AssertionError();
        if (keySearchReInit) {
            // if key search was re-init, entry was re-init too during the search
            e.readExistingEntry(entryPos);
        }

        hl().checkValueForPut(entryPos);
        hl().writeEntryVolatile(addr(), hlp.hashLookupPos, searchKey, entryPos);
    }

    public boolean checkSlotContainsExpectedKeyAndValue(long value) {
        // volatile read not needed here because this method is for verifying within-thread
        // invariants
        long entry = hl().readEntry(addr(), hlp.hashLookupPos);
        return hl().key(entry) == searchKey && hl().value(entry) == value;
    }
}
