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

import net.openhft.chronicle.hash.impl.CompactOffHeapLinearHashTable;
import net.openhft.chronicle.hash.impl.VanillaChronicleHashHolder;
import net.openhft.chronicle.hash.impl.stage.query.KeySearch;
import net.openhft.sg.Stage;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

import static net.openhft.chronicle.hash.impl.CompactOffHeapLinearHashTable.UNSET_KEY;

@Staged
public abstract class HashLookupSearch {
    
    @StageRef SegmentStages s;
    @StageRef public VanillaChronicleHashHolder<?> hh;
    @StageRef HashLookupPos hlp;
    @StageRef KeySearch<?> ks;
    
    @Stage("SearchKey") long searchKey = UNSET_KEY;
    @Stage("SearchKey") public long searchStartPos;

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
        while (true) {
            long entry = hl().readEntry(addr(), pos);
            if (hl().empty(entry)) {
                hlp.setHashLookupPos(pos);
                return -1L;
            }
            pos = hl().step(pos);
            if (pos == searchStartPos)
                break;
            if (hl().key(entry) == searchKey) {
                hlp.setHashLookupPos(pos);
                return hl().value(entry);
            }
        }
        throw new IllegalStateException("MultiMap is full, that most likely means you " +
                "misconfigured entrySize/chunkSize, and entries tend to take less chunks than " +
                "expected");
    }

    public void found() {
        hlp.setHashLookupPos(hl().stepBack(hlp.hashLookupPos));
    }

    public void remove() {
        hlp.setHashLookupPos(hl().remove(addr(), hlp.hashLookupPos));
    }

    public void putNewVolatile(long value) {
        // correctness check + make putNewVolatile() dependant on keySearch, this, in turn,
        // is needed for hlp.hashLookupPos re-initialization after nextTier()
        assert !ks.searchStatePresent();

        hl().checkValueForPut(value);
        long currentEntry = hl().readEntry(addr(), hlp.hashLookupPos);
        hl().writeEntryVolatile(addr(), hlp.hashLookupPos, currentEntry, searchKey, value);
    }
    
    public boolean checkSlotContainsExpectedKeyAndValue(long value) {
        long entry = hl().readEntry(addr(), hlp.hashLookupPos);
        return hl().key(entry) == searchKey && hl().value(entry) == value;
    }
}
