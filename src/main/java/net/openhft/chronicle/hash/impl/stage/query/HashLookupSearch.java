/*
 * Copyright 2015 Higher Frequency Trading
 *
 *  http://www.higherfrequencytrading.com
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package net.openhft.chronicle.hash.impl.stage.query;

import net.openhft.chronicle.hash.impl.VanillaChronicleHashHolder;
import net.openhft.chronicle.hash.impl.stage.entry.HashEntryStages;
import net.openhft.chronicle.hash.impl.stage.entry.HashLookup;
import net.openhft.chronicle.hash.impl.stage.entry.HashLookupPos;
import net.openhft.chronicle.hash.impl.stage.entry.SegmentStages;
import net.openhft.sg.Stage;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

import static net.openhft.chronicle.hash.impl.stage.entry.HashLookup.UNSET_KEY;

@Staged
public abstract class HashLookupSearch {
    
    @StageRef SegmentStages s;
    @StageRef HashEntryStages<?> e;
    @StageRef HashLookup hl;
    @StageRef HashQuery op;
    @StageRef VanillaChronicleHashHolder<?, ?, ?> hh;
    @StageRef HashLookupPos hlp;
    
    @Stage("SearchKey") long searchKey = UNSET_KEY;
    @Stage("SearchKey") long searchStartPos;
    
    void initSearchKey() {
        searchKey = hl.maskUnsetKey(hh.h().hashSplitting.segmentHash(op.hashOfKey));
        searchStartPos = hl.hlPos(searchKey);
    }

    public long nextPos() {
        long pos = hlp.hashLookupPos;
        while (true) {
            long entry = hl.readEntry(pos);
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
        throw new IllegalStateException("MultiMap is full, that most likely means you " +
                "misconfigured entrySize/chunkSize, and entries tend to take less chunks than " +
                "expected");
    }

    public void found() {
        hlp.setHashLookupPos(hl.stepBack(hlp.hashLookupPos));
    }

    public void remove() {
        hlp.setHashLookupPos(hl.remove(hlp.hashLookupPos));
    }

    void put(long value) {
        hl.checkValueForPut(value);
        hl.writeEntry(hlp.hashLookupPos, hl.readEntry(hlp.hashLookupPos), searchKey, value);
    }

    public void putVolatile(long value) {
        hl.checkValueForPut(value);
        long currentEntry = hl.readEntry(hlp.hashLookupPos);
        assert hl.key(currentEntry) == searchKey;
        hl.writeEntryVolatile(hlp.hashLookupPos, currentEntry, searchKey, value);
    }
    
    public void putNewVolatile(long value) {
        hl.checkValueForPut(value);
        long currentEntry = hl.readEntry(hlp.hashLookupPos);
        hl.writeEntryVolatile(hlp.hashLookupPos, currentEntry, searchKey, value);
    }
    
    public boolean checkSlotContainsExpectedKeyAndValue(long value) {
        long entry = hl.readEntry(hlp.hashLookupPos);
        return hl.key(entry) == searchKey && hl.value(entry) == value;
    }
    
    public boolean checkSlotIsEmpty() {
        return hl.empty(hl.readEntry(hlp.hashLookupPos));
    }


}
