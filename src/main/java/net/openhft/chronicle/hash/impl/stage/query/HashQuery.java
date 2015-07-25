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

package net.openhft.chronicle.hash.impl.stage.query;

import net.openhft.chronicle.algo.hashing.LongHashFunction;
import net.openhft.chronicle.bytes.BytesUtil;
import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.HashEntry;
import net.openhft.chronicle.hash.impl.VanillaChronicleHashHolder;
import net.openhft.chronicle.hash.impl.stage.data.bytes.InputKeyBytesData;
import net.openhft.chronicle.hash.impl.stage.data.instance.InputKeyInstanceData;
import net.openhft.chronicle.hash.impl.stage.entry.HashEntryStages;
import net.openhft.chronicle.hash.impl.stage.entry.HashLookupPos;
import net.openhft.chronicle.hash.impl.stage.entry.SegmentStages;
import net.openhft.chronicle.hash.impl.stage.hash.CheckOnEachPublicOperation;
import net.openhft.sg.Stage;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

import static net.openhft.chronicle.hash.impl.stage.query.HashQuery.SearchState.DELETED;
import static net.openhft.chronicle.hash.impl.stage.query.HashQuery.SearchState.PRESENT;

@Staged
public abstract class HashQuery<K> implements HashEntry<K> {

    @StageRef public VanillaChronicleHashHolder<K, ?, ?> hh;
    @StageRef public SegmentStages s;
    @StageRef public HashEntryStages<K> entry;
    @StageRef public HashLookupSearch hashLookupSearch;
    @StageRef public CheckOnEachPublicOperation checkOnEachPublicOperation;
    @StageRef public HashLookupPos hlp;
    
    @StageRef public InputKeyInstanceData<K, ?, ?> inputKeyInstanceValue;
    @StageRef public InputKeyBytesData<K> inputKeyBytesValue;
    
    public void dropSearchIfNestedContextsAndPresentHashLookupSlotCheckFailed() {
        if (s.locksInit()) {
            if (s.concurrentSameThreadContexts &&
                    s.rootContextOnThisSegment.latestSameThreadSegmentModCount !=
                            s.contextModCount) {
                if (keySearchInit()) {
                    if (searchState == PRESENT) {
                        if (!hashLookupSearch.checkSlotContainsExpectedKeyAndValue(entry.pos))
                            hlp.closeHashLookupPos();
                    }
                }
            }
        }
    }

    public Data<K> inputKey = null;
    
    public void initInputKey(Data<K> inputKey) {
        this.inputKey = inputKey;
    }

    public Data<K> queriedKey() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return inputKey;
    }
    
    public long hashOfKey = 0;
    
    void initHashOfKey() {
        hashOfKey = inputKey.hash(LongHashFunction.city_1_1());
    }
    
    public enum SearchState {
        PRESENT,
        DELETED,
        ABSENT
    }
    
    @Stage("KeySearch") protected SearchState searchState = null;
    
    abstract boolean keySearchInit();

    @Stage("KeySearch")
    public void setSearchState(SearchState newSearchState) {
        this.searchState = newSearchState;
    }

    void initKeySearch() {
        for (long pos; (pos = hashLookupSearch.nextPos()) >= 0L;) {
            entry.readExistingEntry(pos);
            if (!keyEquals())
                continue;
            hashLookupSearch.found();
            keyFound();
            return;
        }
        searchState = SearchState.ABSENT;
    }

    boolean keyEquals() {
        return inputKey.size() == entry.keySize &&
                BytesUtil.bytesEqual(entry.entryBS, entry.keyOffset,
                        inputKey.bytes(), inputKey.offset(), entry.keySize);
    }
    
    @Stage("KeySearch")
    void keyFound() {
        searchState = PRESENT;
    }
    
    abstract void closeKeySearch();
    
    public boolean searchStatePresent() {
        return searchState == PRESENT;
    }
    
    public boolean searchStateDeleted() {
        return searchState == DELETED && !s.concurrentSameThreadContexts &&
                s.innerUpdateLock.isHeldByCurrentThread();
    }
    
    public boolean searchStateAbsent() {
        return !searchStatePresent() && !searchStateDeleted();
    }

    public boolean entryPresent() {
        return searchStatePresent();
    }

    @Override
    public void doRemove() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        s.innerUpdateLock.lock();
        if (searchStatePresent()) {
            // TODO optimize: if shift-deletion is trivial, updateLock.lock()
            s.innerWriteLock.lock();
            hashLookupSearch.remove();
            entry.innerRemoveEntryExceptHashLookupUpdate();
            setSearchState(DELETED);
        } else {
            throw new IllegalStateException("Entry is absent when doRemove() is called");
        }
    }
}
