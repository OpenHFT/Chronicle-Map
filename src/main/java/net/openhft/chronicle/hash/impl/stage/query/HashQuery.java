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

package net.openhft.chronicle.hash.impl.stage.query;

import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.impl.VanillaChronicleHashHolder;
import net.openhft.chronicle.hash.impl.stage.entry.HashEntryStages;
import net.openhft.chronicle.hash.impl.stage.entry.HashLookupPos;
import net.openhft.chronicle.hash.impl.stage.entry.HashLookupSearch;
import net.openhft.chronicle.hash.impl.stage.entry.SegmentStages;
import net.openhft.chronicle.hash.impl.stage.hash.CheckOnEachPublicOperation;
import net.openhft.chronicle.hash.serialization.DataAccess;
import net.openhft.chronicle.set.SetEntry;
import net.openhft.sg.Stage;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

import static net.openhft.chronicle.hash.impl.stage.query.KeySearch.SearchState.ABSENT;

@Staged
public abstract class HashQuery<K> implements SetEntry<K> {

    @StageRef
    public VanillaChronicleHashHolder<K> hh;
    final DataAccess<K> innerInputKeyDataAccess = hh.h().keyDataAccess.copy();
    @StageRef
    public SegmentStages s;
    @StageRef
    public HashEntryStages<K> entry;
    @StageRef
    public HashLookupSearch hashLookupSearch;
    @StageRef
    public CheckOnEachPublicOperation checkOnEachPublicOperation;
    @StageRef
    public HashLookupPos hlp;
    @StageRef
    public KeySearch<K> ks;
    /**
     * This stage exists for hooking {@link #innerInputKeyDataAccess} usage, to trigger {@link
     * DataAccess#uninit()} on context exit
     */
    @Stage("InputKeyDataAccess")
    private boolean inputKeyDataAccessInitialized = false;
    @Stage("PresenceOfEntry")
    private EntryPresence entryPresence = null;

    void initInputKeyDataAccess() {
        inputKeyDataAccessInitialized = true;
    }

    void closeInputKeyDataAccess() {
        innerInputKeyDataAccess.uninit();
        inputKeyDataAccessInitialized = false;
    }

    public DataAccess<K> inputKeyDataAccess() {
        initInputKeyDataAccess();
        return innerInputKeyDataAccess;
    }

    public void dropSearchIfNestedContextsAndPresentHashLookupSlotCheckFailed() {
        if (s.locksInit()) {
            if (s.nestedContextsLockedOnSameSegment &&
                    s.rootContextLockedOnThisSegment.latestSameThreadSegmentModCount() !=
                            s.contextModCount) {
                if (ks.keySearchInit() && ks.searchStatePresent() &&
                        !hashLookupSearch.checkSlotContainsExpectedKeyAndValue(entry.pos)) {
                    hlp.closeHashLookupPos();
                }
            }
        }
    }

    public Data<K> queriedKey() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return ks.inputKey;
    }

    private void initPresenceOfEntry() {
        if (ks.searchStatePresent() || tieredEntryPresent()) {
            entryPresence = EntryPresence.PRESENT;
        } else {
            entryPresence = EntryPresence.ABSENT;
        }
    }

    public void initPresenceOfEntry(EntryPresence entryPresence) {
        this.entryPresence = entryPresence;
    }

    private boolean tieredEntryPresent() {
        int firstTier = s.tier;
        long firstTierBaseAddr = s.tierBaseAddr;
        while (true) {
            if (s.hasNextTier()) {
                s.nextTier();
            } else {
                if (s.tier != 0)
                    s.initSegmentTier(); // loop to the root tier
            }
            if (s.tierBaseAddr == firstTierBaseAddr)
                break;
            if (ks.searchStatePresent())
                return true;
        }
        // not found
        if (firstTier != 0) {
            // key is absent; probably are going to allocate a new entry;
            // start trying from the root tier
            s.initSegmentTier();
        }
        return false;
    }

    public boolean entryPresent() {
        return entryPresence == EntryPresence.PRESENT;
    }

    @Override
    public void doRemove() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        s.innerWriteLock.lock();
        if (ks.searchStatePresent()) {
            entry.innerRemoveEntryExceptHashLookupUpdate();
            hashLookupSearch.remove();
            ks.setSearchState(ABSENT);
            initPresenceOfEntry(EntryPresence.ABSENT);
        } else {
            throw new IllegalStateException(
                    hh.h().toIdentityString() + ": Entry is absent when doRemove() is called");
        }
    }

    public enum EntryPresence {PRESENT, ABSENT}
}
