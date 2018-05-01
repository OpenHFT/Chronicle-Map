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
