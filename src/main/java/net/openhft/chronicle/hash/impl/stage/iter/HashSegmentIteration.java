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

import net.openhft.chronicle.hash.HashEntry;
import net.openhft.chronicle.hash.HashSegmentContext;
import net.openhft.chronicle.hash.impl.CompactOffHeapLinearHashTable;
import net.openhft.chronicle.hash.impl.VanillaChronicleHashHolder;
import net.openhft.chronicle.hash.impl.stage.entry.HashEntryStages;
import net.openhft.chronicle.hash.impl.stage.entry.HashLookupPos;
import net.openhft.chronicle.hash.impl.stage.hash.CheckOnEachPublicOperation;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

import java.util.function.Consumer;
import java.util.function.Predicate;

@Staged
public abstract class HashSegmentIteration<K, E extends HashEntry<K>>
        implements HashEntry<K>, HashSegmentContext<K, E> {

    @StageRef
    public IterationSegmentStages s;
    @StageRef
    public CheckOnEachPublicOperation checkOnEachPublicOperation;
    public boolean entryRemovedOnThisIteration = false;
    public long hashLookupEntry = 0;
    @StageRef
    protected HashLookupPos hlp;
    @StageRef
    HashEntryStages<K> e;
    @StageRef
    VanillaChronicleHashHolder<?> hh;

    public boolean shouldTestEntry() {
        return true;
    }

    public Object entryForIteration() {
        return this;
    }

    public long tierEntriesForIteration() {
        return s.tierEntries();
    }

    abstract boolean entryRemovedOnThisIterationInit();

    protected void initEntryRemovedOnThisIteration(boolean entryRemovedOnThisIteration) {
        this.entryRemovedOnThisIteration = entryRemovedOnThisIteration;
    }

    public abstract boolean hashLookupEntryInit();

    public void initHashLookupEntry(long entry) {
        hashLookupEntry = entry;
    }

    abstract void closeHashLookupEntry();

    @Override
    public boolean forEachSegmentEntryWhile(Predicate<? super E> predicate) {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        s.innerUpdateLock.lock();
        return innerForEachSegmentEntryWhile(predicate);
    }

    public <T> boolean innerForEachSegmentEntryWhile(Predicate<? super T> predicate) {
        try {
            s.goToLastTier();
            while (true) {
                int currentTier = s.tier;
                long currentTierBaseAddr = s.tierBaseAddr;
                long currentTierIndex = s.tierIndex;
                boolean interrupted = forEachTierEntryWhile(
                        predicate, currentTier, currentTierBaseAddr, currentTierIndex);
                if (interrupted)
                    return false;
                if (currentTier == 0)
                    return true;
                s.prevTier();
            }
        } finally {
            closeHashLookupEntry();
            s.innerReadLock.unlock();
            initEntryRemovedOnThisIteration(false);
        }
    }

    public <T> boolean forEachTierEntryWhile(
            Predicate<? super T> predicate,
            int currentTier, long currentTierBaseAddr, long tierIndex) {
        long leftEntries = tierEntriesForIteration();
        boolean interrupted = false;
        long startPos = 0L;
        CompactOffHeapLinearHashTable hashLookup = hh.h().hashLookup;
        // volatile read not needed because iteration is performed at least under update lock
        while (!hashLookup.empty(hashLookup.readEntry(currentTierBaseAddr, startPos))) {
            startPos = hashLookup.step(startPos);
        }
        hlp.initHashLookupPos(startPos);
        long currentHashLookupPos;
        int steps = 0;
        do {
            // Step from hlp.hashLookupPos, not currentHashLookupPos (with additional initialization
            // of this local variable to startPos outside the loop), because if e.remove() is
            // called in the `predicate`, hlp.hashLookupPos is stepped back in doRemove(), and
            // currentHashLookupPos become invalid
            currentHashLookupPos = hashLookup.step(hlp.hashLookupPos);
            steps++;
            hlp.setHashLookupPos(currentHashLookupPos);
            // volatile read not needed because iteration is performed at least under update lock
            long entry = hashLookup.readEntry(currentTierBaseAddr, currentHashLookupPos);
            initHashLookupEntry(entry);
            if (!hashLookup.empty(entry)) {
                e.readExistingEntry(hashLookup.value(entry));
                if (shouldTestEntry()) {
                    initEntryRemovedOnThisIteration(false);
                    try {
                        if (!predicate.test((T) entryForIteration())) {
                            interrupted = true;
                            break;
                        } else {
                            if (--leftEntries == 0)
                                break;
                        }
                    } finally {
                        hookAfterEachIteration();
                        // if doReplaceValue() -> relocation() -> alloc() -> nextTier()
                        // was called, restore the tier we were iterating over
                        if (s.tier != currentTier) {
                            s.initSegmentTier_WithBaseAddr(
                                    currentTier, currentTierBaseAddr, tierIndex);
                            // To cover shift deleted slot, at the next step forward.
                            // hash lookup entry is relocated to the next chained tier, and the
                            // slot in _current_ tier's hash lookup is shift deleted, see
                            // relocation()
                            currentHashLookupPos = hashLookup.stepBack(currentHashLookupPos);
                            steps--;
                            hlp.initHashLookupPos(currentHashLookupPos);
                        }
                        s.innerWriteLock.unlock();
                        // force entry checksum update (delayedUpdateChecksum depends on keyOffset)
                        e.closeKeyOffset();
                    }
                }
            }
            // the `steps == 0` condition and this variable updates in the loop fix the bug, when
            // shift deletion occurs on the first entry of the tier, and the currentHashLookupPos
            // becomes equal to start pos without making the whole loop, but only visiting a single
            // entry
        } while (currentHashLookupPos != startPos || steps == 0);
        if (!interrupted && leftEntries > 0) {
            throw new IllegalStateException(hh.h().toIdentityString() +
                    ": We a tier without interruption, " +
                    "but according to tier counters there should be " + leftEntries +
                    " more entries. Size diverged?");
        }
        return interrupted;
    }

    public void hookAfterEachIteration() {
    }

    @Override
    public void forEachSegmentEntry(Consumer<? super E> action) {
        forEachSegmentEntryWhile(e -> {
            action.accept(e);
            return true;
        });
    }

    public void checkEntryNotRemovedOnThisIteration() {
        if (entryRemovedOnThisIterationInit()) {
            throw new IllegalStateException(
                    hh.h().toIdentityString() + ": Entry was already removed on this iteration");
        }
    }

    @Override
    public void doRemove() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        s.innerWriteLock.lock();
        try {
            iterationRemove();
        } finally {
            s.innerWriteLock.unlock();
        }
        initEntryRemovedOnThisIteration(true);
    }

    public void iterationRemove() {
        // this condition mean -- some other entry taken place of the removed one
        if (hh.h().hashLookup.remove(s.tierBaseAddr, hlp.hashLookupPos) != hlp.hashLookupPos) {
            // if so, should make step back, to compensate step forward on the next iteration,
            // to consume the shifted entry
            hlp.setHashLookupPos(hh.h().hashLookup.stepBack(hlp.hashLookupPos));
        }
        e.innerRemoveEntryExceptHashLookupUpdate();
    }
}
