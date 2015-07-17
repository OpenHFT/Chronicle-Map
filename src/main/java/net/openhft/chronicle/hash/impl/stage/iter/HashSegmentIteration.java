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

package net.openhft.chronicle.hash.impl.stage.iter;

import net.openhft.chronicle.hash.HashEntry;
import net.openhft.chronicle.hash.impl.stage.entry.HashEntryStages;
import net.openhft.chronicle.hash.impl.stage.entry.HashLookup;
import net.openhft.chronicle.hash.impl.stage.entry.HashLookupPos;
import net.openhft.chronicle.hash.impl.stage.entry.SegmentStages;
import net.openhft.chronicle.hash.impl.stage.hash.CheckOnEachPublicOperation;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

import java.util.function.Predicate;

@Staged
public abstract class HashSegmentIteration<K, E extends HashEntry<K>> implements HashEntry<K> {
    
    @StageRef public SegmentStages s;
    @StageRef HashEntryStages<K> e;
    @StageRef protected HashLookup hashLookup;
    @StageRef public CheckOnEachPublicOperation checkOnEachPublicOperation;
    @StageRef protected HashLookupPos hlp;
    
    public boolean entryIsPresent() {
        return true;
    }

    public boolean entryRemovedOnThisIteration = false;
    
    abstract boolean entryRemovedOnThisIterationInit();
    
    protected void initEntryRemovedOnThisIteration(boolean entryRemovedOnThisIteration) {
        this.entryRemovedOnThisIteration = entryRemovedOnThisIteration;
    }

    public boolean forEachRemoving(Predicate<? super E> action) {
        s.innerUpdateLock.lock();
        try {
            long size = s.size();
            if (size == 0)
                return true;
            boolean interrupted = false;
            long startPos = 0L;
            while (!hashLookup.empty(hashLookup.readEntry(startPos))) {
                startPos = hashLookup.step(startPos);
            }
            hlp.initHashLookupPos(startPos);
            do {
                hlp.setHashLookupPos(hashLookup.step(hlp.hashLookupPos));
                long entry = hashLookup.readEntry(hlp.hashLookupPos);
                if (!hashLookup.empty(entry)) {
                    e.readExistingEntry(hashLookup.value(entry));
                    if (entryIsPresent()) {
                        initEntryRemovedOnThisIteration(false);
                        if (!action.test((E) this)) {
                            interrupted = true;
                            break;
                        } else {
                            if (--size == 0)
                                break;
                        }
                        
                    }
                }
            } while (hlp.hashLookupPos != startPos);
            return !interrupted;
        } finally {
            s.innerReadLock.unlock();
            initEntryRemovedOnThisIteration(false);
        }
    }
    
    public void checkEntryNotRemovedOnThisIteration() {
        if (entryRemovedOnThisIterationInit())
            throw new IllegalStateException("Entry was already removed on this iteration");
    }

    @Override
    public void doRemove() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        initEntryRemovedOnThisIteration(true);
        s.innerWriteLock.lock();
        try {
            // this condition mean -- some other entry taken place of the removed one
            if (hashLookup.remove(hlp.hashLookupPos) != hlp.hashLookupPos) {
                // if so, should make step back, to compensate step forward on the next iteration,
                // to consume the shifted entry
                hlp.setHashLookupPos(hashLookup.stepBack(hlp.hashLookupPos));
            }
            e.innerRemoveEntryExceptHashLookupUpdate();
        } finally {
            s.innerWriteLock.unlock();
        }
    }
}
