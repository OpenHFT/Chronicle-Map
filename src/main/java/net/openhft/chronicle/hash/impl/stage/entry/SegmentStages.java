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

import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.SegmentLock;
import net.openhft.chronicle.hash.impl.*;
import net.openhft.chronicle.hash.impl.stage.hash.Chaining;
import net.openhft.chronicle.hash.impl.stage.hash.CheckOnEachPublicOperation;
import net.openhft.chronicle.hash.impl.stage.query.KeySearch;
import net.openhft.chronicle.hash.locks.InterProcessLock;
import net.openhft.lang.collection.DirectBitSet;
import net.openhft.lang.collection.SingleThreadedDirectBitSet;
import net.openhft.lang.io.MultiStoreBytes;
import net.openhft.sg.Stage;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;

import static net.openhft.chronicle.hash.impl.LocalLockState.UNLOCKED;

@Staged
public abstract class SegmentStages implements SegmentLock {

    @StageRef Chaining chaining;
    @StageRef public VanillaChronicleHashHolder<?, ?, ?> hh;
    @StageRef public CheckOnEachPublicOperation checkOnEachPublicOperation;

    public int segmentIndex = -1;
    
    public void initSegmentIndex(int segmentIndex) {
        this.segmentIndex = segmentIndex;
    }

    @Stage("SegmentHeader") long segmentHeaderAddress;
    @Stage("SegmentHeader") SegmentHeader segmentHeader = null;

    abstract boolean segmentHeaderInit();

    private void initSegmentHeader() {
        segmentHeaderAddress = hh.h().ms.address() + hh.h().segmentHeaderOffset(segmentIndex);
        segmentHeader = BigSegmentHeader.INSTANCE;
    }

    public long entries() {
        return segmentHeader.size(segmentHeaderAddress);
    }

    public void entries(long size) {
        segmentHeader.size(segmentHeaderAddress, size);
    }

    long nextPosToSearchFrom() {
        return segmentHeader.nextPosToSearchFrom(segmentHeaderAddress);
    }

    public void nextPosToSearchFrom(long nextPosToSearchFrom) {
        segmentHeader.nextPosToSearchFrom(segmentHeaderAddress, nextPosToSearchFrom);
    }

    public long deleted() {
        return segmentHeader.deleted(segmentHeaderAddress);
    }

    public void deleted(long deleted) {
        segmentHeader.deleted(segmentHeaderAddress, deleted);
    }

    public long size() {
        return entries() - deleted();
    }


    @Stage("Locks") public SegmentStages rootContextOnThisSegment = null;
    /**
     * See the ChMap Ops spec, considerations of nested same-thread concurrent contexts.
     * Once context enters the segment, and observes concurrent same-thread context,
     * it sets concurrentSameThreadContexts = true for itself and that concurrent context.
     * This flag is not dropped on exit of one of these contexts, because between calls of
     * this context, nested one could be initialized, does some changes that break our thread-local
     * assumptions _and exit_, that is why on exit concurrent context should remain "dirty".
     */
    @Stage("Locks") public boolean concurrentSameThreadContexts;

    @Stage("Locks") public int latestSameThreadSegmentModCount;
    @Stage("Locks") public int contextModCount;

    @Stage("Locks")
    public void incrementModCount() {
        contextModCount = ++rootContextOnThisSegment.latestSameThreadSegmentModCount;
    }

    // chain
    @Stage("Locks") SegmentStages nextNode;

    @Stage("Locks") LocalLockState localLockState;
    @Stage("Locks") int totalReadLockCount;
    @Stage("Locks") int totalUpdateLockCount;
    @Stage("Locks") int totalWriteLockCount;

    @Stage("Locks")
    public boolean readZero() {
        return rootContextOnThisSegment.totalReadLockCount == 0;
    }

    @Stage("Locks")
    public boolean updateZero() {
        return rootContextOnThisSegment.totalUpdateLockCount == 0;
    }

    @Stage("Locks")
    public boolean writeZero() {
        return rootContextOnThisSegment.totalWriteLockCount == 0;
    }

    @Stage("Locks")
    public int decrementRead() {
        return rootContextOnThisSegment.totalReadLockCount -= 1;
    }

    @Stage("Locks")
    public int decrementUpdate() {
        return rootContextOnThisSegment.totalUpdateLockCount -= 1;
    }

    @Stage("Locks")
    public int decrementWrite() {
        return rootContextOnThisSegment.totalWriteLockCount -= 1;
    }

    @Stage("Locks")
    public void incrementRead() {
        rootContextOnThisSegment.totalReadLockCount += 1;
    }

    @Stage("Locks")
    public void incrementUpdate() {
        rootContextOnThisSegment.totalUpdateLockCount += 1;
    }

    @Stage("Locks")
    public void incrementWrite() {
        rootContextOnThisSegment.totalWriteLockCount += 1;
    }

    public abstract boolean locksInit();

    void initLocks() {
        localLockState = UNLOCKED;
        int indexOfThisContext = chaining.indexInContextChain;
        for (int i = indexOfThisContext - 1; i >= 0; i--) {
            if (tryFindInitLocksOfThisSegment(this, i))
                return;
        }
        for (int i = indexOfThisContext + 1, size = chaining.contextChain.size(); i < size; i++) {
            if (tryFindInitLocksOfThisSegment(this, i))
                return;
        }
        rootContextOnThisSegment = this;
        concurrentSameThreadContexts = false;

        latestSameThreadSegmentModCount = 0;
        contextModCount = 0;

        totalReadLockCount = 0;
        totalUpdateLockCount = 0;
        totalWriteLockCount = 0;
    }

    @Stage("Locks")
    boolean tryFindInitLocksOfThisSegment(Object thisContext, int index) {
        SegmentStages c = chaining.contextAtIndexInChain(index);
        if (c.segmentHeaderInit() &&
                c.segmentHeaderAddress == segmentHeaderAddress &&
                c.locksInit()) {
            SegmentStages root = c.rootContextOnThisSegment;
            this.rootContextOnThisSegment = root;
            root.concurrentSameThreadContexts = true;
            this.concurrentSameThreadContexts = true;
            this.contextModCount = root.latestSameThreadSegmentModCount;
            linkToSegmentContextsChain();
            return true;
        } else {
            return false;
        }
    }

    void closeLocks() {
        if (rootContextOnThisSegment == this) {
            closeRootLocks();
        } else {
            closeNestedLocks();
        }
        localLockState = null;
        rootContextOnThisSegment = null;
    }

    @Stage("Locks")
    private void closeNestedLocks() {
        unlinkFromSegmentContextsChain();
        readUnlockAndDecrementCount();
    }

    @Stage("Locks")
    public void readUnlockAndDecrementCount() {
        switch (localLockState) {
            case UNLOCKED:
                return;
            case READ_LOCKED:
                int newTotalReadLockCount = decrementRead();
                if (newTotalReadLockCount == 0) {
                    if (updateZero() && writeZero())
                        segmentHeader.readUnlock(segmentHeaderAddress);
                } else {
                    assert newTotalReadLockCount > 0 : "read underflow";
                }
                return;
            case UPDATE_LOCKED:
                int newTotalUpdateLockCount = decrementUpdate();
                if (newTotalUpdateLockCount == 0) {
                    if (writeZero()) {
                        if (readZero()) {
                            segmentHeader.updateUnlock(segmentHeaderAddress);
                        } else {
                            segmentHeader.downgradeUpdateToReadLock(segmentHeaderAddress);
                        }
                    }
                } else {
                    assert newTotalUpdateLockCount > 0 : "update underflow";
                }
                return;
            case WRITE_LOCKED:
                int newTotalWriteLockCount = decrementWrite();
                if (newTotalWriteLockCount == 0) {
                    if (!updateZero()) {
                        segmentHeader.downgradeWriteToUpdateLock(segmentHeaderAddress);
                    } else {
                        if (!readZero()) {
                            segmentHeader.downgradeWriteToReadLock(segmentHeaderAddress);
                        } else {
                            segmentHeader.writeUnlock(segmentHeaderAddress);
                        }
                    }
                } else {
                    assert newTotalWriteLockCount > 0 : "write underflow";
                }
        }
    }

    @Stage("Locks")
    private void linkToSegmentContextsChain() {
        SegmentStages innermostContextOnThisSegment = rootContextOnThisSegment;
        while (true) {
            Data key = ((KeySearch) (Object) innermostContextOnThisSegment).inputKey;
            if (Objects.equals(key, ((KeySearch) (Object) this).inputKey)) {
                throw new IllegalStateException("Nested same-thread contexts cannot access " +
                        "the same key " + key);
            }
            if (innermostContextOnThisSegment.nextNode == null)
                break;
            innermostContextOnThisSegment = innermostContextOnThisSegment.nextNode;
        }
        innermostContextOnThisSegment.nextNode = this;
    }

    @Stage("Locks")
    private void unlinkFromSegmentContextsChain() {
        SegmentStages prevContext = rootContextOnThisSegment;
        while (true) {
            assert prevContext.nextNode != null;
            if (prevContext.nextNode == this)
                break;
            prevContext = prevContext.nextNode;
        }
        // i. e. structured unlocking
        verifyInnermostContext();
        prevContext.nextNode = null;
    }

    @Stage("Locks")
    private void verifyInnermostContext() {
        if (nextNode != null)
            throw new IllegalStateException("Attempt to close contexts not structurally");
    }

    @Stage("Locks")
    private void closeRootLocks() {
        verifyInnermostContext();

        switch (localLockState) {
            case UNLOCKED:
                return;
            case READ_LOCKED:
                segmentHeader.readUnlock(segmentHeaderAddress);
                return;
            case UPDATE_LOCKED:
                segmentHeader.updateUnlock(segmentHeaderAddress);
                return;
            case WRITE_LOCKED:
                segmentHeader.writeUnlock(segmentHeaderAddress);
        }
    }

    @Stage("Locks")
    public void setLocalLockState(LocalLockState newState) {
        localLockState = newState;
    }

    @StageRef public ReadLock innerReadLock;
    @StageRef public UpdateLock innerUpdateLock;
    @StageRef public WriteLock innerWriteLock;

    @NotNull
    @Override
    public InterProcessLock readLock() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return innerReadLock;
    }

    @NotNull
    @Override
    public InterProcessLock updateLock() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return innerUpdateLock;
    }

    @NotNull
    @Override
    public InterProcessLock writeLock() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return innerWriteLock;
    }
    
    @Stage("Segment") MultiStoreBytes freeListBytes = new MultiStoreBytes();
    @Stage("Segment") public SingleThreadedDirectBitSet freeList = new SingleThreadedDirectBitSet();
    @Stage("Segment") long entrySpaceOffset = 0;
    @Stage("Segment") public long segmentBase;

    boolean segmentInit() {
        return entrySpaceOffset > 0;
    }

    void initSegment() {
        VanillaChronicleHash<?, ?, ?, ?, ?, ?> h = hh.h();
        long hashLookupOffset = h.segmentOffset(segmentIndex);
        segmentBase = hh.h().ms.address() + hashLookupOffset;
        long freeListOffset = hashLookupOffset + h.segmentHashLookupOuterSize;
        freeListBytes.storePositionAndSize(h.ms, freeListOffset, h.segmentFreeListInnerSize);
        freeList.reuse(freeListBytes);
        entrySpaceOffset = freeListOffset + h.segmentFreeListOuterSize +
                h.segmentEntrySpaceInnerOffset;
    }
    
    void closeSegment() {
        entrySpaceOffset = 0;
    }

    public long allocReturnCode(int chunks) {
        VanillaChronicleHash<?, ?, ?, ?, ?, ?> h = hh.h();
        if (chunks > h.maxChunksPerEntry)
            throw new IllegalArgumentException("Entry is too large: requires " + chunks +
                    " entry size chucks, " + h.maxChunksPerEntry + " is maximum.");
        long ret = freeList.setNextNContinuousClearBits(nextPosToSearchFrom(), chunks);
        if (ret == DirectBitSet.NOT_FOUND || ret + chunks > h.actualChunksPerSegment) {
            if (ret != DirectBitSet.NOT_FOUND &&
                    ret + chunks > h.actualChunksPerSegment && ret < h.actualChunksPerSegment) {
                freeList.clear(ret, h.actualChunksPerSegment);
            }
            ret = freeList.setNextNContinuousClearBits(0L, chunks);
            if (ret == DirectBitSet.NOT_FOUND || ret + chunks > h.actualChunksPerSegment) {
                if (ret != DirectBitSet.NOT_FOUND &&
                        ret + chunks > h.actualChunksPerSegment &&
                        ret < h.actualChunksPerSegment) {
                    freeList.clear(ret, h.actualChunksPerSegment);
                }
                return -1;
            }
            updateNextPosToSearchFrom(ret, chunks);
        } else {
            // if bit at nextPosToSearchFrom is clear, it was skipped because
            // more than 1 chunk was requested. Don't move nextPosToSearchFrom
            // in this case. chunks == 1 clause is just a fast path.
            if (chunks == 1 || freeList.isSet(nextPosToSearchFrom())) {
                updateNextPosToSearchFrom(ret, chunks);
            }
        }
        return ret;
    }

    public long alloc(int chunks) {
        long ret = allocReturnCode(chunks);
        if (ret >= 0) {
            return ret;
        } else {
            if (chunks == 1) {
                throw new IllegalStateException(
                        "Segment is full, no free entries found");
            } else {
                throw new IllegalStateException(
                        "Segment is full or has no ranges of " + chunks
                                + " continuous free chunks"
                );
            }
        }
    }

    public void free(long fromPos, int chunks) {
        freeList.clear(fromPos, fromPos + chunks);
        if (fromPos < nextPosToSearchFrom())
            nextPosToSearchFrom(fromPos);
    }

    public void updateNextPosToSearchFrom(long allocated, int chunks) {
        long nextPosToSearchFrom = allocated + chunks;
        if (nextPosToSearchFrom >= hh.h().actualChunksPerSegment)
            nextPosToSearchFrom = 0L;
        nextPosToSearchFrom(nextPosToSearchFrom);
    }

    public void clearSegment() {
        innerWriteLock.lock();
        hh.h().hashLookup.clearHashLookup(segmentBase);
        freeList.clear();
        nextPosToSearchFrom(0L);
        entries(0L);
    }
    
    public void clear() {
        clearSegment();
    }
}
