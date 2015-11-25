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

import net.openhft.chronicle.algo.bitset.BitSetFrame;
import net.openhft.chronicle.algo.bitset.ReusableBitSet;
import net.openhft.chronicle.algo.bitset.SingleThreadedFlatBitSetFrame;
import net.openhft.chronicle.algo.bytes.Access;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.PointerBytesStore;
import net.openhft.chronicle.bytes.VanillaBytes;
import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.SegmentLock;
import net.openhft.chronicle.hash.impl.*;
import net.openhft.chronicle.hash.impl.stage.hash.Chaining;
import net.openhft.chronicle.hash.impl.stage.hash.CheckOnEachPublicOperation;
import net.openhft.chronicle.hash.impl.stage.query.KeySearch;
import net.openhft.chronicle.hash.locks.InterProcessLock;
import net.openhft.chronicle.map.impl.IterationContext;
import net.openhft.sg.Stage;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;

import static net.openhft.chronicle.algo.MemoryUnit.BITS;
import static net.openhft.chronicle.algo.MemoryUnit.LONGS;
import static net.openhft.chronicle.hash.impl.LocalLockState.UNLOCKED;
import static net.openhft.chronicle.hash.impl.VanillaChronicleHash.TIER_COUNTERS_AREA_SIZE;

@Staged
public abstract class SegmentStages implements SegmentLock, LocksInterface {

    @StageRef Chaining chaining;
    @StageRef public VanillaChronicleHashHolder<?> hh;
    @StageRef public CheckOnEachPublicOperation checkOnEachPublicOperation;

    public int segmentIndex = -1;
    
    public void initSegmentIndex(int segmentIndex) {
        this.segmentIndex = segmentIndex;
    }

    public abstract boolean segmentIndexInit();

    @Stage("SegmentHeader") long segmentHeaderAddress;
    @Stage("SegmentHeader") SegmentHeader segmentHeader = null;

    private void initSegmentHeader() {
        segmentHeaderAddress = hh.h().segmentHeaderAddress(segmentIndex);
        segmentHeader = BigSegmentHeader.INSTANCE;
    }

    public long entries() {
        return segmentHeader.size(segmentHeaderAddress);
    }

    public void entries(long size) {
        segmentHeader.size(segmentHeaderAddress, size);
    }

    long nextPosToSearchFrom() {
        if (segmentTier == 0) {
            return segmentHeader.nextPosToSearchFrom(segmentHeaderAddress);
        } else {
            return nextPosToSearchFromTiered();
        }
    }

    public void nextPosToSearchFrom(long nextPosToSearchFrom) {
        if (segmentTier == 0) {
            segmentHeader.nextPosToSearchFrom(segmentHeaderAddress, nextPosToSearchFrom);
        } else {
            nextPosToSearchFromTiered(nextPosToSearchFrom);
        }
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


    @Stage("Locks") public LocksInterface rootContextLockedOnThisSegment = null;
    /**
     * See the ChMap Ops spec, considerations of nested same-thread concurrent contexts.
     * Once context enters the segment, and observes concurrent same-thread context,
     * it sets nestedContextsLockedOnSameSegment = true for itself and that concurrent context.
     * This flag is not dropped on exit of one of these contexts, because between calls of
     * this context, nested one could be initialized, does some changes that break our thread-local
     * assumptions _and exit_, that is why on exit concurrent context should remain "dirty".
     */
    @Stage("Locks") public boolean nestedContextsLockedOnSameSegment;

    @Override
    @Stage("Locks")
    public void setNestedContextsLockedOnSameSegment(boolean nestedContextsLockedOnSameSegment) {
        this.nestedContextsLockedOnSameSegment = nestedContextsLockedOnSameSegment;
    }

    @Stage("Locks") public int latestSameThreadSegmentModCount;

    @Override
    @Stage("Locks")
    public int changeAndGetLatestSameThreadSegmentModCount(int change) {
        return this.latestSameThreadSegmentModCount += change;
    }

    @Stage("Locks") public int contextModCount;

    @Stage("Locks")
    public void incrementModCount() {
        contextModCount =
                rootContextLockedOnThisSegment.changeAndGetLatestSameThreadSegmentModCount(1);
    }

    // chain
    @Stage("Locks") LocksInterface nextNode;

    @Override
    @Stage("Locks")
    public void setNextNode(LocksInterface nextNode) {
        this.nextNode = nextNode;
    }

    @Stage("Locks") LocalLockState localLockState;
    @Stage("Locks") int totalReadLockCount;
    @Stage("Locks") int totalUpdateLockCount;
    @Stage("Locks") int totalWriteLockCount;

    @Stage("Locks")
    public boolean readZero() {
        return rootContextLockedOnThisSegment.totalReadLockCount() == 0;
    }

    @Override
    @Stage("Locks")
    public int changeAndGetTotalReadLockCount(int change) {
        return totalReadLockCount += change;
    }

    @Stage("Locks")
    public boolean updateZero() {
        return rootContextLockedOnThisSegment.totalUpdateLockCount() == 0;
    }

    @Override
    @Stage("Locks")
    public int changeAndGetTotalUpdateLockCount(int change) {
        return totalUpdateLockCount += change;
    }

    @Stage("Locks")
    public boolean writeZero() {
        return rootContextLockedOnThisSegment.totalWriteLockCount() == 0;
    }

    @Override
    @Stage("Locks")
    public int changeAndGetTotalWriteLockCount(int change) {
        return totalWriteLockCount += change;
    }

    @Stage("Locks")
    public int decrementRead() {
        return rootContextLockedOnThisSegment.changeAndGetTotalReadLockCount(-1);
    }

    @Stage("Locks")
    public int decrementUpdate() {
        return rootContextLockedOnThisSegment.changeAndGetTotalUpdateLockCount(-1);
    }

    @Stage("Locks")
    public int decrementWrite() {
        return rootContextLockedOnThisSegment.changeAndGetTotalWriteLockCount(-1);
    }

    @Stage("Locks")
    public void incrementRead() {
        rootContextLockedOnThisSegment.changeAndGetTotalReadLockCount(1);
    }

    @Stage("Locks")
    public void incrementUpdate() {
        rootContextLockedOnThisSegment.changeAndGetTotalUpdateLockCount(1);
    }

    @Stage("Locks")
    public void incrementWrite() {
        rootContextLockedOnThisSegment.changeAndGetTotalWriteLockCount(1);
    }

    public abstract boolean locksInit();

    void initLocks() {
        localLockState = UNLOCKED;
        int indexOfThisContext = chaining.indexInContextChain;
        for (int i = indexOfThisContext - 1; i >= 0; i--) {
            if (tryFindInitLocksOfThisSegment(i))
                return;
        }
        for (int i = indexOfThisContext + 1, size = chaining.contextChain.size(); i < size; i++) {
            if (tryFindInitLocksOfThisSegment(i))
                return;
        }
        rootContextLockedOnThisSegment = this;
        nestedContextsLockedOnSameSegment = false;

        latestSameThreadSegmentModCount = 0;
        contextModCount = 0;

        totalReadLockCount = 0;
        totalUpdateLockCount = 0;
        totalWriteLockCount = 0;
    }

    @Stage("Locks")
    boolean tryFindInitLocksOfThisSegment(int index) {
        LocksInterface c = chaining.contextAtIndexInChain(index);
        if (c.segmentHeaderInit() &&
                c.segmentHeaderAddress() == segmentHeaderAddress &&
                c.locksInit()) {
            LocksInterface root = c.rootContextLockedOnThisSegment();
            this.rootContextLockedOnThisSegment = root;
            root.setNestedContextsLockedOnSameSegment(true);
            this.nestedContextsLockedOnSameSegment = true;
            this.contextModCount = root.latestSameThreadSegmentModCount();
            linkToSegmentContextsChain();
            return true;
        } else {
            return false;
        }
    }

    void closeLocks() {
        if (rootContextLockedOnThisSegment == this) {
            closeRootLocks();
        } else {
            closeNestedLocks();
        }
        deregisterIterationContextLockedInThisThread();
        localLockState = null;
        rootContextLockedOnThisSegment = null;
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
        LocksInterface innermostContextOnThisSegment = rootContextLockedOnThisSegment;
        while (true) {
            checkNestedContextsQueryDifferentKeys(innermostContextOnThisSegment);

            if (innermostContextOnThisSegment.nextNode() == null)
                break;
            innermostContextOnThisSegment = innermostContextOnThisSegment.nextNode();
        }
        innermostContextOnThisSegment.setNextNode(this);
    }

    public void checkNestedContextsQueryDifferentKeys(
            LocksInterface innermostContextOnThisSegment) {
        // TODO Spoon doesn't replace RHS instanceof occurances
        if (innermostContextOnThisSegment.getClass() == this.getClass()) {
            Data key = ((KeySearch) innermostContextOnThisSegment).inputKey;
            if (Objects.equals(key, ((KeySearch) (Object) this).inputKey)) {
                throw new IllegalStateException("Nested same-thread contexts cannot access " +
                        "the same key " + key);
            }
        }
    }

    @Stage("Locks")
    private void unlinkFromSegmentContextsChain() {
        LocksInterface prevContext = rootContextLockedOnThisSegment;
        while (true) {
            assert prevContext.nextNode() != null;
            if (prevContext.nextNode() == this)
                break;
            prevContext = prevContext.nextNode();
        }
        // i. e. structured unlocking
        verifyInnermostContext();
        prevContext.setNextNode(null);
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
        boolean isLocked = localLockState != UNLOCKED && localLockState != null;
        boolean goingToLock = newState != UNLOCKED && newState != null;
        if (isLocked) {
            if (!goingToLock)
                deregisterIterationContextLockedInThisThread();
        } else if (goingToLock) {
            registerIterationContextLockedInThisThread();
        }
        localLockState = newState;
    }

    public void checkIterationContextNotLockedInThisThread() {
        if (chaining.rootContextInThisThread.iterationContextLockedInThisThread) {
            throw new IllegalStateException("Update or Write locking is forbidden in the context" +
                    "of locked iteration context");
        }
    }

    private void registerIterationContextLockedInThisThread() {
        if (this instanceof IterationContext) {
            chaining.rootContextInThisThread.iterationContextLockedInThisThread = true;
        }
    }

    private void deregisterIterationContextLockedInThisThread() {
        if (this instanceof IterationContext) {
            chaining.rootContextInThisThread.iterationContextLockedInThisThread = false;
        }
    }

    @Stage("Locks")
    public String debugContextsAndLocks() {
        String message = "";
        message += "Contexts locked on this segment:\n";

        for (LocksInterface cxt = rootContextLockedOnThisSegment; cxt != null;
             cxt = cxt.nextNode()) {
            message += cxt.debugLocksState() + "\n";
        }
        message += "Current thread contexts:\n";
        for (int i = 0, size = chaining.contextChain.size(); i < size; i++) {
            LocksInterface cxt = chaining.contextAtIndexInChain(i);
            message += cxt.debugLocksState() + "\n";
        }
        return message;
    }

    @Override
    public String debugLocksState() {
        String s = this + ": ";
        if (!chaining.usedInit()) {
            s += "unused";
            return s;
        }
        s += "used, ";
        if (!segmentIndexInit()) {
            s += "segment uninitialized";
            return s;
        }
        s += "segment " + segmentIndex() + ", ";
        if (!locksInit()) {
            s += "locks uninitialized";
            return s;
        }
        s += "local state: " + localLockState + ", ";
        s += "read lock count: " + rootContextLockedOnThisSegment.totalReadLockCount() + ", ";
        s += "update lock count: " + rootContextLockedOnThisSegment.totalUpdateLockCount() + ", ";
        s += "write lock count: " + rootContextLockedOnThisSegment.totalWriteLockCount();
        return s;
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

    @Stage("SegmentTier") public int segmentTier = -1;
    @Stage("SegmentTier") public long tierIndex;
    @Stage("SegmentTier") public long segmentBaseAddr;

    public void initSegmentTier() {
        tierIndex = segmentIndex + 1; // tiers are 1-counted
        segmentBaseAddr = hh.h().segmentBaseAddr(segmentIndex);
        // assign the field of stage on which init() checks last, because flushed in segmentIndex()
        // initialization
        // TODO this is dangerous... review other stages and compilation mechanism
        segmentTier = 0;
    }

    private void initSegmentTier(int tier, long tierIndex) {
        segmentTier = tier;
        this.tierIndex = tierIndex;
        assert tierIndex > 0;
        this.segmentBaseAddr = hh.h().tierIndexToBaseAddr(tierIndex);
    }

    public void initSegmentTier(int tier, long tierIndex, long tierBaseAddr) {
        segmentTier = tier;
        this.tierIndex = tierIndex;
        this.segmentBaseAddr = tierBaseAddr;
    }

    public long tierCountersAreaAddr() {
        return segmentBaseAddr + hh.h().tierHashLookupOuterSize;
    }

    public long nextTierIndex() {
        return TierCountersArea.nextTierIndex(tierCountersAreaAddr());
    }

    public void nextTierIndex(long nextTierIndex) {
        TierCountersArea.nextTierIndex(tierCountersAreaAddr(), nextTierIndex);
    }

    public long nextPosToSearchFromTiered() {
        return TierCountersArea.nextPosToSearchFromTiered(tierCountersAreaAddr());
    }

    public void nextPosToSearchFromTiered(long nextPosToSearchFrom) {
        TierCountersArea.nextPosToSearchFromTiered(tierCountersAreaAddr(), nextPosToSearchFrom);
    }

    public long prevTierIndex() {
        return TierCountersArea.prevTierIndex(tierCountersAreaAddr());
    }

    public void prevTierIndex(long prevTierIndex) {
        TierCountersArea.prevTierIndex(tierCountersAreaAddr(), prevTierIndex);
    }

    public void nextTier() {
        VanillaChronicleHash<?, ?, ?, ?> h = hh.h();
        long nextTierIndex = nextTierIndex();
        if (nextTierIndex == 0) {
            nextTierIndex = h.allocateTier(segmentIndex, segmentTier + 1);
            nextTierIndex(nextTierIndex);
            long currentTierIndex = tierIndex;
            initSegmentTier(segmentTier + 1, nextTierIndex);
            prevTierIndex(currentTierIndex);
        } else {
            initSegmentTier(segmentTier + 1, nextTierIndex);
        }
    }

    public boolean hasNextTier() {
        return nextTierIndex() != 0;
    }

    public void prevTier() {
        if (segmentTier == 0)
            throw new IllegalStateException("first tier doesn't have previous");
        initSegmentTier(segmentTier - 1, prevTierIndex());
    }

    public void goToLastTier() {
        while (hasNextTier()) {
            nextTier();
        }
    }

    public void goToFirstTier() {
        while (segmentTier != 0) {
            prevTier();
        }
    }
    
    @Stage("Segment") public final PointerBytesStore segmentBS = new PointerBytesStore();
    @Stage("Segment") public final Bytes segmentBytes = new VanillaBytes(segmentBS);
    @Stage("Segment") public final ReusableBitSet freeList = new ReusableBitSet(
            new SingleThreadedFlatBitSetFrame(LONGS.align(hh.h().actualChunksPerSegment, BITS)),
            Access.nativeAccess(), null, 0);
    @Stage("Segment") long entrySpaceOffset = 0;

    boolean segmentInit() {
        return entrySpaceOffset > 0;
    }

    void initSegment() {
        VanillaChronicleHash<?, ?, ?, ?> h = hh.h();

        long segmentBaseAddr = this.segmentBaseAddr;
        segmentBS.set(segmentBaseAddr, h.tierSize);
        segmentBytes.clear();

        long freeListOffset = h.tierHashLookupOuterSize + TIER_COUNTERS_AREA_SIZE;
        freeList.setOffset(segmentBaseAddr + freeListOffset);

        entrySpaceOffset = freeListOffset + h.tierFreeListOuterSize +
                h.tierEntrySpaceInnerOffset;
    }

    @Stage("Segment")
    public Bytes segmentBytesForRead() {
        segmentBytes.readLimit(segmentBytes.capacity());
        return segmentBytes;
    }

    @Stage("Segment")
    public Bytes segmentBytesForWrite() {
        segmentBytes.readPosition(0);
        return segmentBytes;
    }
    
    void closeSegment() {
        entrySpaceOffset = 0;
    }

    public long allocReturnCode(int chunks) {
        VanillaChronicleHash<?, ?, ?, ?> h = hh.h();
        if (chunks > h.maxChunksPerEntry) {
            throw new IllegalArgumentException("Entry is too large: requires " + chunks +
                    " chucks, " + h.maxChunksPerEntry + " is maximum.");
        }
        long ret = freeList.setNextNContinuousClearBits(nextPosToSearchFrom(), chunks);
        if (ret == BitSetFrame.NOT_FOUND || ret + chunks > h.actualChunksPerSegment) {
            if (ret != BitSetFrame.NOT_FOUND &&
                    ret + chunks > h.actualChunksPerSegment && ret < h.actualChunksPerSegment) {
                freeList.clearRange(ret, h.actualChunksPerSegment);
            }
            ret = freeList.setNextNContinuousClearBits(0L, chunks);
            if (ret == BitSetFrame.NOT_FOUND || ret + chunks > h.actualChunksPerSegment) {
                if (ret != BitSetFrame.NOT_FOUND &&
                        ret + chunks > h.actualChunksPerSegment &&
                        ret < h.actualChunksPerSegment) {
                    freeList.clearRange(ret, h.actualChunksPerSegment);
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

    public void free(long fromPos, int chunks) {
        freeList.clearRange(fromPos, fromPos + chunks);
        if (fromPos < nextPosToSearchFrom())
            nextPosToSearchFrom(fromPos);
    }

    public void updateNextPosToSearchFrom(long allocated, int chunks) {
        long nextPosToSearchFrom = allocated + chunks;
        if (nextPosToSearchFrom >= hh.h().actualChunksPerSegment)
            nextPosToSearchFrom = 0L;
        nextPosToSearchFrom(nextPosToSearchFrom);
    }
}
