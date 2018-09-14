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
import net.openhft.chronicle.hash.impl.stage.hash.LogHolder;
import net.openhft.chronicle.hash.impl.stage.query.KeySearch;
import net.openhft.chronicle.hash.locks.InterProcessDeadLockException;
import net.openhft.chronicle.hash.locks.InterProcessLock;
import net.openhft.chronicle.map.impl.IterationContext;
import net.openhft.sg.Stage;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;

import static net.openhft.chronicle.algo.MemoryUnit.BITS;
import static net.openhft.chronicle.algo.MemoryUnit.LONGS;
import static net.openhft.chronicle.algo.bitset.BitSetFrame.NOT_FOUND;
import static net.openhft.chronicle.hash.impl.LocalLockState.UNLOCKED;
import static net.openhft.chronicle.hash.impl.VanillaChronicleHash.TIER_COUNTERS_AREA_SIZE;

@Staged
public abstract class SegmentStages implements SegmentLock, LocksInterface {

    @Stage("Segment")
    public final PointerBytesStore segmentBS = new PointerBytesStore();
    @Stage("Segment")
    public final Bytes segmentBytes = new VanillaBytes(segmentBS);
    @StageRef
    public VanillaChronicleHashHolder<?> hh;
    @Stage("Segment")
    public final ReusableBitSet freeList = new ReusableBitSet(
            new SingleThreadedFlatBitSetFrame(LONGS.align(hh.h().actualChunksPerSegmentTier, BITS)),
            Access.nativeAccess(), null, 0);
    @StageRef
    public CheckOnEachPublicOperation checkOnEachPublicOperation;
    public int segmentIndex = -1;
    @Stage("SegmentHeader")
    public long segmentHeaderAddress;
    @Stage("SegmentHeader")
    public SegmentHeader segmentHeader = null;
    @Stage("Locks")
    public LocksInterface rootContextLockedOnThisSegment = null;
    /**
     * See the ChMap Ops spec, considerations of nested same-thread concurrent contexts.
     * Once context enters the segment, and observes concurrent same-thread context,
     * it sets nestedContextsLockedOnSameSegment = true for itself and that concurrent context.
     * This flag is not dropped on exit of one of these contexts, because between calls of
     * this context, nested one could be initialized, does some changes that break our thread-local
     * assumptions _and exit_, that is why on exit concurrent context should remain "dirty".
     */
    @Stage("Locks")
    public boolean nestedContextsLockedOnSameSegment;
    @Stage("Locks")
    public int latestSameThreadSegmentModCount;
    @Stage("Locks")
    public int contextModCount;
    @StageRef
    public ReadLock innerReadLock;
    @StageRef
    public UpdateLock innerUpdateLock;
    @StageRef
    public WriteLock innerWriteLock;
    @Stage("SegmentTier")
    public int tier = -1;
    @Stage("SegmentTier")
    public long tierIndex;
    @Stage("SegmentTier")
    public long tierBaseAddr;
    @Stage("Segment")
    public long entrySpaceOffset = 0;
    @StageRef
    LogHolder log;
    @StageRef
    Chaining chaining;
    // chain
    @Stage("Locks")
    LocksInterface nextNode;
    @Stage("Locks")
    LocalLockState localLockState;
    @Stage("Locks")
    int totalReadLockCount;
    @Stage("Locks")
    int totalUpdateLockCount;
    @Stage("Locks")
    int totalWriteLockCount;

    public void initSegmentIndex(int segmentIndex) {
        this.segmentIndex = segmentIndex;
    }

    public abstract boolean segmentIndexInit();

    private void initSegmentHeader() {
        segmentHeaderAddress = hh.h().segmentHeaderAddress(segmentIndex);
        segmentHeader = BigSegmentHeader.INSTANCE;
    }

    public long tierEntries() {
        if (tier == 0) {
            return segmentHeader.entries(segmentHeaderAddress);
        } else {
            return TierCountersArea.entries(tierCountersAreaAddr());
        }
    }

    public void tierEntries(long tierEntries) {
        if (tier == 0) {
            segmentHeader.entries(segmentHeaderAddress, tierEntries);
        } else {
            TierCountersArea.entries(tierCountersAreaAddr(), tierEntries);
        }
    }

    public long lowestPossiblyFreeChunk() {
        if (tier == 0) {
            return segmentHeader.lowestPossiblyFreeChunk(segmentHeaderAddress);
        } else {
            return TierCountersArea.lowestPossiblyFreeChunkTiered(tierCountersAreaAddr());
        }
    }

    public void lowestPossiblyFreeChunk(long lowestPossiblyFreeChunk) {
        if (tier == 0) {
            segmentHeader.lowestPossiblyFreeChunk(segmentHeaderAddress, lowestPossiblyFreeChunk);
        } else {
            TierCountersArea.lowestPossiblyFreeChunkTiered(tierCountersAreaAddr(),
                    lowestPossiblyFreeChunk);
        }
    }

    public long tierDeleted() {
        if (tier == 0) {
            return segmentHeader.deleted(segmentHeaderAddress);
        } else {
            return TierCountersArea.deleted(tierCountersAreaAddr());
        }
    }

    public void tierDeleted(long tierDeleted) {
        if (tier == 0) {
            segmentHeader.deleted(segmentHeaderAddress, tierDeleted);
        } else {
            TierCountersArea.deleted(tierCountersAreaAddr(), tierDeleted);
        }
    }

    public long nextTierIndex() {
        if (tier == 0) {
            return segmentHeader.nextTierIndex(segmentHeaderAddress);
        } else {
            return TierCountersArea.nextTierIndex(tierCountersAreaAddr());
        }
    }

    public void nextTierIndex(long nextTierIndex) {
        if (tier == 0) {
            segmentHeader.nextTierIndex(segmentHeaderAddress, nextTierIndex);
        } else {
            TierCountersArea.nextTierIndex(tierCountersAreaAddr(), nextTierIndex);
        }
    }

    public long size() {
        goToFirstTier();
        long size = tierEntries() - tierDeleted();
        while (hasNextTier()) {
            nextTier();
            size += tierEntries() - tierDeleted();
        }
        return size;
    }

    @Override
    @Stage("Locks")
    public void setNestedContextsLockedOnSameSegment(boolean nestedContextsLockedOnSameSegment) {
        this.nestedContextsLockedOnSameSegment = nestedContextsLockedOnSameSegment;
    }

    @Override
    @Stage("Locks")
    public int changeAndGetLatestSameThreadSegmentModCount(int change) {
        return this.latestSameThreadSegmentModCount += change;
    }

    @Stage("Locks")
    public void incrementModCount() {
        contextModCount =
                rootContextLockedOnThisSegment.changeAndGetLatestSameThreadSegmentModCount(1);
    }

    @Override
    @Stage("Locks")
    public void setNextNode(LocksInterface nextNode) {
        this.nextNode = nextNode;
    }

    @Stage("Locks")
    public boolean readZero() {
        return rootContextLockedOnThisSegment.totalReadLockCount() == 0;
    }

    @Override
    @Stage("Locks")
    public int changeAndGetTotalReadLockCount(int change) {
        assert totalReadLockCount + change >= 0 : "read underflow";
        return totalReadLockCount += change;
    }

    @Stage("Locks")
    public boolean updateZero() {
        return rootContextLockedOnThisSegment.totalUpdateLockCount() == 0;
    }

    @Override
    @Stage("Locks")
    public int changeAndGetTotalUpdateLockCount(int change) {
        assert totalUpdateLockCount + change >= 0 : "update underflow";
        return totalUpdateLockCount += change;
    }

    @Stage("Locks")
    public boolean writeZero() {
        return rootContextLockedOnThisSegment.totalWriteLockCount() == 0;
    }

    @Override
    @Stage("Locks")
    public int changeAndGetTotalWriteLockCount(int change) {
        assert totalWriteLockCount + change >= 0 : "write underflow";
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
        // This is a dummy check needed to make the Locks stage dependent on the chaining.Used
        // stage, to move the chaining.Used stage up in topological order, to make it closed later
        // in the global context close() method, to ensure the context is unlocked (this is done in
        // the chaining.closeUsed() method) after all other stages, potentially accessing Map's
        // off-heap memory, are closed.
        assert chaining.used;
        // Ensure SegmentHeader is init. This method initLocks() doesn't trigger SegmentHeader
        // initialization otherwise, so it could remain uninit on the beginning of locking methods.
        // But Locks still anyway depends on SegmentHeader stage (at least via
        // readUnlockAndDecrementCount() method), so when on later stages of locking methods
        // SegmentHeader is init, Locks stage is closed (and then need to be re-init again), as
        // a dependency of SegmentHeader. So ensuring SegmentHeader is always init before Locks
        // allows to avoid redundant work.
        if (segmentHeader == null)
            throw new AssertionError();
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
                if (decrementRead() == 0) {
                    if (updateZero() && writeZero())
                        segmentHeader.readUnlock(segmentHeaderAddress);
                }
                return;
            case UPDATE_LOCKED:
                if (decrementUpdate() == 0) {
                    if (writeZero()) {
                        if (readZero()) {
                            segmentHeader.updateUnlock(segmentHeaderAddress);
                        } else {
                            segmentHeader.downgradeUpdateToReadLock(segmentHeaderAddress);
                        }
                    }
                }
                return;
            case WRITE_LOCKED:
                if (decrementWrite() == 0) {
                    if (!updateZero()) {
                        segmentHeader.downgradeWriteToUpdateLock(segmentHeaderAddress);
                    } else {
                        if (!readZero()) {
                            segmentHeader.downgradeWriteToReadLock(segmentHeaderAddress);
                        } else {
                            segmentHeader.writeUnlock(segmentHeaderAddress);
                        }
                    }
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
        // TODO Spoon doesn't replace RHS instanceof occurrences
        if (innermostContextOnThisSegment.getClass() == this.getClass()) {
            Data key = ((KeySearch) innermostContextOnThisSegment).inputKey;
            if (Objects.equals(key, ((KeySearch) (Object) this).inputKey)) {
                throw new IllegalStateException(hh.h().toIdentityString() +
                        ": Nested same-thread contexts cannot access the same key " + key);
            }
        }
    }

    @Stage("Locks")
    private void unlinkFromSegmentContextsChain() {
        LocksInterface prevContext = rootContextLockedOnThisSegment;
        while (true) {
            LocksInterface nextNode = prevContext.nextNode();
            // nextNode could be null, if this context failed initLocks() in
            // checkNestedContextsQueryDifferentKeys()
            if (nextNode == this || nextNode == null)
                break;
            prevContext = nextNode;
        }
        // i. e. structured unlocking
        verifyInnermostContext();
        prevContext.setNextNode(null);
    }

    @Stage("Locks")
    private void verifyInnermostContext() {
        if (nextNode != null) {
            throw new IllegalStateException(
                    hh.h().toIdentityString() + ": Attempt to close contexts not structurally");
        }
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
            throw new IllegalStateException(hh.h().toIdentityString() + ": Update or Write " +
                    "locking is forbidden in the context of locked iteration context");
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
    public RuntimeException debugContextsAndLocks(InterProcessDeadLockException e) {
        String message = hh.h().toIdentityString() + ":\n";
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
        throw new InterProcessDeadLockException(message, e);
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

    public abstract boolean segmentTierInit();

    public void initSegmentTier() {
        tierIndex = segmentIndex + 1; // tiers are 1-counted
        tierBaseAddr = hh.h().segmentBaseAddr(segmentIndex);
        // assign the field of stage on which init() checks last, because flushed in segmentIndex()
        // initialization
        // TODO this is dangerous... review other stages and compilation mechanism
        tier = 0;
    }

    public void initSegmentTier(int tier, long tierIndex) {
        this.tier = tier;
        this.tierIndex = tierIndex;
        assert tierIndex > 0;
        this.tierBaseAddr = hh.h().tierIndexToBaseAddr(tierIndex);
    }

    public void initSegmentTier(int tier, long tierIndex, long tierBaseAddr) {
        this.tier = tier;
        this.tierIndex = tierIndex;
        this.tierBaseAddr = tierBaseAddr;
    }

    public long tierCountersAreaAddr() {
        return tierBaseAddr + hh.h().tierHashLookupOuterSize;
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
            log.LOG.debug("Allocate tier for segment # {}, tier {}", segmentIndex, tier + 1);
            nextTierIndex = h.allocateTier();
            nextTierIndex(nextTierIndex);
            long prevTierIndex = tierIndex;

            initSegmentTier(tier + 1, nextTierIndex);

            TierCountersArea.segmentIndex(tierCountersAreaAddr(), segmentIndex);
            TierCountersArea.tier(tierCountersAreaAddr(), tier);
            nextTierIndex(0);
            prevTierIndex(prevTierIndex);
        } else {
            initSegmentTier(tier + 1, nextTierIndex);
        }
    }

    public boolean hasNextTier() {
        return nextTierIndex() != 0;
    }

    public void prevTier() {
        if (tier == 0) {
            throw new IllegalStateException(
                    hh.h().toIdentityString() + ": first tier doesn't have previous");
        }
        initSegmentTier(tier - 1, prevTierIndex());
    }

    public void goToLastTier() {
        while (hasNextTier()) {
            nextTier();
        }
    }

    public void goToFirstTier() {
        while (tier != 0) {
            prevTier();
        }
    }

    boolean segmentInit() {
        return entrySpaceOffset > 0;
    }

    void initSegment() {
        VanillaChronicleHash<?, ?, ?, ?> h = hh.h();

        long segmentBaseAddr = this.tierBaseAddr;
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

    @Stage("Segment")
    public long allocReturnCode(int chunks) {
        VanillaChronicleHash<?, ?, ?, ?> h = hh.h();
        if (chunks > h.maxChunksPerEntry) {
            throw new IllegalArgumentException(hh.h().toIdentityString() +
                    ": Entry is too large: requires " + chunks +
                    " chunks, " + h.maxChunksPerEntry + " is maximum.");
        }
        long lowestPossiblyFreeChunk = lowestPossiblyFreeChunk();
        if (lowestPossiblyFreeChunk + chunks > h.actualChunksPerSegmentTier)
            return -1;
        if (tierEntries() >= h.maxEntriesPerHashLookup)
            return -1;
        assert lowestPossiblyFreeChunk < h.actualChunksPerSegmentTier;
        long ret = freeList.setNextNContinuousClearBits(lowestPossiblyFreeChunk, chunks);
        if (ret == NOT_FOUND || ret + chunks > h.actualChunksPerSegmentTier) {
            if (ret + chunks > h.actualChunksPerSegmentTier) {
                assert ret != NOT_FOUND;
                freeList.clearRange(ret, ret + chunks);
            }
            return -1;
        } else {
            tierEntries(tierEntries() + 1);
            // if bit at lowestPossiblyFreeChunk is clear, it was skipped because
            // more than 1 chunk was requested. Don't move lowestPossiblyFreeChunk
            // in this case. chunks == 1 clause is just a fast path.
            if (chunks == 1 || freeList.isSet(lowestPossiblyFreeChunk)) {
                lowestPossiblyFreeChunk(ret + chunks);
            }
            return ret;
        }
    }

    @Stage("Segment")
    public boolean realloc(long fromPos, int oldChunks, int newChunks) {
        if (fromPos + newChunks < hh.h().actualChunksPerSegmentTier &&
                freeList.isRangeClear(fromPos + oldChunks, fromPos + newChunks)) {
            freeList.setRange(fromPos + oldChunks, fromPos + newChunks);
            // checking and updating lowestPossiblyFreeChunk is omitted because adds computational
            // complexity for seemingly very small gain
            return true;
        } else {
            return false;
        }
    }

    @Stage("Segment")
    public void free(long fromPos, int chunks) {
        tierEntries(tierEntries() - 1);
        freeList.clearRange(fromPos, fromPos + chunks);
        if (fromPos < lowestPossiblyFreeChunk())
            lowestPossiblyFreeChunk(fromPos);
    }

    @Stage("Segment")
    public void freeExtra(long pos, int oldChunks, int newChunks) {
        long from = pos + newChunks;
        freeList.clearRange(from, pos + oldChunks);
        if (from < lowestPossiblyFreeChunk())
            lowestPossiblyFreeChunk(from);
    }

    public void verifyTierCountersAreaData() {
        goToFirstTier();
        while (true) {
            int tierSegmentIndex = TierCountersArea.segmentIndex(tierCountersAreaAddr());
            if (tierSegmentIndex != segmentIndex) {
                throw new AssertionError("segmentIndex: " + segmentIndex +
                        ", tier: " + tier + ", tierIndex: " + tierIndex + ", tierBaseAddr: " +
                        tierBaseAddr + " reports it belongs to segmentIndex " + tierSegmentIndex);
            }
            if (hasNextTier()) {
                long currentTierIndex = this.tierIndex;
                nextTier();
                if (prevTierIndex() != currentTierIndex) {
                    throw new AssertionError("segmentIndex: " + segmentIndex +
                            ", tier: " + tier + ", tierIndex: " + tierIndex + ", tierBaseAddr: " +
                            tierBaseAddr + " reports the previous tierIndex is " + prevTierIndex() +
                            " while actually it is " + currentTierIndex);
                }
            } else {
                break;
            }
        }
    }
}
