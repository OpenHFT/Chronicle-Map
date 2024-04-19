package net.openhft.chronicle.map.impl;

import net.openhft.chronicle.algo.MemoryUnit;
import net.openhft.chronicle.algo.bitset.BitSetFrame;
import net.openhft.chronicle.algo.bitset.ReusableBitSet;
import net.openhft.chronicle.algo.bitset.SingleThreadedFlatBitSetFrame;
import net.openhft.chronicle.algo.bytes.Access;
import net.openhft.chronicle.algo.hashing.LongHashFunction;
import net.openhft.chronicle.bytes.*;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.hash.*;
import net.openhft.chronicle.hash.impl.*;
import net.openhft.chronicle.hash.impl.stage.entry.*;
import net.openhft.chronicle.hash.impl.stage.hash.ChainingInterface;
import net.openhft.chronicle.hash.impl.util.Objects;
import net.openhft.chronicle.hash.locks.InterProcessDeadLockException;
import net.openhft.chronicle.hash.locks.InterProcessLock;
import net.openhft.chronicle.hash.serialization.DataAccess;
import net.openhft.chronicle.hash.serialization.SizedReader;
import net.openhft.chronicle.map.*;
import net.openhft.chronicle.set.ChronicleSet;
import net.openhft.chronicle.set.SetContext;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static net.openhft.chronicle.hash.impl.LocalLockState.UNLOCKED;

/**
 * Generated code
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class CompiledMapIterationContext<K, V, R> extends ChainingInterface implements AutoCloseable , ChecksumEntry , HashEntry<K> , HashSegmentContext<K, MapEntry<K, V>> , SegmentLock , Alloc , KeyHashCode , LocksInterface , MapContext<K, V, R> , MapEntry<K, V> , IterationContext<K, V, R> , VanillaChronicleMapHolder<K, V, R> , SetContext<K, R> {
    public boolean readZeroGuarded() {
        if (!(this.locksInit()))
            this.initLocks();
        
        return readZero();
    }

    public boolean reallocGuarded(long fromPos, int oldChunks, int newChunks) {
        if (!(this.segmentInit()))
            this.initSegment();
        
        return realloc(fromPos, oldChunks, newChunks);
    }

    public boolean updateZeroGuarded() {
        if (!(this.locksInit()))
            this.initLocks();
        
        return updateZero();
    }

    public boolean writeZeroGuarded() {
        if (!(this.locksInit()))
            this.initLocks();
        
        return writeZero();
    }

    public int decrementUpdateGuarded() {
        if (!(this.locksInit()))
            this.initLocks();
        
        return decrementUpdate();
    }

    public int decrementWriteGuarded() {
        if (!(this.locksInit()))
            this.initLocks();
        
        return decrementWrite();
    }

    public RuntimeException debugContextsAndLocksGuarded(InterProcessDeadLockException e) {
        if (!(this.locksInit()))
            this.initLocks();
        
        return debugContextsAndLocks(e);
    }

    public long allocReturnCodeGuarded(int chunks) {
        if (!(this.segmentInit()))
            this.initSegment();
        
        return allocReturnCode(chunks);
    }

    public Bytes segmentBytesForReadGuarded() {
        if (!(this.segmentInit()))
            this.initSegment();
        
        return segmentBytesForRead();
    }

    public Bytes segmentBytesForWriteGuarded() {
        if (!(this.segmentInit()))
            this.initSegment();
        
        return segmentBytesForWrite();
    }

    public void close() {
        this.doCloseDelayedUpdateChecksum();
        this.doCloseKeySearch();
        this.doCloseHashLookupPos();
        this.doCloseLocks();
        this.doCloseUsed();
        this.entryKey.doCloseCachedEntryKey();
        this.entryValue.doCloseCachedEntryValue();
        this.doCloseEntrySizeInChunks();
        this.doCloseEntryOffset();
        this.doCloseValueSize();
        this.doCloseSegment();
        this.doCloseKeyHash();
        this.doCloseSegmentTier();
        this.doCloseSegmentHeader();
        this.doCloseSearchKey();
        this.wrappedValueInstanceDataHolder.doCloseWrappedData();
        this.wrappedValueInstanceDataHolder.doCloseValue();
        this.doCloseMap();
        this.doCloseAllocatedChunks();
        this.doCloseValueSizeOffset();
        this.doCloseKeyOffset();
        this.wrappedValueInstanceDataHolder.doCloseNext();
        this.doClosePos();
        this.wrappedValueBytesData.doCloseCachedWrappedValue();
        this.wrappedValueBytesData.doCloseWrappedValueBytes();
        this.wrappedValueBytesData.doCloseWrappedValueBytesStore();
        this.wrappedValueBytesData.doCloseNext();
        this.doCloseInputKey();
        this.doCloseEntryRemovedOnThisIteration();
        this.doCloseSegmentIndex();
        this.doCloseHashLookupEntry();
        this.doCloseKeySize();
    }

    public void doCloseAllocatedChunks() {
        this.allocatedChunks = 0;
    }

    public void doCloseDelayedUpdateChecksum() {
        if (!(this.delayedUpdateChecksumInit()))
            return ;
        
        if (this.h().checksumEntries)
            this.hashEntryChecksumStrategy.computeAndStoreChecksum();
        
        delayedUpdateChecksum = false;
    }

    public void doCloseEntryOffset() {
        this.keySizeOffset = -1;
    }

    public void doCloseEntryRemovedOnThisIteration() {
        this.entryRemovedOnThisIteration = false;
    }

    public void doCloseEntrySizeInChunks() {
        this.entrySizeInChunks = 0;
    }

    public void doCloseHashLookupEntry() {
        this.hashLookupEntry = 0;
    }

    public void doCloseHashLookupPos() {
        this.hashLookupPos = -1;
    }

    public void doCloseInputKey() {
        this.inputKey = null;
    }

    public void doCloseKeyHash() {
        this.keyHash = 0;
    }

    public void doCloseKeyOffset() {
        this.keyOffset = -1;
    }

    public void doCloseKeySearch() {
        this.searchState = null;
    }

    public void doCloseKeySize() {
        this.keySize = -1;
    }

    public void doCloseLocks() {
        if (!(this.locksInit()))
            return ;
        
        if ((rootContextLockedOnThisSegment) == (this)) {
            closeRootLocks();
        } else {
            closeNestedLocks();
        }
        deregisterIterationContextLockedInThisThread();
        localLockState = null;
        rootContextLockedOnThisSegment = null;
    }

    public void doCloseMap() {
        this.m = null;
    }

    public void doClosePos() {
        this.pos = -1;
    }

    public void doCloseSearchKey() {
        this.searchKey = CompactOffHeapLinearHashTable.UNSET_KEY;
    }

    public void doCloseSegment() {
        if (!(this.segmentInit()))
            return ;
        
        entrySpaceOffset = 0;
    }

    public void doCloseSegmentHeader() {
        this.segmentHeader = null;
    }

    public void doCloseSegmentIndex() {
        this.segmentIndex = -1;
    }

    public void doCloseSegmentTier() {
        this.tier = -1;
    }

    public void doCloseUsed() {
        if (!(this.usedInit()))
            return ;
        
        used = false;
        if (firstContextLockedInThisThread)
            rootContextInThisThread.unlockContextLocally();
        
    }

    public void doCloseValueSize() {
        this.valueSize = -1;
    }

    public void doCloseValueSizeOffset() {
        this.valueSizeOffset = -1;
    }

    public void freeExtraGuarded(long pos, int oldChunks, int newChunks) {
        if (!(this.segmentInit()))
            this.initSegment();
        
        freeExtra(pos, oldChunks, newChunks);
    }

    public void freeGuarded(long fromPos, int chunks) {
        if (!(this.segmentInit()))
            this.initSegment();
        
        free(fromPos, chunks);
    }

    public void incrementModCountGuarded() {
        if (!(this.locksInit()))
            this.initLocks();
        
        incrementModCount();
    }

    public void incrementReadGuarded() {
        if (!(this.locksInit()))
            this.initLocks();
        
        incrementRead();
    }

    public void incrementUpdateGuarded() {
        if (!(this.locksInit()))
            this.initLocks();
        
        incrementUpdate();
    }

    public void incrementWriteGuarded() {
        if (!(this.locksInit()))
            this.initLocks();
        
        incrementWrite();
    }

    public void readUnlockAndDecrementCountGuarded() {
        if (!(this.locksInit()))
            this.initLocks();
        
        readUnlockAndDecrementCount();
    }

    public void setHashLookupPosGuarded(long hashLookupPos) {
        if (!(this.hashLookupPosInit()))
            this.initHashLookupPos();
        
        setHashLookupPos(hashLookupPos);
    }

    public void setLocalLockStateGuarded(LocalLockState newState) {
        if (!(this.locksInit()))
            this.initLocks();
        
        setLocalLockState(newState);
    }

    public CompiledMapIterationContext(ChainingInterface rootContextInThisThread ,VanillaChronicleMap map) {
        contextChain = rootContextInThisThread.getContextChain();
        indexInContextChain = contextChain.size();
        contextChain.add(this);
        this.rootContextInThisThread = rootContextInThisThread;
        initMap(map);
        this.entryValue = new EntryValueBytesData();
        this.valueReader = net.openhft.chronicle.hash.serialization.StatefulCopyable.copyIfNeeded(this.m().valueReader);
        this.wrappedValueInstanceDataHolder = new WrappedValueInstanceDataHolder();
        this.innerReadLock = new ReadLock();
        this.keyReader = net.openhft.chronicle.hash.serialization.StatefulCopyable.copyIfNeeded(this.h().keyReader);
        this.owner = Thread.currentThread();
        this.innerWriteLock = new WriteLock();
        this.wrappedValueBytesData = new WrappedValueBytesData();
        this.entryKey = new EntryKeyBytesData();
        this.segmentBS = new PointerBytesStore();
        this.segmentBytes = CompiledMapIterationContext.unmonitoredVanillaBytes(segmentBS);
        this.hashEntryChecksumStrategy = new HashEntryChecksumStrategy();
        this.checksumStrategy = this.h().checksumEntries ? this.hashEntryChecksumStrategy : NoChecksumStrategy.INSTANCE;
        this.freeList = new ReusableBitSet(new SingleThreadedFlatBitSetFrame(MemoryUnit.LONGS.align(this.h().actualChunksPerSegmentTier, MemoryUnit.BITS)) , Access.nativeAccess() , null , 0);
        this.innerUpdateLock = new UpdateLock();
    }

    public CompiledMapIterationContext(VanillaChronicleMap map) {
        contextChain = new ArrayList<>();
        contextChain.add(this);
        indexInContextChain = 0;
        rootContextInThisThread = this;
        initMap(map);
        this.entryValue = new EntryValueBytesData();
        this.valueReader = net.openhft.chronicle.hash.serialization.StatefulCopyable.copyIfNeeded(this.m().valueReader);
        this.wrappedValueInstanceDataHolder = new WrappedValueInstanceDataHolder();
        this.innerReadLock = new ReadLock();
        this.keyReader = net.openhft.chronicle.hash.serialization.StatefulCopyable.copyIfNeeded(this.h().keyReader);
        this.owner = Thread.currentThread();
        this.innerWriteLock = new WriteLock();
        this.wrappedValueBytesData = new WrappedValueBytesData();
        this.entryKey = new EntryKeyBytesData();
        this.segmentBS = new PointerBytesStore();
        this.segmentBytes = CompiledMapIterationContext.unmonitoredVanillaBytes(segmentBS);
        this.hashEntryChecksumStrategy = new HashEntryChecksumStrategy();
        this.checksumStrategy = this.h().checksumEntries ? this.hashEntryChecksumStrategy : NoChecksumStrategy.INSTANCE;
        this.freeList = new ReusableBitSet(new SingleThreadedFlatBitSetFrame(MemoryUnit.LONGS.align(this.h().actualChunksPerSegmentTier, MemoryUnit.BITS)) , Access.nativeAccess() , null , 0);
        this.innerUpdateLock = new UpdateLock();
    }

    boolean tryFindInitLocksOfThisSegment(int index) {
        LocksInterface c = this.contextAtIndexInChain(index);
        if (((c.segmentHeaderInit()) && ((c.segmentHeaderAddress()) == (segmentHeaderAddress()))) && (c.locksInit())) {
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

    public class EntryKeyBytesData extends AbstractData<K> {
        public void doCloseCachedEntryKey() {
            this.cachedEntryKeyRead = false;
        }

        @Override
        public RandomDataInput bytes() {
            CompiledMapIterationContext.this.checkOnEachPublicOperation();
            return CompiledMapIterationContext.this.segmentBS();
        }

        @Override
        public long offset() {
            CompiledMapIterationContext.this.checkOnEachPublicOperation();
            return CompiledMapIterationContext.this.keyOffset();
        }

        @Override
        public long hash(LongHashFunction f) {
            CompiledMapIterationContext.this.checkOnEachPublicOperation();
            return super.hash(f);
        }

        @Override
        public long size() {
            CompiledMapIterationContext.this.checkOnEachPublicOperation();
            return CompiledMapIterationContext.this.keySize();
        }

        public void closeEntryKeyBytesDataSizeDependants() {
            this.closeEntryKeyBytesDataInnerGetUsingDependants();
        }

        private K innerGetUsing(K usingKey) {
            Bytes bytes = CompiledMapIterationContext.this.segmentBytesForReadGuarded();
            bytes.readPosition(CompiledMapIterationContext.this.keyOffset());
            return CompiledMapIterationContext.this.keyReader.read(bytes, size(), usingKey);
        }

        public void closeEntryKeyBytesDataInnerGetUsingDependants() {
            this.closeCachedEntryKey();
        }

        private K cachedEntryKey;

        private boolean cachedEntryKeyRead = false;

        public boolean cachedEntryKeyInit() {
            return this.cachedEntryKeyRead;
        }

        private void initCachedEntryKey() {
            cachedEntryKey = innerGetUsing(cachedEntryKey);
            cachedEntryKeyRead = true;
        }

        public K cachedEntryKey() {
            if (!(this.cachedEntryKeyInit()))
                this.initCachedEntryKey();
            
            return this.cachedEntryKey;
        }

        public void closeCachedEntryKey() {
            this.cachedEntryKeyRead = false;
        }

        @Override
        public K get() {
            CompiledMapIterationContext.this.checkOnEachPublicOperation();
            return cachedEntryKey();
        }

        @Override
        public K getUsing(K using) {
            CompiledMapIterationContext.this.checkOnEachPublicOperation();
            return innerGetUsing(using);
        }
    }

    public class EntryValueBytesData extends AbstractData<V> {
        public void doCloseCachedEntryValue() {
            if (!(this.cachedEntryValueInit()))
                return ;
            
            cachedEntryValueRead = false;
        }

        @Override
        public long offset() {
            CompiledMapIterationContext.this.checkOnEachPublicOperation();
            return CompiledMapIterationContext.this.valueOffset();
        }

        @Override
        public long size() {
            CompiledMapIterationContext.this.checkOnEachPublicOperation();
            return CompiledMapIterationContext.this.valueSize();
        }

        public void closeEntryValueBytesDataSizeDependants() {
            this.closeEntryValueBytesDataInnerGetUsingDependants();
        }

        private V innerGetUsing(V usingValue) {
            Bytes segmentBytes = CompiledMapIterationContext.this.segmentBytesForReadGuarded();
            segmentBytes.readPosition(CompiledMapIterationContext.this.valueOffset());
            return CompiledMapIterationContext.this.valueReader.read(segmentBytes, size(), usingValue);
        }

        public void closeEntryValueBytesDataInnerGetUsingDependants() {
            this.closeCachedEntryValue();
        }

        @Override
        public V getUsing(V using) {
            CompiledMapIterationContext.this.checkOnEachPublicOperation();
            return innerGetUsing(using);
        }

        private V cachedEntryValue = (CompiledMapIterationContext.this.m().valueType()) == (CharSequence.class) ? ((V)(new StringBuilder())) : null;

        private boolean cachedEntryValueRead = false;

        public boolean cachedEntryValueInit() {
            return cachedEntryValueRead;
        }

        private void initCachedEntryValue() {
            cachedEntryValue = innerGetUsing(cachedEntryValue);
            cachedEntryValueRead = true;
        }

        public V cachedEntryValue() {
            if (!(this.cachedEntryValueInit()))
                this.initCachedEntryValue();
            
            return this.cachedEntryValue;
        }

        public void closeCachedEntryValue() {
            if (!(this.cachedEntryValueInit()))
                return ;
            
            cachedEntryValueRead = false;
        }

        @Override
        public V get() {
            CompiledMapIterationContext.this.checkOnEachPublicOperation();
            return cachedEntryValue();
        }

        @Override
        public RandomDataInput bytes() {
            CompiledMapIterationContext.this.checkOnEachPublicOperation();
            return CompiledMapIterationContext.this.segmentBS();
        }
    }

    public class HashEntryChecksumStrategy implements ChecksumStrategy {
        @Override
        public long extraEntryBytes() {
            return ChecksumStrategy.CHECKSUM_STORED_BYTES;
        }

        @Override
        public int computeChecksum() {
            long keyHashCode = CompiledMapIterationContext.this.keyHashCode();
            long keyEnd = CompiledMapIterationContext.this.keyEnd();
            long len = (CompiledMapIterationContext.this.entryEnd()) - keyEnd;
            long checksum;
            if (len > 0) {
                long addr = (CompiledMapIterationContext.this.tierBaseAddr()) + keyEnd;
                long payloadChecksum = LongHashFunction.xx_r39().hashMemory(addr, len);
                checksum = net.openhft.chronicle.hash.impl.stage.entry.ChecksumHashing.hash8To16Bytes(CompiledMapIterationContext.this.keySize(), keyHashCode, payloadChecksum);
            } else {
                checksum = keyHashCode;
            }
            return ((int)((checksum >>> 32) ^ checksum));
        }

        public void closeHashEntryChecksumStrategyComputeChecksumDependants() {
            this.closeHashEntryChecksumStrategyComputeAndStoreChecksumDependants();
        }

        @Override
        public void computeAndStoreChecksum() {
            int checksum = computeChecksum();
            CompiledMapIterationContext.this.segmentBS().writeInt(CompiledMapIterationContext.this.entryEnd(), checksum);
        }

        public void closeHashEntryChecksumStrategyComputeAndStoreChecksumDependants() {
            CompiledMapIterationContext.this.closeDelayedUpdateChecksum();
        }

        @Override
        public int storedChecksum() {
            return CompiledMapIterationContext.this.segmentBS().readInt(CompiledMapIterationContext.this.entryEnd());
        }

        @Override
        public boolean innerCheckSum() {
            int oldChecksum = storedChecksum();
            int checksum = computeChecksum();
            return oldChecksum == checksum;
        }
    }

    public class ReadLock implements InterProcessLock {
        @Override
        public void lockInterruptibly() throws InterruptedException {
            CompiledMapIterationContext.this.checkOnEachLockOperation();
            if (Thread.interrupted())
                throw new InterruptedException();
            
            if ((CompiledMapIterationContext.this.localLockState()) == (LocalLockState.UNLOCKED)) {
                if (((CompiledMapIterationContext.this.readZeroGuarded()) && (CompiledMapIterationContext.this.updateZeroGuarded())) && (CompiledMapIterationContext.this.writeZeroGuarded())) {
                    try {
                        CompiledMapIterationContext.this.segmentHeader().readLockInterruptibly(CompiledMapIterationContext.this.segmentHeaderAddress());
                    } catch (InterProcessDeadLockException e) {
                        throw CompiledMapIterationContext.this.debugContextsAndLocksGuarded(e);
                    }
                } 
                CompiledMapIterationContext.this.incrementReadGuarded();
                CompiledMapIterationContext.this.setLocalLockStateGuarded(LocalLockState.READ_LOCKED);
            } 
        }

        @Override
        public void lock() {
            CompiledMapIterationContext.this.checkOnEachLockOperation();
            if ((CompiledMapIterationContext.this.localLockState()) == (LocalLockState.UNLOCKED)) {
                if (((CompiledMapIterationContext.this.readZeroGuarded()) && (CompiledMapIterationContext.this.updateZeroGuarded())) && (CompiledMapIterationContext.this.writeZeroGuarded())) {
                    try {
                        CompiledMapIterationContext.this.segmentHeader().readLock(CompiledMapIterationContext.this.segmentHeaderAddress());
                    } catch (InterProcessDeadLockException e) {
                        throw CompiledMapIterationContext.this.debugContextsAndLocksGuarded(e);
                    }
                } 
                CompiledMapIterationContext.this.incrementReadGuarded();
                CompiledMapIterationContext.this.setLocalLockStateGuarded(LocalLockState.READ_LOCKED);
            } 
        }

        public void closeReadLockLockDependants() {
            CompiledMapIterationContext.this.closeHashLookupPos();
        }

        @Override
        public boolean isHeldByCurrentThread() {
            CompiledMapIterationContext.this.checkOnEachLockOperation();
            return CompiledMapIterationContext.this.localLockState().read;
        }

        @Override
        public void unlock() {
            CompiledMapIterationContext.this.checkOnEachLockOperation();
            if ((CompiledMapIterationContext.this.localLockState()) != (LocalLockState.UNLOCKED)) {
                CompiledMapIterationContext.this.closeHashLookupPos();
                CompiledMapIterationContext.this.closeEntry();
            } 
            CompiledMapIterationContext.this.readUnlockAndDecrementCountGuarded();
            CompiledMapIterationContext.this.setLocalLockStateGuarded(LocalLockState.UNLOCKED);
        }

        @Override
        public boolean tryLock() {
            CompiledMapIterationContext.this.checkOnEachLockOperation();
            if ((CompiledMapIterationContext.this.localLockState()) == (LocalLockState.UNLOCKED)) {
                if ((((!(CompiledMapIterationContext.this.readZeroGuarded())) || (!(CompiledMapIterationContext.this.updateZeroGuarded()))) || (!(CompiledMapIterationContext.this.writeZeroGuarded()))) || (CompiledMapIterationContext.this.segmentHeader().tryReadLock(CompiledMapIterationContext.this.segmentHeaderAddress()))) {
                    CompiledMapIterationContext.this.incrementReadGuarded();
                    CompiledMapIterationContext.this.setLocalLockStateGuarded(LocalLockState.READ_LOCKED);
                    return true;
                } else {
                    return false;
                }
            } else {
                return true;
            }
        }

        @Override
        public boolean tryLock(long time, @NotNull
        TimeUnit unit) throws InterruptedException {
            CompiledMapIterationContext.this.checkOnEachLockOperation();
            if (Thread.interrupted())
                throw new InterruptedException();
            
            if ((CompiledMapIterationContext.this.localLockState()) == (LocalLockState.UNLOCKED)) {
                if ((((!(CompiledMapIterationContext.this.readZeroGuarded())) || (!(CompiledMapIterationContext.this.updateZeroGuarded()))) || (!(CompiledMapIterationContext.this.writeZeroGuarded()))) || (CompiledMapIterationContext.this.segmentHeader().tryReadLock(CompiledMapIterationContext.this.segmentHeaderAddress(), time, unit))) {
                    CompiledMapIterationContext.this.incrementReadGuarded();
                    CompiledMapIterationContext.this.setLocalLockStateGuarded(LocalLockState.READ_LOCKED);
                    return true;
                } else {
                    return false;
                }
            } else {
                return true;
            }
        }

        @Override
        public boolean isHeld() {
            return CompiledMapIterationContext.this.m != null &&
                    CompiledMapIterationContext.this.localLockState != null &&
                    CompiledMapIterationContext.this.localLockState != UNLOCKED;
        }
    }

    public class UpdateLock implements InterProcessLock {
        @NotNull
        private IllegalStateException forbiddenUpdateLockWhenOuterContextReadLocked() {
            return new IllegalStateException((((CompiledMapIterationContext.this.h().toIdentityString()) + ": Cannot acquire update lock, because outer context holds read lock. ") + "In this case you should acquire update lock in the outer context up front"));
        }

        @NotNull
        private IllegalMonitorStateException forbiddenUpgrade() {
            return new IllegalMonitorStateException(((CompiledMapIterationContext.this.h().toIdentityString()) + ": Cannot upgrade from read to update lock"));
        }

        @Override
        public void lockInterruptibly() throws InterruptedException {
            CompiledMapIterationContext.this.checkOnEachLockOperation();
            if (Thread.interrupted())
                throw new InterruptedException();
            
            switch (CompiledMapIterationContext.this.localLockState()) {
                case UNLOCKED :
                    CompiledMapIterationContext.this.checkIterationContextNotLockedInThisThread();
                    if ((CompiledMapIterationContext.this.updateZeroGuarded()) && (CompiledMapIterationContext.this.writeZeroGuarded())) {
                        if (!(CompiledMapIterationContext.this.readZeroGuarded()))
                            throw forbiddenUpdateLockWhenOuterContextReadLocked();
                        
                        try {
                            CompiledMapIterationContext.this.segmentHeader().updateLockInterruptibly(CompiledMapIterationContext.this.segmentHeaderAddress());
                        } catch (InterProcessDeadLockException e) {
                            throw CompiledMapIterationContext.this.debugContextsAndLocksGuarded(e);
                        }
                    } 
                    CompiledMapIterationContext.this.incrementUpdateGuarded();
                    CompiledMapIterationContext.this.setLocalLockStateGuarded(LocalLockState.UPDATE_LOCKED);
                    return ;
                case READ_LOCKED :
                    throw forbiddenUpgrade();
                case UPDATE_LOCKED :
                case WRITE_LOCKED :
            }
        }

        @Override
        public boolean isHeldByCurrentThread() {
            CompiledMapIterationContext.this.checkOnEachLockOperation();
            return CompiledMapIterationContext.this.localLockState().update;
        }

        @Override
        public void unlock() {
            CompiledMapIterationContext.this.checkOnEachLockOperation();
            switch (CompiledMapIterationContext.this.localLockState()) {
                case UNLOCKED :
                case READ_LOCKED :
                    return ;
                case UPDATE_LOCKED :
                    CompiledMapIterationContext.this.closeDelayedUpdateChecksum();
                    if (((CompiledMapIterationContext.this.decrementUpdateGuarded()) == 0) && (CompiledMapIterationContext.this.writeZeroGuarded())) {
                        CompiledMapIterationContext.this.segmentHeader().downgradeUpdateToReadLock(CompiledMapIterationContext.this.segmentHeaderAddress());
                    } 
                    break;
                case WRITE_LOCKED :
                    CompiledMapIterationContext.this.closeDelayedUpdateChecksum();
                    if ((CompiledMapIterationContext.this.decrementWriteGuarded()) == 0) {
                        if (!(CompiledMapIterationContext.this.updateZeroGuarded())) {
                            CompiledMapIterationContext.this.segmentHeader().downgradeWriteToUpdateLock(CompiledMapIterationContext.this.segmentHeaderAddress());
                        } else {
                            CompiledMapIterationContext.this.segmentHeader().downgradeWriteToReadLock(CompiledMapIterationContext.this.segmentHeaderAddress());
                        }
                    } 
            }
            CompiledMapIterationContext.this.incrementReadGuarded();
            CompiledMapIterationContext.this.setLocalLockStateGuarded(LocalLockState.READ_LOCKED);
        }

        @Override
        public void lock() {
            CompiledMapIterationContext.this.checkOnEachLockOperation();
            switch (CompiledMapIterationContext.this.localLockState()) {
                case UNLOCKED :
                    CompiledMapIterationContext.this.checkIterationContextNotLockedInThisThread();
                    if ((CompiledMapIterationContext.this.updateZeroGuarded()) && (CompiledMapIterationContext.this.writeZeroGuarded())) {
                        if (!(CompiledMapIterationContext.this.readZeroGuarded()))
                            throw forbiddenUpdateLockWhenOuterContextReadLocked();
                        
                        try {
                            CompiledMapIterationContext.this.segmentHeader().updateLock(CompiledMapIterationContext.this.segmentHeaderAddress());
                        } catch (InterProcessDeadLockException e) {
                            throw CompiledMapIterationContext.this.debugContextsAndLocksGuarded(e);
                        }
                    } 
                    CompiledMapIterationContext.this.incrementUpdateGuarded();
                    CompiledMapIterationContext.this.setLocalLockStateGuarded(LocalLockState.UPDATE_LOCKED);
                    return ;
                case READ_LOCKED :
                    throw forbiddenUpgrade();
                case UPDATE_LOCKED :
                case WRITE_LOCKED :
            }
        }

        @Override
        public boolean tryLock() {
            CompiledMapIterationContext.this.checkOnEachLockOperation();
            switch (CompiledMapIterationContext.this.localLockState()) {
                case UNLOCKED :
                    CompiledMapIterationContext.this.checkIterationContextNotLockedInThisThread();
                    if ((CompiledMapIterationContext.this.updateZeroGuarded()) && (CompiledMapIterationContext.this.writeZeroGuarded())) {
                        if (!(CompiledMapIterationContext.this.readZeroGuarded()))
                            throw forbiddenUpdateLockWhenOuterContextReadLocked();
                        
                        if (CompiledMapIterationContext.this.segmentHeader().tryUpdateLock(CompiledMapIterationContext.this.segmentHeaderAddress())) {
                            CompiledMapIterationContext.this.incrementUpdateGuarded();
                            CompiledMapIterationContext.this.setLocalLockStateGuarded(LocalLockState.UPDATE_LOCKED);
                            return true;
                        } else {
                            return false;
                        }
                    } else {
                        CompiledMapIterationContext.this.incrementUpdateGuarded();
                        CompiledMapIterationContext.this.setLocalLockStateGuarded(LocalLockState.UPDATE_LOCKED);
                        return true;
                    }
                case READ_LOCKED :
                    throw forbiddenUpgrade();
                case UPDATE_LOCKED :
                case WRITE_LOCKED :
                    return true;
                default :
                    throw new IllegalStateException((((CompiledMapIterationContext.this.h().toIdentityString()) + ": unexpected localLockState=") + (CompiledMapIterationContext.this.localLockState())));
            }
        }

        @Override
        public boolean tryLock(long time, @NotNull
        TimeUnit unit) throws InterruptedException {
            CompiledMapIterationContext.this.checkOnEachLockOperation();
            if (Thread.interrupted())
                throw new InterruptedException();
            
            switch (CompiledMapIterationContext.this.localLockState()) {
                case UNLOCKED :
                    CompiledMapIterationContext.this.checkIterationContextNotLockedInThisThread();
                    if ((CompiledMapIterationContext.this.updateZeroGuarded()) && (CompiledMapIterationContext.this.writeZeroGuarded())) {
                        if (!(CompiledMapIterationContext.this.readZeroGuarded()))
                            throw forbiddenUpdateLockWhenOuterContextReadLocked();
                        
                        if (CompiledMapIterationContext.this.segmentHeader().tryUpdateLock(CompiledMapIterationContext.this.segmentHeaderAddress(), time, unit)) {
                            CompiledMapIterationContext.this.incrementUpdateGuarded();
                            CompiledMapIterationContext.this.setLocalLockStateGuarded(LocalLockState.UPDATE_LOCKED);
                            return true;
                        } else {
                            return false;
                        }
                    } else {
                        CompiledMapIterationContext.this.incrementUpdateGuarded();
                        CompiledMapIterationContext.this.setLocalLockStateGuarded(LocalLockState.UPDATE_LOCKED);
                        return true;
                    }
                case READ_LOCKED :
                    throw forbiddenUpgrade();
                case UPDATE_LOCKED :
                case WRITE_LOCKED :
                    return true;
                default :
                    throw new IllegalStateException((((CompiledMapIterationContext.this.h().toIdentityString()) + ": unexpected localLockState=") + (CompiledMapIterationContext.this.localLockState())));
            }
        }

        @Override
        public boolean isHeld() {
            return CompiledMapIterationContext.this.m != null &&
                    CompiledMapIterationContext.this.localLockState != null &&
                    CompiledMapIterationContext.this.localLockState != UNLOCKED;
        }
    }

    public class WrappedValueBytesData extends AbstractData<V> {
        public void doCloseCachedWrappedValue() {
            this.cachedWrappedValueRead = false;
        }

        public void doCloseNext() {
            if (!(this.nextInit()))
                return ;
            
        }

        public void doCloseWrappedValueBytes() {
            if (!(this.wrappedValueBytesInit()))
                return ;

            wrappedValueBytes.bytesStore(BytesStore.empty(), 0, 0);
            wrappedValueBytesUsed = false;
        }

        public void doCloseWrappedValueBytesStore() {
            if (!(this.wrappedValueBytesStoreInit()))
                return ;
            
            wrappedValueBytesStore = null;
            if ((next) != null)
                next.closeWrappedValueBytesStore();
            
        }

        public WrappedValueBytesData getUnusedWrappedValueBytesDataGuarded() {
            assert this.nextInit() : "Next should be init";
            return getUnusedWrappedValueBytesData();
        }

        public WrappedValueBytesData() {
            this.wrappedValueBytes = VanillaBytes.vanillaBytes();
        }

        private final VanillaBytes wrappedValueBytes;

        public WrappedValueBytesData getUnusedWrappedValueBytesData() {
            if (!(wrappedValueBytesStoreInit()))
                return this;
            
            if ((next) == null)
                next = new WrappedValueBytesData();
            
            return next.getUnusedWrappedValueBytesData();
        }

        private WrappedValueBytesData next;

        boolean nextInit() {
            return true;
        }

        public WrappedValueBytesData next() {
            assert this.nextInit() : "Next should be init";
            return this.next;
        }

        void closeNext() {
            if (!(this.nextInit()))
                return ;
            
            this.closeNextDependants();
        }

        public void closeNextDependants() {
            this.closeWrappedValueBytesStore();
        }

        private long wrappedValueBytesOffset;

        private long wrappedValueBytesSize;

        private BytesStore<?, ?> wrappedValueBytesStore;

        boolean wrappedValueBytesStoreInit() {
            return (wrappedValueBytesStore) != null;
        }

        public void initWrappedValueBytesStore(BytesStore<?, ?> bytesStore, long offset, long size) {
            boolean wasWrappedValueBytesStoreInit = this.wrappedValueBytesStoreInit();
            wrappedValueBytesStore = bytesStore;
            wrappedValueBytesOffset = offset;
            wrappedValueBytesSize = size;
            if (wasWrappedValueBytesStoreInit)
                this.closeWrappedValueBytesStoreDependants();
            
        }

        public long wrappedValueBytesSize() {
            assert this.wrappedValueBytesStoreInit() : "WrappedValueBytesStore should be init";
            return this.wrappedValueBytesSize;
        }

        public long wrappedValueBytesOffset() {
            assert this.wrappedValueBytesStoreInit() : "WrappedValueBytesStore should be init";
            return this.wrappedValueBytesOffset;
        }

        public BytesStore<?, ?> wrappedValueBytesStore() {
            assert this.wrappedValueBytesStoreInit() : "WrappedValueBytesStore should be init";
            return this.wrappedValueBytesStore;
        }

        void closeWrappedValueBytesStore() {
            if (!(this.wrappedValueBytesStoreInit()))
                return ;
            
            this.closeWrappedValueBytesStoreDependants();
            wrappedValueBytesStore = null;
            if ((next()) != null)
                next().closeWrappedValueBytesStore();
            
        }

        public void closeWrappedValueBytesStoreDependants() {
            this.closeWrappedValueBytes();
            this.closeWrappedValueBytesDataInnerGetUsingDependants();
        }

        private boolean wrappedValueBytesUsed = false;

        boolean wrappedValueBytesInit() {
            return wrappedValueBytesUsed;
        }

        void initWrappedValueBytes() {
            boolean wasWrappedValueBytesInit = this.wrappedValueBytesInit();
            wrappedValueBytes.bytesStore(wrappedValueBytesStore(), wrappedValueBytesOffset(), wrappedValueBytesSize());
            wrappedValueBytesUsed = true;
            if (wasWrappedValueBytesInit)
                this.closeWrappedValueBytesDependants();
            
        }

        public VanillaBytes wrappedValueBytes() {
            if (!(this.wrappedValueBytesInit()))
                this.initWrappedValueBytes();
            
            return this.wrappedValueBytes;
        }

        void closeWrappedValueBytes() {
            if (!(this.wrappedValueBytesInit()))
                return ;
            
            this.closeWrappedValueBytesDependants();
            wrappedValueBytes.bytesStore(BytesStore.empty(), 0, 0);
            wrappedValueBytesUsed = false;
        }

        public void closeWrappedValueBytesDependants() {
            this.closeWrappedValueBytesDataInnerGetUsingDependants();
        }

        private V innerGetUsing(V usingValue) {
            wrappedValueBytes().readPosition(wrappedValueBytesOffset());
            return CompiledMapIterationContext.this.valueReader.read(wrappedValueBytes(), wrappedValueBytesSize(), usingValue);
        }

        public void closeWrappedValueBytesDataInnerGetUsingDependants() {
            this.closeCachedWrappedValue();
        }

        private V cachedWrappedValue;

        private boolean cachedWrappedValueRead = false;

        public boolean cachedWrappedValueInit() {
            return this.cachedWrappedValueRead;
        }

        private void initCachedWrappedValue() {
            cachedWrappedValue = innerGetUsing(cachedWrappedValue);
            cachedWrappedValueRead = true;
        }

        public V cachedWrappedValue() {
            if (!(this.cachedWrappedValueInit()))
                this.initCachedWrappedValue();
            
            return this.cachedWrappedValue;
        }

        public void closeCachedWrappedValue() {
            this.cachedWrappedValueRead = false;
        }

        @Override
        public V get() {
            CompiledMapIterationContext.this.checkOnEachPublicOperation();
            return cachedWrappedValue();
        }

        @Override
        public long size() {
            CompiledMapIterationContext.this.checkOnEachPublicOperation();
            return wrappedValueBytesSize();
        }

        @Override
        public long offset() {
            CompiledMapIterationContext.this.checkOnEachPublicOperation();
            return wrappedValueBytesOffset();
        }

        @Override
        public V getUsing(V using) {
            CompiledMapIterationContext.this.checkOnEachPublicOperation();
            return innerGetUsing(using);
        }

        @Override
        public RandomDataInput bytes() {
            CompiledMapIterationContext.this.checkOnEachPublicOperation();
            return wrappedValueBytes().bytesStore();
        }
    }

    public class WrappedValueInstanceDataHolder {
        public void doCloseValue() {
            if (!(this.valueInit()))
                return ;
            
            value = null;
            if ((next) != null)
                next.closeValue();
            
        }

        public void doCloseNext() {
            if (!(this.nextInit()))
                return ;
            
        }

        public void doCloseWrappedData() {
            if (!(this.wrappedDataInit()))
                return ;
            
            wrappedData = null;
            wrappedValueDataAccess.uninit();
        }

        public WrappedValueInstanceDataHolder getUnusedWrappedValueHolderGuarded() {
            assert this.nextInit() : "Next should be init";
            return getUnusedWrappedValueHolder();
        }

        public WrappedValueInstanceDataHolder() {
            this.wrappedValueDataAccess = CompiledMapIterationContext.this.m().valueDataAccess.copy();
        }

        private final DataAccess<V> wrappedValueDataAccess;

        public DataAccess<V> wrappedValueDataAccess() {
            return this.wrappedValueDataAccess;
        }

        public WrappedValueInstanceDataHolder getUnusedWrappedValueHolder() {
            if (!(valueInit()))
                return this;
            
            if ((next) == null)
                next = new WrappedValueInstanceDataHolder();
            
            return next.getUnusedWrappedValueHolder();
        }

        private WrappedValueInstanceDataHolder next;

        boolean nextInit() {
            return true;
        }

        public WrappedValueInstanceDataHolder next() {
            assert this.nextInit() : "Next should be init";
            return this.next;
        }

        void closeNext() {
            if (!(this.nextInit()))
                return ;
            
            this.closeNextDependants();
        }

        public void closeNextDependants() {
            this.closeValue();
        }

        private V value;

        public boolean valueInit() {
            return (value) != null;
        }

        public void initValue(V value) {
            boolean wasValueInit = this.valueInit();
            CompiledMapIterationContext.this.m().checkValue(value);
            this.value = value;
            if (wasValueInit)
                this.closeValueDependants();
            
        }

        public V value() {
            assert this.valueInit() : "Value should be init";
            return this.value;
        }

        public void closeValue() {
            if (!(this.valueInit()))
                return ;
            
            this.closeValueDependants();
            value = null;
            if ((next()) != null)
                next().closeValue();
            
        }

        public void closeValueDependants() {
            this.closeWrappedData();
        }

        public Data<V> wrappedData = null;

        public boolean wrappedDataInit() {
            return (this.wrappedData) != null;
        }

        private void initWrappedData() {
            wrappedData = wrappedValueDataAccess.getData(value());
        }

        public Data<V> wrappedData() {
            if (!(this.wrappedDataInit()))
                this.initWrappedData();
            
            return this.wrappedData;
        }

        private void closeWrappedData() {
            if (!(this.wrappedDataInit()))
                return ;
            
            wrappedData = null;
            wrappedValueDataAccess.uninit();
        }
    }

    public class WriteLock implements InterProcessLock {
        @NotNull
        private IllegalMonitorStateException forbiddenUpgrade() {
            return new IllegalMonitorStateException(((CompiledMapIterationContext.this.h().toIdentityString()) + ": Cannot upgrade from read to write lock"));
        }

        @NotNull
        private IllegalStateException forbiddenWriteLockWhenOuterContextReadLocked() {
            return new IllegalStateException((((CompiledMapIterationContext.this.h().toIdentityString()) + ": Cannot acquire write lock, because outer context holds read lock. ") + "In this case you should acquire update lock in the outer context up front"));
        }

        @Override
        public boolean tryLock() {
            CompiledMapIterationContext.this.checkOnEachLockOperation();
            switch (CompiledMapIterationContext.this.localLockState()) {
                case UNLOCKED :
                    CompiledMapIterationContext.this.checkIterationContextNotLockedInThisThread();
                    if (CompiledMapIterationContext.this.writeZeroGuarded()) {
                        if (!(CompiledMapIterationContext.this.updateZeroGuarded())) {
                            if (CompiledMapIterationContext.this.segmentHeader().tryUpgradeUpdateToWriteLock(CompiledMapIterationContext.this.segmentHeaderAddress())) {
                                CompiledMapIterationContext.this.incrementWriteGuarded();
                                CompiledMapIterationContext.this.setLocalLockStateGuarded(LocalLockState.WRITE_LOCKED);
                                return true;
                            } else {
                                return false;
                            }
                        } else {
                            if (!(CompiledMapIterationContext.this.readZeroGuarded()))
                                throw forbiddenWriteLockWhenOuterContextReadLocked();
                            
                            if (CompiledMapIterationContext.this.segmentHeader().tryWriteLock(CompiledMapIterationContext.this.segmentHeaderAddress())) {
                                CompiledMapIterationContext.this.incrementWriteGuarded();
                                CompiledMapIterationContext.this.setLocalLockStateGuarded(LocalLockState.WRITE_LOCKED);
                                return true;
                            } else {
                                return false;
                            }
                        }
                    } else {
                        CompiledMapIterationContext.this.incrementWriteGuarded();
                        CompiledMapIterationContext.this.setLocalLockStateGuarded(LocalLockState.WRITE_LOCKED);
                        return true;
                    }
                case READ_LOCKED :
                    throw forbiddenUpgrade();
                case UPDATE_LOCKED :
                    if (CompiledMapIterationContext.this.writeZeroGuarded()) {
                        assert !(CompiledMapIterationContext.this.updateZeroGuarded());
                        if (CompiledMapIterationContext.this.segmentHeader().tryUpgradeUpdateToWriteLock(CompiledMapIterationContext.this.segmentHeaderAddress())) {
                            CompiledMapIterationContext.this.decrementUpdateGuarded();
                            CompiledMapIterationContext.this.incrementWriteGuarded();
                            CompiledMapIterationContext.this.setLocalLockStateGuarded(LocalLockState.WRITE_LOCKED);
                            return true;
                        } else {
                            return false;
                        }
                    } else {
                        CompiledMapIterationContext.this.decrementUpdateGuarded();
                        CompiledMapIterationContext.this.incrementWriteGuarded();
                        CompiledMapIterationContext.this.setLocalLockStateGuarded(LocalLockState.WRITE_LOCKED);
                        return true;
                    }
                case WRITE_LOCKED :
                    return true;
                default :
                    throw new IllegalStateException((((CompiledMapIterationContext.this.h().toIdentityString()) + ": unexpected localLockState=") + (CompiledMapIterationContext.this.localLockState())));
            }
        }

        @Override
        public boolean tryLock(long time, @NotNull
        TimeUnit unit) throws InterruptedException {
            CompiledMapIterationContext.this.checkOnEachLockOperation();
            if (Thread.interrupted())
                throw new InterruptedException();
            
            switch (CompiledMapIterationContext.this.localLockState()) {
                case UNLOCKED :
                    CompiledMapIterationContext.this.checkIterationContextNotLockedInThisThread();
                    if (CompiledMapIterationContext.this.writeZeroGuarded()) {
                        if (!(CompiledMapIterationContext.this.updateZeroGuarded())) {
                            if (CompiledMapIterationContext.this.segmentHeader().tryUpgradeUpdateToWriteLock(CompiledMapIterationContext.this.segmentHeaderAddress(), time, unit)) {
                                CompiledMapIterationContext.this.incrementWriteGuarded();
                                CompiledMapIterationContext.this.setLocalLockStateGuarded(LocalLockState.WRITE_LOCKED);
                                return true;
                            } else {
                                return false;
                            }
                        } else {
                            if (!(CompiledMapIterationContext.this.readZeroGuarded()))
                                throw forbiddenWriteLockWhenOuterContextReadLocked();
                            
                            if (CompiledMapIterationContext.this.segmentHeader().tryWriteLock(CompiledMapIterationContext.this.segmentHeaderAddress(), time, unit)) {
                                CompiledMapIterationContext.this.incrementWriteGuarded();
                                CompiledMapIterationContext.this.setLocalLockStateGuarded(LocalLockState.WRITE_LOCKED);
                                return true;
                            } else {
                                return false;
                            }
                        }
                    } else {
                        CompiledMapIterationContext.this.incrementWriteGuarded();
                        CompiledMapIterationContext.this.setLocalLockStateGuarded(LocalLockState.WRITE_LOCKED);
                        return true;
                    }
                case READ_LOCKED :
                    throw forbiddenUpgrade();
                case UPDATE_LOCKED :
                    if (CompiledMapIterationContext.this.writeZeroGuarded()) {
                        assert !(CompiledMapIterationContext.this.updateZeroGuarded());
                        if (CompiledMapIterationContext.this.segmentHeader().tryUpgradeUpdateToWriteLock(CompiledMapIterationContext.this.segmentHeaderAddress(), time, unit)) {
                            CompiledMapIterationContext.this.decrementUpdateGuarded();
                            CompiledMapIterationContext.this.incrementWriteGuarded();
                            CompiledMapIterationContext.this.setLocalLockStateGuarded(LocalLockState.WRITE_LOCKED);
                            return true;
                        } else {
                            return false;
                        }
                    } else {
                        CompiledMapIterationContext.this.decrementUpdateGuarded();
                        CompiledMapIterationContext.this.incrementWriteGuarded();
                        CompiledMapIterationContext.this.setLocalLockStateGuarded(LocalLockState.WRITE_LOCKED);
                        return true;
                    }
                case WRITE_LOCKED :
                    return true;
                default :
                    throw new IllegalStateException((((CompiledMapIterationContext.this.h().toIdentityString()) + ": unexpected localLockState=") + (CompiledMapIterationContext.this.localLockState())));
            }
        }

        @Override
        public void lockInterruptibly() throws InterruptedException {
            CompiledMapIterationContext.this.checkOnEachLockOperation();
            if (Thread.interrupted())
                throw new InterruptedException();
            
            switch (CompiledMapIterationContext.this.localLockState()) {
                case UNLOCKED :
                    CompiledMapIterationContext.this.checkIterationContextNotLockedInThisThread();
                    if (CompiledMapIterationContext.this.writeZeroGuarded()) {
                        if (!(CompiledMapIterationContext.this.updateZeroGuarded())) {
                            CompiledMapIterationContext.this.segmentHeader().upgradeUpdateToWriteLockInterruptibly(CompiledMapIterationContext.this.segmentHeaderAddress());
                        } else {
                            if (!(CompiledMapIterationContext.this.readZeroGuarded()))
                                throw forbiddenWriteLockWhenOuterContextReadLocked();
                            
                            try {
                                CompiledMapIterationContext.this.segmentHeader().writeLockInterruptibly(CompiledMapIterationContext.this.segmentHeaderAddress());
                            } catch (InterProcessDeadLockException e) {
                                throw CompiledMapIterationContext.this.debugContextsAndLocksGuarded(e);
                            }
                        }
                    } 
                    CompiledMapIterationContext.this.incrementWriteGuarded();
                    CompiledMapIterationContext.this.setLocalLockStateGuarded(LocalLockState.WRITE_LOCKED);
                    return ;
                case READ_LOCKED :
                    throw forbiddenUpgrade();
                case UPDATE_LOCKED :
                    if (CompiledMapIterationContext.this.writeZeroGuarded()) {
                        assert !(CompiledMapIterationContext.this.updateZeroGuarded());
                        try {
                            CompiledMapIterationContext.this.segmentHeader().upgradeUpdateToWriteLockInterruptibly(CompiledMapIterationContext.this.segmentHeaderAddress());
                        } catch (InterProcessDeadLockException e) {
                            throw CompiledMapIterationContext.this.debugContextsAndLocksGuarded(e);
                        }
                    } 
                    CompiledMapIterationContext.this.decrementUpdateGuarded();
                    CompiledMapIterationContext.this.incrementWriteGuarded();
                    CompiledMapIterationContext.this.setLocalLockStateGuarded(LocalLockState.WRITE_LOCKED);
                    break;
                case WRITE_LOCKED :
                    break;
            }
        }

        @Override
        public void unlock() {
            CompiledMapIterationContext.this.checkOnEachLockOperation();
            switch (CompiledMapIterationContext.this.localLockState()) {
                case UNLOCKED :
                case READ_LOCKED :
                case UPDATE_LOCKED :
                    return ;
                case WRITE_LOCKED :
                    CompiledMapIterationContext.this.closeDelayedUpdateChecksum();
                    if ((CompiledMapIterationContext.this.decrementWriteGuarded()) == 0)
                        CompiledMapIterationContext.this.segmentHeader().downgradeWriteToUpdateLock(CompiledMapIterationContext.this.segmentHeaderAddress());
                    
                    CompiledMapIterationContext.this.incrementUpdateGuarded();
                    CompiledMapIterationContext.this.setLocalLockStateGuarded(LocalLockState.UPDATE_LOCKED);
            }
        }

        @Override
        public void lock() {
            CompiledMapIterationContext.this.checkOnEachLockOperation();
            switch (CompiledMapIterationContext.this.localLockState()) {
                case UNLOCKED :
                    CompiledMapIterationContext.this.checkIterationContextNotLockedInThisThread();
                    if (CompiledMapIterationContext.this.writeZeroGuarded()) {
                        if (!(CompiledMapIterationContext.this.updateZeroGuarded())) {
                            CompiledMapIterationContext.this.segmentHeader().upgradeUpdateToWriteLock(CompiledMapIterationContext.this.segmentHeaderAddress());
                        } else {
                            if (!(CompiledMapIterationContext.this.readZeroGuarded()))
                                throw forbiddenWriteLockWhenOuterContextReadLocked();
                            
                            try {
                                CompiledMapIterationContext.this.segmentHeader().writeLock(CompiledMapIterationContext.this.segmentHeaderAddress());
                            } catch (InterProcessDeadLockException e) {
                                throw CompiledMapIterationContext.this.debugContextsAndLocksGuarded(e);
                            }
                        }
                    } 
                    CompiledMapIterationContext.this.incrementWriteGuarded();
                    CompiledMapIterationContext.this.setLocalLockStateGuarded(LocalLockState.WRITE_LOCKED);
                    return ;
                case READ_LOCKED :
                    throw forbiddenUpgrade();
                case UPDATE_LOCKED :
                    if (CompiledMapIterationContext.this.writeZeroGuarded()) {
                        assert !(CompiledMapIterationContext.this.updateZeroGuarded());
                        try {
                            CompiledMapIterationContext.this.segmentHeader().upgradeUpdateToWriteLock(CompiledMapIterationContext.this.segmentHeaderAddress());
                        } catch (InterProcessDeadLockException e) {
                            throw CompiledMapIterationContext.this.debugContextsAndLocksGuarded(e);
                        }
                    } 
                    CompiledMapIterationContext.this.decrementUpdateGuarded();
                    CompiledMapIterationContext.this.incrementWriteGuarded();
                    CompiledMapIterationContext.this.setLocalLockStateGuarded(LocalLockState.WRITE_LOCKED);
                    break;
                case WRITE_LOCKED :
                    break;
            }
        }

        @Override
        public boolean isHeldByCurrentThread() {
            CompiledMapIterationContext.this.checkOnEachLockOperation();
            return CompiledMapIterationContext.this.localLockState().write;
        }

        @Override
        public boolean isHeld() {
            return CompiledMapIterationContext.this.m != null &&
                    CompiledMapIterationContext.this.localLockState != null &&
                    CompiledMapIterationContext.this.localLockState != UNLOCKED;
        }
    }

    @Override
    public int changeAndGetLatestSameThreadSegmentModCount(int change) {
        return this.latestSameThreadSegmentModCount += change;
    }

    @Override
    public int changeAndGetTotalReadLockCount(int change) {
        assert ((totalReadLockCount) + change) >= 0 : "read underflow";
        return totalReadLockCount += change;
    }

    @Override
    public int changeAndGetTotalUpdateLockCount(int change) {
        assert ((totalUpdateLockCount) + change) >= 0 : "update underflow";
        return totalUpdateLockCount += change;
    }

    @Override
    public int changeAndGetTotalWriteLockCount(int change) {
        assert ((totalWriteLockCount) + change) >= 0 : "write underflow";
        return totalWriteLockCount += change;
    }

    public int decrementRead() {
        return rootContextLockedOnThisSegment.changeAndGetTotalReadLockCount(-1);
    }

    public int decrementUpdate() {
        return rootContextLockedOnThisSegment.changeAndGetTotalUpdateLockCount(-1);
    }

    public int decrementWrite() {
        return rootContextLockedOnThisSegment.changeAndGetTotalWriteLockCount(-1);
    }

    public enum SearchState {
PRESENT, ABSENT;    }

    private long _HashEntryStages_entryEnd() {
        return keyEnd();
    }

    public long allocReturnCode(int chunks) {
        VanillaChronicleHash<K, ?, ?, ?> h = this.h();
        if (chunks > (h.maxChunksPerEntry)) {
            throw new IllegalArgumentException(((((((this.h().toIdentityString()) + ": Entry is too large: requires ") + chunks) + " chunks, ") + (h.maxChunksPerEntry)) + " is maximum."));
        } 
        long lowestPossiblyFreeChunk = lowestPossiblyFreeChunk();
        if ((lowestPossiblyFreeChunk + chunks) > (h.actualChunksPerSegmentTier))
            return -1;
        
        if ((tierEntries()) >= (h.maxEntriesPerHashLookup))
            return -1;
        
        assert lowestPossiblyFreeChunk < (h.actualChunksPerSegmentTier);
        long ret = freeList.setNextNContinuousClearBits(lowestPossiblyFreeChunk, chunks);
        if ((ret == (BitSetFrame.NOT_FOUND)) || ((ret + chunks) > (h.actualChunksPerSegmentTier))) {
            if ((ret + chunks) > (h.actualChunksPerSegmentTier)) {
                assert ret != (BitSetFrame.NOT_FOUND);
                freeList.clearRange(ret, (ret + chunks));
            } 
            return -1;
        } else {
            tierEntries(((tierEntries()) + 1));
            if ((chunks == 1) || (freeList.isSet(lowestPossiblyFreeChunk))) {
                lowestPossiblyFreeChunk((ret + chunks));
            } 
            return ret;
        }
    }

    private void _CheckOnEachPublicOperation_checkOnEachPublicOperation() {
        checkOnEachLockOperation();
    }

    private void _HashSegmentIteration_hookAfterEachIteration() {
        throwExceptionIfClosed();
    }

    private void _SegmentStages_checkNestedContextsQueryDifferentKeys(LocksInterface innermostContextOnThisSegment) {
        if ((innermostContextOnThisSegment.getClass()) == (getClass())) {
            Data key = ((CompiledMapIterationContext)(innermostContextOnThisSegment)).inputKey();
            if (java.util.Objects.equals(key, ((CompiledMapIterationContext)((Object)(this))).inputKey())) {
                throw new IllegalStateException((((this.h().toIdentityString()) + ": Nested same-thread contexts cannot access the same key ") + key));
            } 
        } 
    }

    private void _SegmentStages_nextTier() {
        VanillaChronicleHash<K, ?, ?, ?> h = this.h();
        long nextTierIndex = nextTierIndex();
        if (nextTierIndex == 0) {
            Jvm.debug().on(getClass(), ((("Allocate tier for segment #  " + (segmentIndex())) + " tier ") + ((tier()) + 1)));
            nextTierIndex = h.allocateTier();
            nextTierIndex(nextTierIndex);
            long prevTierIndex = tierIndex();
            initSegmentTier(((tier()) + 1), nextTierIndex);
            TierCountersArea.segmentIndex(tierCountersAreaAddr(), segmentIndex());
            TierCountersArea.tier(tierCountersAreaAddr(), tier());
            nextTierIndex(0);
            prevTierIndex(prevTierIndex);
        } else {
            initSegmentTier(((tier()) + 1), nextTierIndex);
        }
    }

    public void free(long fromPos, int chunks) {
        tierEntries(((tierEntries()) - 1));
        freeList.clearRange(fromPos, (fromPos + chunks));
        if (fromPos < (lowestPossiblyFreeChunk()))
            lowestPossiblyFreeChunk(fromPos);
        
    }

    public void freeExtra(long pos, int oldChunks, int newChunks) {
        long from = pos + newChunks;
        freeList.clearRange(from, (pos + oldChunks));
        if (from < (lowestPossiblyFreeChunk()))
            lowestPossiblyFreeChunk(from);
        
    }

    public void incrementModCount() {
        contextModCount = rootContextLockedOnThisSegment.changeAndGetLatestSameThreadSegmentModCount(1);
    }

    public void incrementRead() {
        rootContextLockedOnThisSegment.changeAndGetTotalReadLockCount(1);
    }

    public void incrementUpdate() {
        rootContextLockedOnThisSegment.changeAndGetTotalUpdateLockCount(1);
    }

    public void incrementWrite() {
        rootContextLockedOnThisSegment.changeAndGetTotalWriteLockCount(1);
    }

    public void readUnlockAndDecrementCount() {
        switch (localLockState) {
            case UNLOCKED :
                return ;
            case READ_LOCKED :
                if ((decrementRead()) == 0) {
                    if ((updateZero()) && (writeZero()))
                        segmentHeader().readUnlock(segmentHeaderAddress());
                    
                } 
                return ;
            case UPDATE_LOCKED :
                if ((decrementUpdate()) == 0) {
                    if (writeZero()) {
                        if (readZero()) {
                            segmentHeader().updateUnlock(segmentHeaderAddress());
                        } else {
                            segmentHeader().downgradeUpdateToReadLock(segmentHeaderAddress());
                        }
                    } 
                } 
                return ;
            case WRITE_LOCKED :
                if ((decrementWrite()) == 0) {
                    if (!(updateZero())) {
                        segmentHeader().downgradeWriteToUpdateLock(segmentHeaderAddress());
                    } else {
                        if (!(readZero())) {
                            segmentHeader().downgradeWriteToReadLock(segmentHeaderAddress());
                        } else {
                            segmentHeader().writeUnlock(segmentHeaderAddress());
                        }
                    }
                } 
        }
    }

    public void setHashLookupPos(long hashLookupPos) {
        this.hashLookupPos = hashLookupPos;
    }

    public void setLocalLockState(LocalLockState newState) {
        boolean isLocked = ((localLockState) != (LocalLockState.UNLOCKED)) && ((localLockState) != null);
        boolean goingToLock = (newState != (LocalLockState.UNLOCKED)) && (newState != null);
        if (isLocked) {
            if (!goingToLock)
                deregisterIterationContextLockedInThisThread();
            
        } else if (goingToLock) {
            registerIterationContextLockedInThisThread();
        } 
        localLockState = newState;
    }

    @Override
    public void setNestedContextsLockedOnSameSegment(boolean nestedContextsLockedOnSameSegment) {
        this.nestedContextsLockedOnSameSegment = nestedContextsLockedOnSameSegment;
    }

    @Override
    public void setNextNode(LocksInterface nextNode) {
        this.nextNode = nextNode;
    }

    public void setSearchState(CompiledMapIterationContext.SearchState newSearchState) {
        this.searchState = newSearchState;
    }

    final Thread owner;

    public Thread owner() {
        return this.owner;
    }

    public Bytes segmentBytesForRead() {
        segmentBytes.readLimit(segmentBS.capacity());
        return segmentBytes;
    }

    public Bytes segmentBytesForWrite() {
        segmentBytes.readPosition(0).readLimit(segmentBS.capacity());
        return segmentBytes;
    }

    private void closeNestedLocks() {
        unlinkFromSegmentContextsChain();
        readUnlockAndDecrementCount();
    }

    private void closeRootLocks() {
        verifyInnermostContext();
        switch (localLockState) {
            case UNLOCKED :
                return ;
            case READ_LOCKED :
                segmentHeader().readUnlock(segmentHeaderAddress());
                return ;
            case UPDATE_LOCKED :
                segmentHeader().updateUnlock(segmentHeaderAddress());
                return ;
            case WRITE_LOCKED :
                segmentHeader().writeUnlock(segmentHeaderAddress());
        }
    }

    private void linkToSegmentContextsChain() {
        LocksInterface innermostContextOnThisSegment = rootContextLockedOnThisSegment;
        while (true) {
            checkNestedContextsQueryDifferentKeys(innermostContextOnThisSegment);
            if ((innermostContextOnThisSegment.nextNode()) == null)
                break;
            
            innermostContextOnThisSegment = innermostContextOnThisSegment.nextNode();
        }
        innermostContextOnThisSegment.setNextNode(this);
    }

    private void unlinkFromSegmentContextsChain() {
        LocksInterface prevContext = rootContextLockedOnThisSegment;
        while (true) {
            LocksInterface nextNode = prevContext.nextNode();
            if ((nextNode == (this)) || (nextNode == null))
                break;
            
            prevContext = nextNode;
        }
        verifyInnermostContext();
        prevContext.setNextNode(null);
    }

    private void verifyInnermostContext() {
        if ((nextNode) != null) {
            throw new IllegalStateException(((this.h().toIdentityString()) + ": Attempt to close contexts not structurally"));
        } 
    }

    public boolean readZero() {
        return (rootContextLockedOnThisSegment.totalReadLockCount()) == 0;
    }

    public boolean realloc(long fromPos, int oldChunks, int newChunks) {
        if (((fromPos + newChunks) < (this.h().actualChunksPerSegmentTier)) && (freeList.isRangeClear((fromPos + oldChunks), (fromPos + newChunks)))) {
            freeList.setRange((fromPos + oldChunks), (fromPos + newChunks));
            return true;
        } else {
            return false;
        }
    }

    public boolean updateZero() {
        return (rootContextLockedOnThisSegment.totalUpdateLockCount()) == 0;
    }

    public boolean writeZero() {
        return (rootContextLockedOnThisSegment.totalWriteLockCount()) == 0;
    }

    public final ReadLock innerReadLock;

    public ReadLock innerReadLock() {
        return this.innerReadLock;
    }

    public final int indexInContextChain;

    public final WriteLock innerWriteLock;

    public WriteLock innerWriteLock() {
        return this.innerWriteLock;
    }

    public int indexInContextChain() {
        return this.indexInContextChain;
    }

    public final UpdateLock innerUpdateLock;

    public UpdateLock innerUpdateLock() {
        return this.innerUpdateLock;
    }

    public RuntimeException debugContextsAndLocks(InterProcessDeadLockException e) {
        String message = (this.h().toIdentityString()) + ":\n";
        message += "Contexts locked on this segment:\n";
        for (LocksInterface cxt = rootContextLockedOnThisSegment ; cxt != null ; cxt = cxt.nextNode()) {
            message += (cxt.debugLocksState()) + "\n";
        }
        message += "Current thread contexts:\n";
        for (int i = 0, size = this.contextChain.size() ; i < size ; i++) {
            LocksInterface cxt = this.contextAtIndexInChain(i);
            message += (cxt.debugLocksState()) + "\n";
        }
        throw new InterProcessDeadLockException(message , e);
    }

    final WrappedValueBytesData wrappedValueBytesData;

    public WrappedValueBytesData wrappedValueBytesData() {
        return this.wrappedValueBytesData;
    }

    final HashEntryChecksumStrategy hashEntryChecksumStrategy;

    public HashEntryChecksumStrategy hashEntryChecksumStrategy() {
        return this.hashEntryChecksumStrategy;
    }

    public final SizedReader<K> keyReader;

    public final ReusableBitSet freeList;

    public final EntryKeyBytesData entryKey;

    public final SizedReader<V> valueReader;

    public SizedReader<V> valueReader() {
        return this.valueReader;
    }

    public EntryKeyBytesData entryKey() {
        return this.entryKey;
    }

    public SizedReader<K> keyReader() {
        return this.keyReader;
    }

    @NotNull
    private static VanillaBytes unmonitoredVanillaBytes(PointerBytesStore segmentBS) {
        VanillaBytes bytes = new VanillaBytes(segmentBS) {};
        IOTools.unmonitor(bytes);
        return bytes;
    }

    public final EntryValueBytesData entryValue;

    public EntryValueBytesData entryValue() {
        return this.entryValue;
    }

    public final ChainingInterface rootContextInThisThread;

    public final PointerBytesStore segmentBS;

    public ChainingInterface rootContextInThisThread() {
        return this.rootContextInThisThread;
    }

    final WrappedValueInstanceDataHolder wrappedValueInstanceDataHolder;

    public WrappedValueInstanceDataHolder wrappedValueInstanceDataHolder() {
        return this.wrappedValueInstanceDataHolder;
    }

    public final List<ChainingInterface> contextChain;

    public List<ChainingInterface> contextChain() {
        return this.contextChain;
    }

    private static <T extends ChainingInterface>T initUsedAndReturn(VanillaChronicleMap map, ChainingInterface context) {
        try {
            context.initUsed(true, map);
            return ((T)(context));
        } catch (Throwable throwable) {
            try {
                ((AutoCloseable)(context)).close();
            } catch (Throwable t) {
                throwable.addSuppressed(t);
            }
            throw throwable;
        }
    }

    public final Bytes segmentBytes;

    public final ChecksumStrategy checksumStrategy;

    public ChecksumStrategy checksumStrategy() {
        return this.checksumStrategy;
    }

    public void readFoundEntry(long pos, long keySizeOffset, long keySize, long keyOffset) {
        initPos(pos);
        initEntryOffset(keySizeOffset);
        initKeySize(keySize);
        initKeyOffset(keyOffset);
    }

    public void closeMapEntryStagesReadFoundEntryDependants() {
        this.closeKeySearch();
    }

    private void deregisterIterationContextLockedInThisThread() {
        if ((this) instanceof IterationContext) {
            this.rootContextInThisThread.iterationContextLockedInThisThread = false;
        } 
    }

    public void closeIterationSegmentStagesDeregisterIterationContextLockedInThisThreadDependants() {
        this.closeLocks();
    }

    public <T>T contextAtIndexInChain(int index) {
        return ((T)(contextChain.get(index)));
    }

    public void closeVanillaChronicleMapHolderImplContextAtIndexInChainDependants() {
        this.closeLocks();
    }

    public void hookAfterEachIteration() {
        throwExceptionIfClosed();
        this.wrappedValueInstanceDataHolder.closeValue();
    }

    public Object entryForIteration() {
        return this;
    }

    @Override
    public <T extends ChainingInterface>T getContext(Class<? extends T> contextClass, BiFunction<ChainingInterface, VanillaChronicleMap, T> createChaining, VanillaChronicleMap map) {
        for (int i = 0 ; i < (contextChain.size()) ; i++) {
            ChainingInterface context = contextChain.get(i);
            if (((context.getClass()) == contextClass) && (!(context.usedInit()))) {
                return CompiledMapIterationContext.initUsedAndReturn(map, context);
            } 
        }
        int maxNestedContexts = 1 << 10;
        if ((contextChain.size()) > maxNestedContexts) {
            throw new IllegalStateException(((((((((map.toIdentityString()) + ": More than ") + maxNestedContexts) + " nested ChronicleHash contexts\n") + "are not supported. Very probable that you simply forgot to close context\n") + "somewhere (recommended to use try-with-resources statement).\n") + "Otherwise this is a bug, please report with this\n") + "stack trace on https://github.com/OpenHFT/Chronicle-Map/issues"));
        } 
        T context = createChaining.apply(this, map);
        return CompiledMapIterationContext.initUsedAndReturn(map, context);
    }

    @NotNull
    @Override
    public CompiledMapIterationContext<K, V, R> context() {
        return this;
    }

    public boolean entryDeleted() {
        return false;
    }

    public void closeEntry() {
        closePos();
        closeEntryOffset();
        closeKeySize();
        closeKeyOffset();
    }

    public long newEntrySize(Data<V> newValue, long entryStartOffset, long newValueOffset) {
        return (((checksumStrategy.extraEntryBytes()) + newValueOffset) + (newValue.size())) - entryStartOffset;
    }

    private void registerIterationContextLockedInThisThread() {
        if ((this) instanceof IterationContext) {
            this.rootContextInThisThread.iterationContextLockedInThisThread = true;
        } 
    }

    public void closeIterationSegmentStagesRegisterIterationContextLockedInThisThreadDependants() {
        this.closeLocks();
    }

    public boolean shouldTestEntry() {
        return true;
    }

    @Override
    public List<ChainingInterface> getContextChain() {
        return contextChain;
    }

    public long keySize = -1;

    public boolean keySizeInit() {
        return (this.keySize) != (-1);
    }

    public void initKeySize(long keySize) {
        boolean wasKeySizeInit = this.keySizeInit();
        this.keySize = keySize;
        if (wasKeySizeInit)
            this.closeKeySizeDependants();
        
    }

    public long keySize() {
        assert this.keySizeInit() : "KeySize should be init";
        return this.keySize;
    }

    public void closeKeySize() {
        if (!(this.keySizeInit()))
            return ;
        
        this.closeKeySizeDependants();
        this.keySize = -1;
    }

    public void closeKeySizeDependants() {
        this.closeMapEntryStagesKeyEndDependants();
        this.entryKey.closeEntryKeyBytesDataSizeDependants();
        this.closeKeyHash();
        this.hashEntryChecksumStrategy.closeHashEntryChecksumStrategyComputeChecksumDependants();
    }

    public long hashLookupEntry = 0;

    public boolean hashLookupEntryInit() {
        return (this.hashLookupEntry) != 0;
    }

    public void initHashLookupEntry(long entry) {
        throwExceptionIfClosed();
        hashLookupEntry = entry;
    }

    public long hashLookupEntry() {
        assert this.hashLookupEntryInit() : "HashLookupEntry should be init";
        return this.hashLookupEntry;
    }

    void closeHashLookupEntry() {
        this.hashLookupEntry = 0;
    }

    public int segmentIndex = -1;

    public boolean segmentIndexInit() {
        return (this.segmentIndex) >= 0;
    }

    public void initSegmentIndex(int segmentIndex) {
        boolean wasSegmentIndexInit = this.segmentIndexInit();
        this.segmentIndex = segmentIndex;
        if (wasSegmentIndexInit)
            this.closeSegmentIndexDependants();
        
    }

    public int segmentIndex() {
        assert this.segmentIndexInit() : "SegmentIndex should be init";
        return this.segmentIndex;
    }

    public void closeSegmentIndex() {
        if (!(this.segmentIndexInit()))
            return ;
        
        this.closeSegmentIndexDependants();
        this.segmentIndex = -1;
    }

    public void closeSegmentIndexDependants() {
        this.closeSegmentHeader();
        this.closeSegmentTier();
    }

    public boolean entryRemovedOnThisIteration = false;

    boolean entryRemovedOnThisIterationInit() {
        return this.entryRemovedOnThisIteration;
    }

    protected void initEntryRemovedOnThisIteration(boolean entryRemovedOnThisIteration) {
        this.entryRemovedOnThisIteration = entryRemovedOnThisIteration;
    }

    public void closeEntryRemovedOnThisIteration() {
        this.entryRemovedOnThisIteration = false;
    }

    public Data<K> inputKey = null;

    public boolean inputKeyInit() {
        return (this.inputKey) != null;
    }

    public void initInputKey(Data<K> inputKey) {
        boolean wasInputKeyInit = this.inputKeyInit();
        this.inputKey = inputKey;
        if (wasInputKeyInit)
            this.closeInputKeyDependants();
        
    }

    public Data<K> inputKey() {
        assert this.inputKeyInit() : "InputKey should be init";
        return this.inputKey;
    }

    public void closeInputKey() {
        if (!(this.inputKeyInit()))
            return ;
        
        this.closeInputKeyDependants();
        this.inputKey = null;
    }

    public void closeInputKeyDependants() {
        this.closeIterationSegmentStagesCheckNestedContextsQueryDifferentKeysDependants();
        this.closeKeySearchKeyEqualsDependants();
    }

    public long pos = -1;

    public boolean posInit() {
        return (this.pos) != (-1);
    }

    public void initPos(long pos) {
        boolean wasPosInit = this.posInit();
        this.pos = pos;
        if (wasPosInit)
            this.closePosDependants();
        
    }

    public long pos() {
        assert this.posInit() : "Pos should be init";
        return this.pos;
    }

    public void closePos() {
        if (!(this.posInit()))
            return ;
        
        this.closePosDependants();
        this.pos = -1;
    }

    public void closePosDependants() {
        this.closeEntryOffset();
    }

    public long keyOffset = -1;

    public boolean keyOffsetInit() {
        return (this.keyOffset) != (-1);
    }

    public void initKeyOffset(long keyOffset) {
        boolean wasKeyOffsetInit = this.keyOffsetInit();
        this.keyOffset = keyOffset;
        if (wasKeyOffsetInit)
            this.closeKeyOffsetDependants();
        
    }

    public long keyOffset() {
        assert this.keyOffsetInit() : "KeyOffset should be init";
        return this.keyOffset;
    }

    public void closeKeyOffset() {
        if (!(this.keyOffsetInit()))
            return ;
        
        this.closeKeyOffsetDependants();
        this.keyOffset = -1;
    }

    public void closeKeyOffsetDependants() {
        this.closeMapEntryStagesKeyEndDependants();
        this.entryKey.closeEntryKeyBytesDataInnerGetUsingDependants();
        this.closeKeyHash();
    }

    public long keyEnd() {
        return (keyOffset()) + (keySize());
    }

    public void closeMapEntryStagesKeyEndDependants() {
        this.closeMapEntryStagesCountValueSizeOffsetDependants();
        this.closeMapEntryStagesEntryEndDependants();
        this.hashEntryChecksumStrategy.closeHashEntryChecksumStrategyComputeChecksumDependants();
    }

    long countValueSizeOffset() {
        return keyEnd();
    }

    public void closeMapEntryStagesCountValueSizeOffsetDependants() {
        this.closeValueSizeOffset();
    }

    public long valueSizeOffset = -1;

    public boolean valueSizeOffsetInit() {
        return (this.valueSizeOffset) != (-1);
    }

    @SuppressWarnings(value = "unused")
    void initValueSizeOffset() {
        boolean wasValueSizeOffsetInit = this.valueSizeOffsetInit();
        valueSizeOffset = countValueSizeOffset();
        if (wasValueSizeOffsetInit)
            this.closeValueSizeOffsetDependants();
        
    }

    public long valueSizeOffset() {
        if (!(this.valueSizeOffsetInit()))
            this.initValueSizeOffset();
        
        return this.valueSizeOffset;
    }

    public void closeValueSizeOffset() {
        if (!(this.valueSizeOffsetInit()))
            return ;
        
        this.closeValueSizeOffsetDependants();
        this.valueSizeOffset = -1;
    }

    public void closeValueSizeOffsetDependants() {
        this.closeValueSize();
    }

    public int allocatedChunks = 0;

    public boolean allocatedChunksInit() {
        return (this.allocatedChunks) != 0;
    }

    public void initAllocatedChunks(int allocatedChunks) {
        this.allocatedChunks = allocatedChunks;
    }

    public int allocatedChunks() {
        assert this.allocatedChunksInit() : "AllocatedChunks should be init";
        return this.allocatedChunks;
    }

    public void closeAllocatedChunks() {
        this.allocatedChunks = 0;
    }

    private VanillaChronicleMap<K, V, R> m = null;

    public boolean mapInit() {
        return (this.m) != null;
    }

    public void initMap(VanillaChronicleMap map) {
        boolean wasMapInit = this.mapInit();
        m = map;
        if (wasMapInit)
            this.closeMapDependants();
        
    }

    public VanillaChronicleMap<K, V, R> m() {
        assert this.mapInit() : "Map should be init";
        return this.m;
    }

    public void closeMap() {
        if (!(this.mapInit()))
            return ;
        
        this.closeMapDependants();
        this.m = null;
    }

    public void closeMapDependants() {
        this.closeVanillaChronicleMapHolderImplMDependants();
        this.closeVanillaChronicleMapHolderImplHDependants();
    }

    public void closeVanillaChronicleMapHolderImplMDependants() {
        this.closeValueSize();
        this.wrappedValueInstanceDataHolder.closeValue();
        this.closeKeySearch();
    }

    long sizeOfEverythingBeforeValue(long keySize, long valueSize) {
        return (((this.m().keySizeMarshaller.storingLength(keySize)) + keySize) + (checksumStrategy.extraEntryBytes())) + (this.m().valueSizeMarshaller.storingLength(valueSize));
    }

    public long innerEntrySize(long sizeOfEverythingBeforeValue, long valueSize) {
        if ((!(this.m().constantlySizedEntry)) && (this.m().couldNotDetermineAlignmentBeforeAllocation))
            sizeOfEverythingBeforeValue += this.m().worstAlignment;
        
        int alignment = this.m().alignment;
        return (VanillaChronicleMap.alignAddr(sizeOfEverythingBeforeValue, alignment)) + (VanillaChronicleMap.alignAddr(valueSize, alignment));
    }

    public final long entrySize(long keySize, long valueSize) {
        long sizeOfEverythingBeforeValue = sizeOfEverythingBeforeValue(keySize, valueSize);
        return innerEntrySize(sizeOfEverythingBeforeValue, valueSize);
    }

    @Override
    public ChronicleSet<K> set() {
        return m().chronicleSet;
    }

    @Override
    public ChronicleMap<K, V> map() {
        return m();
    }

    public ChronicleHash<K, ?, ?, ?> hash() {
        return (set()) != null ? set() : map();
    }

    @Override
    public VanillaChronicleHash<K, ?, ?, ?> h() {
        return m();
    }

    public void closeVanillaChronicleMapHolderImplHDependants() {
        this.closeIterationSegmentStagesCheckNestedContextsQueryDifferentKeysDependants();
        this.closeHashLookupSearchHlDependants();
        this.closeSegmentHeader();
        this.closeLocks();
        this.closeSegmentTier();
        this.closeIterationSegmentStagesTierCountersAreaAddrDependants();
        this.closeSegment();
        this.closeEntryOffset();
        this.closeEntrySizeInChunks();
        this.closeDelayedUpdateChecksum();
        this.closeOwnerThreadHolderCheckAccessingFromOwnerThreadDependants();
        this.closeHashLookupSearchNextPosDependants();
        this.closeKeySearch();
        this.closeMapSegmentIterationCheckEntryNotRemovedOnThisIterationDependants();
    }

    public void checkNestedContextsQueryDifferentKeys(LocksInterface innermostContextOnThisSegment) {
    }

    public void closeIterationSegmentStagesCheckNestedContextsQueryDifferentKeysDependants() {
        this.closeLocks();
    }

    public void checkIterationContextNotLockedInThisThread() {
        if (this.rootContextInThisThread.iterationContextLockedInThisThread) {
            throw new IllegalStateException((((this.h().toIdentityString()) + ": Update or Write ") + "locking is forbidden in the context of locked iteration context"));
        } 
    }

    public CompactOffHeapLinearHashTable hl() {
        return this.h().hashLookup;
    }

    public void closeHashLookupSearchHlDependants() {
        this.closeSearchKey();
        this.closeHashLookupSearchNextPosDependants();
        this.closeHashLookupSearchFoundDependants();
    }

    long searchKey = CompactOffHeapLinearHashTable.UNSET_KEY;

    public long searchStartPos;

    public boolean searchKeyInit() {
        return (this.searchKey) != (CompactOffHeapLinearHashTable.UNSET_KEY);
    }

    public void initSearchKey(long searchKey) {
        boolean wasSearchKeyInit = this.searchKeyInit();
        this.searchKey = searchKey;
        searchStartPos = hl().hlPos(searchKey);
        if (wasSearchKeyInit)
            this.closeSearchKeyDependants();
        
    }

    public long searchKey() {
        assert this.searchKeyInit() : "SearchKey should be init";
        return this.searchKey;
    }

    public long searchStartPos() {
        assert this.searchKeyInit() : "SearchKey should be init";
        return this.searchStartPos;
    }

    public void closeSearchKey() {
        if (!(this.searchKeyInit()))
            return ;
        
        this.closeSearchKeyDependants();
        this.searchKey = CompactOffHeapLinearHashTable.UNSET_KEY;
    }

    public void closeSearchKeyDependants() {
        this.closeHashLookupPos();
        this.closeHashLookupSearchNextPosDependants();
    }

    public long segmentHeaderAddress;

    public SegmentHeader segmentHeader = null;

    public boolean segmentHeaderInit() {
        return (this.segmentHeader) != null;
    }

    private void initSegmentHeader() {
        boolean wasSegmentHeaderInit = this.segmentHeaderInit();
        segmentHeaderAddress = this.h().segmentHeaderAddress(segmentIndex());
        segmentHeader = BigSegmentHeader.INSTANCE;
        if (wasSegmentHeaderInit)
            this.closeSegmentHeaderDependants();
        
    }

    public long segmentHeaderAddress() {
        if (!(this.segmentHeaderInit()))
            this.initSegmentHeader();
        
        return this.segmentHeaderAddress;
    }

    public SegmentHeader segmentHeader() {
        if (!(this.segmentHeaderInit()))
            this.initSegmentHeader();
        
        return this.segmentHeader;
    }

    public void closeSegmentHeader() {
        if (!(this.segmentHeaderInit()))
            return ;
        
        this.closeSegmentHeaderDependants();
        this.segmentHeader = null;
    }

    public void closeSegmentHeaderDependants() {
        this.closeIterationSegmentStagesTierEntriesDependants();
        this.closeIterationSegmentStagesTierEntriesDependants();
        this.closeLocks();
        this.innerReadLock.closeReadLockLockDependants();
        this.closeIterationSegmentStagesLowestPossiblyFreeChunkDependants();
        this.closeIterationSegmentStagesLowestPossiblyFreeChunkDependants();
    }

    private void resetSegmentLock(ChronicleHashCorruption.Listener corruptionListener, ChronicleHashCorruptionImpl corruption) {
        long lockState = this.segmentHeader().getLockState(this.segmentHeaderAddress());
        if (lockState != (this.segmentHeader().resetLockState())) {
            ChronicleHashCorruptionImpl.report(corruptionListener, corruption, this.segmentIndex(), () -> ChronicleHashCorruptionImpl.format("lock of segment {} is not clear: {}", this.segmentIndex(), this.segmentHeader().lockStateToString(lockState)));
            this.segmentHeader().resetLock(this.segmentHeaderAddress());
        } 
    }

    public int tier = -1;

    public long tierIndex;

    public long tierBaseAddr;

    public boolean segmentTierInit() {
        return (this.tier) >= 0;
    }

    public void initSegmentTier() {
        boolean wasSegmentTierInit = this.segmentTierInit();
        tierIndex = (segmentIndex()) + 1L;
        tierBaseAddr = this.h().segmentBaseAddr(segmentIndex());
        tier = 0;
        if (wasSegmentTierInit)
            this.closeSegmentTierDependants();
        
    }

    public void initSegmentTier(int tier, long tierIndex) {
        boolean wasSegmentTierInit = this.segmentTierInit();
        this.tier = tier;
        this.tierIndex = tierIndex;
        assert tierIndex > 0;
        this.tierBaseAddr = this.h().tierIndexToBaseAddr(tierIndex);
        if (wasSegmentTierInit)
            this.closeSegmentTierDependants();
        
    }

    public void initSegmentTier(int tier, long tierIndex, long tierBaseAddr) {
        boolean wasSegmentTierInit = this.segmentTierInit();
        this.tier = tier;
        this.tierIndex = tierIndex;
        this.tierBaseAddr = tierBaseAddr;
        if (wasSegmentTierInit)
            this.closeSegmentTierDependants();
        
    }

    public void initSegmentTier_WithBaseAddr(int tier, long tierBaseAddr, long tierIndex) {
        boolean wasSegmentTierInit = this.segmentTierInit();
        this.tier = tier;
        this.tierIndex = tierIndex;
        this.tierBaseAddr = tierBaseAddr;
        if (wasSegmentTierInit)
            this.closeSegmentTierDependants();
        
    }

    public int tier() {
        if (!(this.segmentTierInit()))
            this.initSegmentTier();
        
        return this.tier;
    }

    public long tierBaseAddr() {
        if (!(this.segmentTierInit()))
            this.initSegmentTier();
        
        return this.tierBaseAddr;
    }

    public long tierIndex() {
        if (!(this.segmentTierInit()))
            this.initSegmentTier();
        
        return this.tierIndex;
    }

    public void closeSegmentTier() {
        if (!(this.segmentTierInit()))
            return ;
        
        this.closeSegmentTierDependants();
        this.tier = -1;
    }

    public void closeSegmentTierDependants() {
        this.closeKeyHash();
        this.closeIterationSegmentStagesTierCountersAreaAddrDependants();
        this.closeIterationSegmentStagesTierEntriesDependants();
        this.closeIterationSegmentStagesTierEntriesDependants();
        this.closeHashLookupPos();
        this.closeHashLookupSearchAddrDependants();
        this.closeIterationSegmentStagesLowestPossiblyFreeChunkDependants();
        this.closeIterationSegmentStagesLowestPossiblyFreeChunkDependants();
        this.closeSegment();
        this.hashEntryChecksumStrategy.closeHashEntryChecksumStrategyComputeChecksumDependants();
    }

    private void shiftHashLookupEntries() {
        VanillaChronicleHash<K, ?, ?, ?> h = this.h();
        CompactOffHeapLinearHashTable hl = h.hashLookup;
        long hlAddr = this.tierBaseAddr();
        long hlPos = 0;
        long steps = 0;
        do {
            long hlEntry = hl.readEntry(hlAddr, hlPos);
            if (!(hl.empty(hlEntry))) {
                long searchKey = hl.key(hlEntry);
                long hlHolePos = hl.hlPos(searchKey);
                while (hlHolePos != hlPos) {
                    long hlHoleEntry = hl.readEntry(hlAddr, hlHolePos);
                    if (hl.empty(hlHoleEntry)) {
                        hl.writeEntry(hlAddr, hlHolePos, hlEntry);
                        if ((hl.remove(hlAddr, hlPos)) != hlPos) {
                            hlPos = hl.stepBack(hlPos);
                            steps--;
                        } 
                        break;
                    } 
                    hlHolePos = hl.step(hlHolePos);
                }
            } 
            hlPos = hl.step(hlPos);
            steps++;
        } while ((hlPos != 0) || (steps == 0) );
    }

    long keyHash = 0;

    public boolean keyHashInit() {
        return (this.keyHash) != 0;
    }

    void initKeyHash() {
        boolean wasKeyHashInit = this.keyHashInit();
        long addr = (this.tierBaseAddr()) + (this.keyOffset());
        long len = this.keySize();
        keyHash = LongHashFunction.xx_r39().hashMemory(addr, len);
        if (wasKeyHashInit)
            this.closeKeyHashDependants();
        
    }

    public long keyHash() {
        if (!(this.keyHashInit()))
            this.initKeyHash();
        
        return this.keyHash;
    }

    public void closeKeyHash() {
        if (!(this.keyHashInit()))
            return ;
        
        this.closeKeyHashDependants();
        this.keyHash = 0;
    }

    public void closeKeyHashDependants() {
        this.closeIterationKeyHashCodeKeyHashCodeDependants();
    }

    @Override
    public long keyHashCode() {
        return keyHash();
    }

    public void closeIterationKeyHashCodeKeyHashCodeDependants() {
        this.hashEntryChecksumStrategy.closeHashEntryChecksumStrategyComputeChecksumDependants();
    }

    public long tierCountersAreaAddr() {
        return (tierBaseAddr()) + (this.h().tierHashLookupOuterSize);
    }

    public void closeIterationSegmentStagesTierCountersAreaAddrDependants() {
        this.closeIterationSegmentStagesTierEntriesDependants();
        this.closeIterationSegmentStagesTierEntriesDependants();
        this.closeIterationSegmentStagesLowestPossiblyFreeChunkDependants();
        this.closeIterationSegmentStagesLowestPossiblyFreeChunkDependants();
    }

    public long prevTierIndex() {
        return TierCountersArea.prevTierIndex(tierCountersAreaAddr());
    }

    public void prevTier() {
        if ((tier()) == 0) {
            throw new IllegalStateException(((this.h().toIdentityString()) + ": first tier doesn\'t have previous"));
        } 
        initSegmentTier(((tier()) - 1), prevTierIndex());
    }

    public void prevTierIndex(long prevTierIndex) {
        TierCountersArea.prevTierIndex(tierCountersAreaAddr(), prevTierIndex);
    }

    public void goToFirstTier() {
        while ((tier()) != 0) {
            prevTier();
        }
    }

    public long nextTierIndex() {
        if ((tier()) == 0) {
            return segmentHeader().nextTierIndex(segmentHeaderAddress());
        } else {
            return TierCountersArea.nextTierIndex(tierCountersAreaAddr());
        }
    }

    public boolean hasNextTier() {
        return (nextTierIndex()) != 0;
    }

    public void nextTierIndex(long nextTierIndex) {
        if ((tier()) == 0) {
            segmentHeader().nextTierIndex(segmentHeaderAddress(), nextTierIndex);
        } else {
            TierCountersArea.nextTierIndex(tierCountersAreaAddr(), nextTierIndex);
        }
    }

    private void zeroOutFirstSegmentTierCountersArea(ChronicleHashCorruption.Listener corruptionListener, ChronicleHashCorruptionImpl corruption) {
        this.nextTierIndex(0);
        if ((this.prevTierIndex()) != 0) {
            ChronicleHashCorruptionImpl.report(corruptionListener, corruption, this.segmentIndex(), () -> ChronicleHashCorruptionImpl.format("stored prev tier index in first tier of segment {}: {}, should be 0", this.segmentIndex(), this.prevTierIndex()));
            this.prevTierIndex(0);
        } 
        long tierCountersAreaAddr = this.tierCountersAreaAddr();
        if ((TierCountersArea.segmentIndex(tierCountersAreaAddr)) != 0) {
            ChronicleHashCorruptionImpl.report(corruptionListener, corruption, this.segmentIndex(), () -> ChronicleHashCorruptionImpl.format("stored segment index in first tier of segment {}: {}, should be 0", this.segmentIndex(), TierCountersArea.segmentIndex(tierCountersAreaAddr)));
            TierCountersArea.segmentIndex(tierCountersAreaAddr, 0);
        } 
        if ((TierCountersArea.tier(tierCountersAreaAddr)) != 0) {
            ChronicleHashCorruptionImpl.report(corruptionListener, corruption, this.segmentIndex(), () -> ChronicleHashCorruptionImpl.format("stored tier in first tier of segment {}: {}, should be 0", this.segmentIndex(), TierCountersArea.tier(tierCountersAreaAddr)));
            TierCountersArea.tier(tierCountersAreaAddr, 0);
        } 
    }

    public long tierEntries() {
        if ((tier()) == 0) {
            return segmentHeader().entries(segmentHeaderAddress());
        } else {
            return TierCountersArea.entries(tierCountersAreaAddr());
        }
    }

    public long tierEntriesForIteration() {
        throwExceptionIfClosed();
        return this.tierEntries();
    }

    public void tierEntries(long tierEntries) {
        if ((tier()) == 0) {
            segmentHeader().entries(segmentHeaderAddress(), tierEntries);
        } else {
            TierCountersArea.entries(tierCountersAreaAddr(), tierEntries);
        }
    }

    public void closeIterationSegmentStagesTierEntriesDependants() {
        this.closeSegment();
    }

    public long tierDeleted() {
        if ((tier()) == 0) {
            return segmentHeader().deleted(segmentHeaderAddress());
        } else {
            return TierCountersArea.deleted(tierCountersAreaAddr());
        }
    }

    public void tierDeleted(long tierDeleted) {
        if ((tier()) == 0) {
            segmentHeader().deleted(segmentHeaderAddress(), tierDeleted);
        } else {
            TierCountersArea.deleted(tierCountersAreaAddr(), tierDeleted);
        }
    }

    private void recoverTierEntriesCounter(long entries, ChronicleHashCorruption.Listener corruptionListener, ChronicleHashCorruptionImpl corruption) {
        if ((this.tierEntries()) != entries) {
            ChronicleHashCorruptionImpl.report(corruptionListener, corruption, this.segmentIndex(), () -> ChronicleHashCorruptionImpl.format((("Wrong number of entries counter for tier with index {}, " + "stored: {}, should be: ") + (this.tierIndex())), this.tierEntries(), entries));
            this.tierEntries(entries);
        } 
    }

    private long addr() {
        return this.tierBaseAddr();
    }

    public void closeHashLookupSearchAddrDependants() {
        this.closeHashLookupSearchNextPosDependants();
    }

    public long lowestPossiblyFreeChunk() {
        if ((tier()) == 0) {
            return segmentHeader().lowestPossiblyFreeChunk(segmentHeaderAddress());
        } else {
            return TierCountersArea.lowestPossiblyFreeChunkTiered(tierCountersAreaAddr());
        }
    }

    public void closeIterationSegmentStagesLowestPossiblyFreeChunkDependants() {
        this.closeSegment();
    }

    public void lowestPossiblyFreeChunk(long lowestPossiblyFreeChunk) {
        if ((tier()) == 0) {
            segmentHeader().lowestPossiblyFreeChunk(segmentHeaderAddress(), lowestPossiblyFreeChunk);
        } else {
            TierCountersArea.lowestPossiblyFreeChunkTiered(tierCountersAreaAddr(), lowestPossiblyFreeChunk);
        }
    }

    public long entrySpaceOffset = 0;

    boolean segmentInit() {
        return (entrySpaceOffset) > 0;
    }

    void initSegment() {
        boolean wasSegmentInit = this.segmentInit();
        VanillaChronicleHash<K, ?, ?, ?> h = this.h();
        long segmentBaseAddr = this.tierBaseAddr();
        segmentBS.set(segmentBaseAddr, h.tierSize);
        segmentBytes.clear();
        long freeListOffset = (h.tierHashLookupOuterSize) + (VanillaChronicleHash.TIER_COUNTERS_AREA_SIZE);
        freeList.setOffset((segmentBaseAddr + freeListOffset));
        entrySpaceOffset = (freeListOffset + (h.tierFreeListOuterSize)) + (h.tierEntrySpaceInnerOffset);
        if (wasSegmentInit)
            this.closeSegmentDependants();
        
    }

    public long entrySpaceOffset() {
        if (!(this.segmentInit()))
            this.initSegment();
        
        return this.entrySpaceOffset;
    }

    public ReusableBitSet freeList() {
        if (!(this.segmentInit()))
            this.initSegment();
        
        return this.freeList;
    }

    public Bytes segmentBytes() {
        if (!(this.segmentInit()))
            this.initSegment();
        
        return this.segmentBytes;
    }

    public PointerBytesStore segmentBS() {
        if (!(this.segmentInit()))
            this.initSegment();
        
        return this.segmentBS;
    }

    void closeSegment() {
        if (!(this.segmentInit()))
            return ;
        
        this.closeSegmentDependants();
        entrySpaceOffset = 0;
    }

    public void closeSegmentDependants() {
        this.closeValueSize();
        this.closeKeySearchKeyEqualsDependants();
        this.closeKeySearch();
        this.hashEntryChecksumStrategy.closeHashEntryChecksumStrategyComputeAndStoreChecksumDependants();
        this.entryValue.closeEntryValueBytesDataInnerGetUsingDependants();
        this.closeEntryOffset();
        this.entryKey.closeEntryKeyBytesDataInnerGetUsingDependants();
    }

    public long valueSize = -1;

    public long valueOffset;

    public boolean valueSizeInit() {
        return (this.valueSize) != (-1);
    }

    @SuppressWarnings(value = "unused")
    void initValueSize() {
        boolean wasValueSizeInit = this.valueSizeInit();
        Bytes segmentBytes = this.segmentBytesForReadGuarded();
        segmentBytes.readPosition(valueSizeOffset());
        valueSize = this.m().readValueSize(segmentBytes);
        valueOffset = segmentBytes.readPosition();
        if (wasValueSizeInit)
            this.closeValueSizeDependants();
        
    }

    void initValueSize(long valueSize) {
        boolean wasValueSizeInit = this.valueSizeInit();
        this.valueSize = valueSize;
        Bytes segmentBytes = this.segmentBytesForWriteGuarded();
        segmentBytes.writePosition(valueSizeOffset());
        this.m().valueSizeMarshaller.writeSize(segmentBytes, valueSize);
        long currentPosition = segmentBytes.writePosition();
        long currentAddr = segmentBytes.addressForRead(currentPosition);
        long skip = (VanillaChronicleMap.alignAddr(currentAddr, this.m().alignment)) - currentAddr;
        if (skip > 0)
            segmentBytes.writeSkip(skip);
        
        valueOffset = segmentBytes.writePosition();
        if (wasValueSizeInit)
            this.closeValueSizeDependants();
        
    }

    public long valueOffset() {
        if (!(this.valueSizeInit()))
            this.initValueSize();
        
        return this.valueOffset;
    }

    public long valueSize() {
        if (!(this.valueSizeInit()))
            this.initValueSize();
        
        return this.valueSize;
    }

    public void closeValueSize() {
        if (!(this.valueSizeInit()))
            return ;
        
        this.closeValueSizeDependants();
        this.valueSize = -1;
    }

    public void closeValueSizeDependants() {
        this.closeMapEntryStagesEntryEndDependants();
        this.entryValue.closeEntryValueBytesDataSizeDependants();
        this.entryValue.closeEntryValueBytesDataInnerGetUsingDependants();
    }

    public long entryEnd() {
        return (valueOffset()) + (valueSize());
    }

    public void closeMapEntryStagesEntryEndDependants() {
        this.closeMapEntryStagesEntrySizeDependants();
        this.hashEntryChecksumStrategy.closeHashEntryChecksumStrategyComputeChecksumDependants();
        this.hashEntryChecksumStrategy.closeHashEntryChecksumStrategyComputeAndStoreChecksumDependants();
    }

    public void writeValue(Data<?> value) {
        initDelayedUpdateChecksum(true);
        value.writeTo(this.segmentBS(), valueOffset());
    }

    public void initValue(Data<?> value) {
        initValueSize(value.size());
        writeValue(value);
    }

    boolean keyEquals(long keySize, long keyOffset) {
        return ((inputKey().size()) == keySize) && (inputKey().equivalent(this.segmentBS(), keyOffset));
    }

    public void closeKeySearchKeyEqualsDependants() {
        this.closeKeySearch();
    }

    public long keySizeOffset = -1;

    public boolean entryOffsetInit() {
        return (this.keySizeOffset) != (-1);
    }

    public void initEntryOffset() {
        boolean wasEntryOffsetInit = this.entryOffsetInit();
        keySizeOffset = (this.entrySpaceOffset()) + ((pos()) * (this.h().chunkSize));
        if (wasEntryOffsetInit)
            this.closeEntryOffsetDependants();
        
    }

    public void initEntryOffset(long keySizeOffset) {
        boolean wasEntryOffsetInit = this.entryOffsetInit();
        this.keySizeOffset = keySizeOffset;
        if (wasEntryOffsetInit)
            this.closeEntryOffsetDependants();
        
    }

    public long keySizeOffset() {
        if (!(this.entryOffsetInit()))
            this.initEntryOffset();
        
        return this.keySizeOffset;
    }

    public void closeEntryOffset() {
        if (!(this.entryOffsetInit()))
            return ;
        
        this.closeEntryOffsetDependants();
        this.keySizeOffset = -1;
    }

    public void closeEntryOffsetDependants() {
        this.closeMapEntryStagesEntrySizeDependants();
        this.closeDelayedUpdateChecksum();
    }

    public void writeNewEntry(long pos, Data<?> key) {
        initPos(pos);
        initKeySize(key.size());
        Bytes segmentBytes = this.segmentBytesForWriteGuarded();
        segmentBytes.writePosition(keySizeOffset());
        this.h().keySizeMarshaller.writeSize(segmentBytes, keySize());
        initKeyOffset(segmentBytes.writePosition());
        key.writeTo(this.segmentBS(), keyOffset());
    }

    long entrySize() {
        return ((checksumStrategy.extraEntryBytes()) + (entryEnd())) - (keySizeOffset());
    }

    public void closeMapEntryStagesEntrySizeDependants() {
        this.closeEntrySizeInChunks();
    }

    public int entrySizeInChunks = 0;

    public boolean entrySizeInChunksInit() {
        return (this.entrySizeInChunks) != 0;
    }

    void initEntrySizeInChunks() {
        entrySizeInChunks = this.h().inChunks(entrySize());
    }

    public void initEntrySizeInChunks(int actuallyUsedChunks) {
        entrySizeInChunks = actuallyUsedChunks;
    }

    public int entrySizeInChunks() {
        if (!(this.entrySizeInChunksInit()))
            this.initEntrySizeInChunks();
        
        return this.entrySizeInChunks;
    }

    public void closeEntrySizeInChunks() {
        this.entrySizeInChunks = 0;
    }

    public final void freeExtraAllocatedChunks() {
        if (((!(this.m().constantlySizedEntry)) && (this.m().couldNotDetermineAlignmentBeforeAllocation)) && ((entrySizeInChunks()) < (this.allocatedChunks()))) {
            this.freeExtraGuarded(pos(), this.allocatedChunks(), entrySizeInChunks());
        } else {
            initEntrySizeInChunks(this.allocatedChunks());
        }
    }

    public long newSizeOfEverythingBeforeValue(Data<V> newValue) {
        return ((valueSizeOffset()) + (this.m().valueSizeMarshaller.storingLength(newValue.size()))) - (keySizeOffset());
    }

    public void readExistingEntry(long pos) {
        initPos(pos);
        Bytes segmentBytes = this.segmentBytesForReadGuarded();
        segmentBytes.readPosition(keySizeOffset());
        initKeySize(this.h().keySizeMarshaller.readSize(segmentBytes));
        initKeyOffset(segmentBytes.readPosition());
    }

    public void copyExistingEntry(long newPos, long bytesToCopy, long oldKeyAddr, long oldKeySizeAddr) {
        initPos(newPos);
        initKeyOffset(((keySizeOffset()) + (oldKeyAddr - oldKeySizeAddr)));
        Access.copy(Access.nativeAccess(), null, oldKeySizeAddr, Access.checkedBytesStoreAccess(), this.segmentBS(), keySizeOffset(), bytesToCopy);
    }

    private void recoverLowestPossibleFreeChunkTiered(ChronicleHashCorruption.Listener corruptionListener, ChronicleHashCorruptionImpl corruption) {
        long lowestFreeChunk = this.freeList().nextClearBit(0);
        if (lowestFreeChunk == (-1))
            lowestFreeChunk = this.m().actualChunksPerSegmentTier;
        
        if ((this.lowestPossiblyFreeChunk()) != lowestFreeChunk) {
            long finalLowestFreeChunk = lowestFreeChunk;
            ChronicleHashCorruptionImpl.report(corruptionListener, corruption, this.segmentIndex(), () -> ChronicleHashCorruptionImpl.format(("wrong lowest free chunk for tier with index {}, " + "stored: {}, should be: {}"), this.tierIndex(), this.lowestPossiblyFreeChunk(), finalLowestFreeChunk));
            this.lowestPossiblyFreeChunk(lowestFreeChunk);
        } 
    }

    public void nextTier() {
        _SegmentStages_nextTier();
        if (this.hashLookupEntryInit())
            this.initSearchKey(this.h().hashLookup.key(this.hashLookupEntry()));
        
    }

    public void goToLastTier() {
        while (hasNextTier()) {
            nextTier();
        }
    }

    public long size() {
        goToFirstTier();
        long size = (tierEntries()) - (tierDeleted());
        while (hasNextTier()) {
            nextTier();
            size += (tierEntries()) - (tierDeleted());
        }
        return size;
    }

    @Override
    public long alloc(int chunks, long prevPos, int prevChunks) {
        long ret = this.allocReturnCodeGuarded(chunks);
        if (prevPos >= 0)
            this.freeGuarded(prevPos, prevChunks);
        
        if (ret >= 0)
            return ret;
        
        while (true) {
            this.nextTier();
            ret = this.allocReturnCodeGuarded(chunks);
            if (ret >= 0)
                return ret;
            
        }
    }

    public boolean initEntryAndKeyCopying(long entrySize, long bytesToCopy, long prevPos, int prevChunks) {
        initAllocatedChunks(this.h().inChunks(entrySize));
        long oldSegmentTierBaseAddr = this.tierBaseAddr();
        long oldKeySizeAddr = oldSegmentTierBaseAddr + (this.keySizeOffset());
        long oldKeyAddr = oldSegmentTierBaseAddr + (this.keyOffset());
        int tierBeforeAllocation = this.tier();
        long pos = this.alloc(allocatedChunks(), prevPos, prevChunks);
        this.copyExistingEntry(pos, bytesToCopy, oldKeyAddr, oldKeySizeAddr);
        return (this.tier()) != tierBeforeAllocation;
    }

    public void verifyTierCountersAreaData() {
        goToFirstTier();
        while (true) {
            int tierSegmentIndex = TierCountersArea.segmentIndex(tierCountersAreaAddr());
            if (tierSegmentIndex != (segmentIndex())) {
                throw new AssertionError(((((((((("segmentIndex: " + (segmentIndex())) + ", tier: ") + (tier())) + ", tierIndex: ") + (tierIndex())) + ", tierBaseAddr: ") + (tierBaseAddr())) + " reports it belongs to segmentIndex ") + tierSegmentIndex));
            } 
            if (hasNextTier()) {
                long currentTierIndex = this.tierIndex();
                nextTier();
                if ((prevTierIndex()) != currentTierIndex) {
                    throw new AssertionError(((((((((((("segmentIndex: " + (segmentIndex())) + ", tier: ") + (tier())) + ", tierIndex: ") + (tierIndex())) + ", tierBaseAddr: ") + (tierBaseAddr())) + " reports the previous tierIndex is ") + (prevTierIndex())) + " while actually it is ") + currentTierIndex));
                } 
            } else {
                break;
            }
        }
    }

    public void checkAccessingFromOwnerThread() {
        if ((owner) != (Thread.currentThread())) {
            throw new ConcurrentModificationException(((this.h().toIdentityString()) + ": Context shouldn\'t be accessed from multiple threads"));
        } 
    }

    public void closeOwnerThreadHolderCheckAccessingFromOwnerThreadDependants() {
        this.closeIterationCheckOnEachPublicOperationCheckOnEachLockOperationDependants();
    }

    public void checkOnEachLockOperation() {
        this.checkAccessingFromOwnerThread();
    }

    public void closeIterationCheckOnEachPublicOperationCheckOnEachLockOperationDependants() {
        this.innerReadLock.closeReadLockLockDependants();
        this.closeIterationCheckOnEachPublicOperationCheckOnEachPublicOperationDependants();
    }

    public void checkEntryNotRemovedOnThisIteration() {
        throwExceptionIfClosed();
        if (entryRemovedOnThisIterationInit()) {
            throw new IllegalStateException(((this.h().toIdentityString()) + ": Entry was already removed on this iteration"));
        } 
    }

    public void closeMapSegmentIterationCheckEntryNotRemovedOnThisIterationDependants() {
        this.closeIterationCheckOnEachPublicOperationCheckOnEachPublicOperationDependants();
    }

    public void checkOnEachPublicOperation() {
        _CheckOnEachPublicOperation_checkOnEachPublicOperation();
        this.checkEntryNotRemovedOnThisIteration();
    }

    public void closeIterationCheckOnEachPublicOperationCheckOnEachPublicOperationDependants() {
        this.entryValue.closeEntryValueBytesDataSizeDependants();
        this.entryKey.closeEntryKeyBytesDataSizeDependants();
    }

    @Override
    public R replaceValue(@NotNull
    MapEntry<K, V> entry, Data<V> newValue) {
        this.checkOnEachPublicOperation();
        return this.m().entryOperations.replaceValue(entry, newValue);
    }

    @NotNull
    @Override
    public Data<V> value() {
        this.checkOnEachPublicOperation();
        return this.entryValue;
    }

    @Override
    public Data<V> wrapValueAsData(V value) {
        this.checkOnEachPublicOperation();
        WrappedValueInstanceDataHolder wrapped = this.wrappedValueInstanceDataHolder;
        wrapped = wrapped.getUnusedWrappedValueHolderGuarded();
        wrapped.initValue(value);
        return wrapped.wrappedData();
    }

    @Override
    public Data<V> defaultValue(@NotNull
    MapAbsentEntry<K, V> absentEntry) {
        this.checkOnEachPublicOperation();
        return this.m().defaultValueProvider.defaultValue(absentEntry);
    }

    @NotNull
    @Override
    public InterProcessLock writeLock() {
        // The write-lock is final and thread-safe
        return this.innerWriteLock;
    }

    @Override
    public Data<V> wrapValueBytesAsData(BytesStore<?, ?> bytesStore, long offset, long size) {
        Objects.requireNonNull(bytesStore);
        this.checkOnEachPublicOperation();
        WrappedValueBytesData wrapped = this.wrappedValueBytesData;
        wrapped = wrapped.getUnusedWrappedValueBytesDataGuarded();
        wrapped.initWrappedValueBytesStore(bytesStore, offset, size);
        return wrapped;
    }

    @Override
    public R remove(@NotNull
    MapEntry<K, V> entry) {
        this.checkOnEachPublicOperation();
        return this.m().entryOperations.remove(entry);
    }

    @NotNull
    @Override
    public Data<K> key() {
        this.checkOnEachPublicOperation();
        return this.entryKey;
    }

    private int checkEntry(long searchKey, long entryPos, int segmentIndex, ChronicleHashCorruption.Listener corruptionListener, ChronicleHashCorruptionImpl corruption) {
        VanillaChronicleHash<K, ?, ?, ?> h = this.h();
        if ((entryPos < 0) || (entryPos >= (h.actualChunksPerSegmentTier))) {
            ChronicleHashCorruptionImpl.report(corruptionListener, corruption, segmentIndex, () -> ChronicleHashCorruptionImpl.format("Entry pos is out of range: {}, should be 0-{}", entryPos, ((h.actualChunksPerSegmentTier) - 1)));
            return -1;
        } 
        try {
            this.readExistingEntry(entryPos);
        } catch (Exception e) {
            ChronicleHashCorruptionImpl.reportException(corruptionListener, corruption, segmentIndex, () -> "Exception while reading entry key size", e);
            return -1;
        }
        if ((this.keyEnd()) > (this.segmentBytes().capacity())) {
            ChronicleHashCorruptionImpl.report(corruptionListener, corruption, segmentIndex, () -> ChronicleHashCorruptionImpl.format(("Wrong key size: " + (this.keySize()))));
            return -1;
        } 
        long keyHashCode = this.keyHashCode();
        int segmentIndexFromKey = h.hashSplitting.segmentIndex(keyHashCode);
        if ((segmentIndexFromKey < 0) || (segmentIndexFromKey >= (h.actualSegments))) {
            ChronicleHashCorruptionImpl.report(corruptionListener, corruption, segmentIndex, () -> ChronicleHashCorruptionImpl.format(("Segment index from the entry key hash code is out of range: {}, " + "should be 0-{}, entry key: {}"), segmentIndexFromKey, ((h.actualSegments) - 1), this.key()));
            return -1;
        } 
        long segmentHashFromKey = h.hashSplitting.segmentHash(keyHashCode);
        long searchKeyFromKey = h.hashLookup.maskUnsetKey(segmentHashFromKey);
        if (searchKey != searchKeyFromKey) {
            ChronicleHashCorruptionImpl.report(corruptionListener, corruption, segmentIndex, () -> ChronicleHashCorruptionImpl.format(("HashLookup searchKey: {}, HashLookup searchKey " + "from the entry key hash code: {}, entry key: {}, entry pos: {}"), searchKey, searchKeyFromKey, this.key(), entryPos));
            return -1;
        } 
        try {
            long entryAndChecksumEnd = (this.entryEnd()) + (this.checksumStrategy.extraEntryBytes());
            if (entryAndChecksumEnd > (this.segmentBytes().capacity())) {
                ChronicleHashCorruptionImpl.report(corruptionListener, corruption, segmentIndex, () -> ChronicleHashCorruptionImpl.format(("Wrong value size: {}, key: " + (this.valueSize())), this.key()));
                return -1;
            } 
        } catch (Exception ex) {
            ChronicleHashCorruptionImpl.reportException(corruptionListener, corruption, segmentIndex, () -> "Exception while reading entry value size, key: " + (this.key()), ex);
            return -1;
        }
        int storedChecksum = this.checksumStrategy.storedChecksum();
        int checksumFromEntry = this.checksumStrategy.computeChecksum();
        if (storedChecksum != checksumFromEntry) {
            ChronicleHashCorruptionImpl.report(corruptionListener, corruption, segmentIndex, () -> ChronicleHashCorruptionImpl.format(("Checksum doesn\'t match, stored: {}, should be from " + "the entry bytes: {}, key: {}, value: {}"), storedChecksum, checksumFromEntry, this.key(), this.value()));
            return -1;
        } 
        if (!(this.freeList().isRangeClear(entryPos, (entryPos + (this.entrySizeInChunks()))))) {
            ChronicleHashCorruptionImpl.report(corruptionListener, corruption, segmentIndex, () -> ChronicleHashCorruptionImpl.format("Overlapping entry: positions {}-{}, key: {}, value: {}", entryPos, ((entryPos + (this.entrySizeInChunks())) - 1), this.key(), this.value()));
            return -1;
        } 
        if (segmentIndex < 0) {
            return segmentIndexFromKey;
        } else {
            if (segmentIndex != segmentIndexFromKey) {
                ChronicleHashCorruptionImpl.report(corruptionListener, corruption, segmentIndex, () -> ChronicleHashCorruptionImpl.format(("Expected segment index: {}, segment index from the entry key: {}, " + "key: {}, value: {}"), segmentIndex, searchKeyFromKey, this.key(), this.value()));
                return -1;
            } else {
                return segmentIndex;
            }
        }
    }

    public void removeDuplicatesInSegment(ChronicleHashCorruption.Listener corruptionListener, ChronicleHashCorruptionImpl corruption) {
        long startHlPos = 0L;
        VanillaChronicleMap<K, ?, ?> m = this.m();
        CompactOffHeapLinearHashTable hashLookup = m.hashLookup;
        long currentTierBaseAddr = this.tierBaseAddr();
        while (!(hashLookup.empty(hashLookup.readEntry(currentTierBaseAddr, startHlPos)))) {
            startHlPos = hashLookup.step(startHlPos);
        }
        long hlPos = startHlPos;
        int steps = 0;
        long entries = 0;
        tierIteration : do {
            hlPos = hashLookup.step(hlPos);
            steps++;
            long entry = hashLookup.readEntry(currentTierBaseAddr, hlPos);
            if (!(hashLookup.empty(entry))) {
                this.readExistingEntry(hashLookup.value(entry));
                Data<K> key = this.key();
                try (ExternalMapQueryContext<?, ?, ?> c = m.queryContext(key)) {
                    MapEntry<?, ?> entry2 = c.entry();
                    Data<?> key2 = ((MapEntry)(c)).key();
                    long keyAddress = key.bytes().addressForRead(key.offset());
                    long key2Address = key2.bytes().addressForRead(key2.offset());
                    if (key2Address != keyAddress) {
                        ChronicleHashCorruptionImpl.report(corruptionListener, corruption, this.segmentIndex(), () -> ChronicleHashCorruptionImpl.format(("entries with duplicate key {} in segment {}: " + "with values {} and {}, removing the latter"), key, c.segmentIndex(), (entry2 != null ? ((MapEntry)(c)).value() : "<deleted>"), (!(this.entryDeleted()) ? this.value() : "<deleted>")));
                        if ((hashLookup.remove(currentTierBaseAddr, hlPos)) != hlPos) {
                            hlPos = hashLookup.stepBack(hlPos);
                            steps--;
                        } 
                        continue tierIteration;
                    } 
                }
                entries++;
            } 
        } while ((hlPos != startHlPos) || (steps == 0) );
        recoverTierEntriesCounter(entries, corruptionListener, corruption);
        recoverLowestPossibleFreeChunkTiered(corruptionListener, corruption);
    }

    private void removeDuplicatesInSegments(ChronicleHashCorruption.Listener corruptionListener, ChronicleHashCorruptionImpl corruption) {
        VanillaChronicleHash<K, ?, ?, ?> h = this.h();
        for (int segmentIndex = 0 ; segmentIndex < (h.actualSegments) ; segmentIndex++) {
            this.initSegmentIndex(segmentIndex);
            this.initSegmentTier();
            this.goToLastTier();
            while (true) {
                this.removeDuplicatesInSegment(corruptionListener, corruption);
                if ((this.tier()) > 0) {
                    this.prevTier();
                } else {
                    break;
                }
            }
        }
    }

    public int recoverTier(int segmentIndex, ChronicleHashCorruption.Listener corruptionListener, ChronicleHashCorruptionImpl corruption) {
        this.freeList().clearAll();
        VanillaChronicleHash<K, ?, ?, ?> h = this.h();
        CompactOffHeapLinearHashTable hl = h.hashLookup;
        long hlAddr = this.tierBaseAddr();
        long validEntries = 0;
        long hlPos = 0;
        do {
            long hlEntry = hl.readEntry(hlAddr, hlPos);
            nextHlPos : if (!(hl.empty(hlEntry))) {
                hl.clearEntry(hlAddr, hlPos);
                if (validEntries >= (h.maxEntriesPerHashLookup)) {
                    ChronicleHashCorruptionImpl.report(corruptionListener, corruption, segmentIndex, () -> ChronicleHashCorruptionImpl.format("Too many entries in tier with index {}, max is {}", this.tierIndex(), h.maxEntriesPerHashLookup));
                    break nextHlPos;
                } 
                long searchKey = hl.key(hlEntry);
                long entryPos = hl.value(hlEntry);
                int si = checkEntry(searchKey, entryPos, segmentIndex, corruptionListener, corruption);
                if (si < 0) {
                    break nextHlPos;
                } else {
                    this.freeList().setRange(entryPos, (entryPos + (this.entrySizeInChunks())));
                    segmentIndex = si;
                }
                long startInsertPos = hl.hlPos(searchKey);
                long insertPos = startInsertPos;
                do {
                    long hlInsertEntry = hl.readEntry(hlAddr, insertPos);
                    if (hl.empty(hlInsertEntry)) {
                        hl.writeEntry(hlAddr, insertPos, hl.entry(searchKey, entryPos));
                        validEntries++;
                        break nextHlPos;
                    } 
                    if (insertPos == hlPos) {
                        throw new ChronicleHashRecoveryFailedException((("Concurrent modification of " + (h.toIdentityString())) + " while recovery procedure is in progress"));
                    } 
                    checkDuplicateKeys : if ((hl.key(hlInsertEntry)) == searchKey) {
                        long anotherEntryPos = hl.value(hlInsertEntry);
                        if (anotherEntryPos == entryPos) {
                            validEntries++;
                            break nextHlPos;
                        } 
                        long currentKeyOffset = this.keyOffset();
                        long currentKeySize = this.keySize();
                        int currentEntrySizeInChunks = this.entrySizeInChunks();
                        if ((insertPos >= 0) && (insertPos < hlPos)) {
                            this.readExistingEntry(anotherEntryPos);
                        } else if ((checkEntry(searchKey, anotherEntryPos, segmentIndex, corruptionListener, corruption)) < 0) {
                            break checkDuplicateKeys;
                        } 
                        if (((this.keySize()) == currentKeySize) && (BytesUtil.bytesEqual(this.segmentBS(), currentKeyOffset, this.segmentBS(), this.keyOffset(), currentKeySize))) {
                            ChronicleHashCorruptionImpl.report(corruptionListener, corruption, segmentIndex, () -> ChronicleHashCorruptionImpl.format(("Entries with duplicate keys within a tier: " + "at pos {} and {} with key {}, first value is {}"), entryPos, anotherEntryPos, this.key(), this.value()));
                            this.freeList().clearRange(entryPos, (entryPos + currentEntrySizeInChunks));
                            break nextHlPos;
                        } 
                    } 
                    insertPos = hl.step(insertPos);
                } while (insertPos != startInsertPos );
                throw new ChronicleHashRecoveryFailedException(((("HashLookup overflow should never occur. " + "It might also be concurrent access to ") + (h.toIdentityString())) + " while recovery procedure is in progress"));
            } 
            hlPos = hl.step(hlPos);
        } while (hlPos != 0 );
        shiftHashLookupEntries();
        return segmentIndex;
    }

    @Override
    public void recoverSegments(ChronicleHashCorruption.Listener corruptionListener, ChronicleHashCorruptionImpl corruption) {
        throwExceptionIfClosed();
        VanillaChronicleHash<K, ?, ?, ?> h = this.h();
        for (int segmentIndex = 0 ; segmentIndex < (h.actualSegments) ; segmentIndex++) {
            this.initSegmentIndex(segmentIndex);
            resetSegmentLock(corruptionListener, corruption);
            zeroOutFirstSegmentTierCountersArea(corruptionListener, corruption);
            this.recoverTier(segmentIndex, corruptionListener, corruption);
        }
        VanillaGlobalMutableState globalMutableState = h.globalMutableState();
        long storedExtraTiersInUse = globalMutableState.getExtraTiersInUse();
        long allocatedExtraTiers = (globalMutableState.getAllocatedExtraTierBulks()) * (h.tiersInBulk);
        long expectedExtraTiersInUse = Math.max(0, Math.min(storedExtraTiersInUse, allocatedExtraTiers));
        long actualExtraTiersInUse = 0;
        long firstFreeExtraTierIndex = -1;
        for (long extraTierIndex = 0 ; extraTierIndex < expectedExtraTiersInUse ; extraTierIndex++) {
            long tierIndex = h.extraTierIndexToTierIndex(extraTierIndex);
            this.initSegmentTier(0, tierIndex);
            int segmentIndex = this.recoverTier(-1, corruptionListener, corruption);
            if (segmentIndex >= 0) {
                long tierCountersAreaAddr = this.tierCountersAreaAddr();
                int storedSegmentIndex = TierCountersArea.segmentIndex(tierCountersAreaAddr);
                if (storedSegmentIndex != segmentIndex) {
                    ChronicleHashCorruptionImpl.report(corruptionListener, corruption, segmentIndex, () -> ChronicleHashCorruptionImpl.format(("wrong segment index stored in tier counters area " + "of tier with index {}: {}, should be, based on entries: {}"), tierIndex, storedSegmentIndex, segmentIndex));
                    TierCountersArea.segmentIndex(tierCountersAreaAddr, segmentIndex);
                } 
                TierCountersArea.nextTierIndex(tierCountersAreaAddr, 0);
                this.initSegmentIndex(segmentIndex);
                this.goToLastTier();
                this.nextTierIndex(tierIndex);
                TierCountersArea.prevTierIndex(tierCountersAreaAddr, this.tierIndex());
                TierCountersArea.tier(tierCountersAreaAddr, ((this.tier()) + 1));
                actualExtraTiersInUse = extraTierIndex + 1;
            } else {
                firstFreeExtraTierIndex = extraTierIndex;
                break;
            }
        }
        if (storedExtraTiersInUse != actualExtraTiersInUse) {
            long finalActualExtraTiersInUse = actualExtraTiersInUse;
            ChronicleHashCorruptionImpl.report(corruptionListener, corruption, -1, () -> ChronicleHashCorruptionImpl.format((("wrong number of actual tiers in use in global mutable state, stored: {}, " + "should be: ") + storedExtraTiersInUse), finalActualExtraTiersInUse));
            globalMutableState.setExtraTiersInUse(actualExtraTiersInUse);
        } 
        long firstFreeTierIndex;
        if (firstFreeExtraTierIndex == (-1)) {
            if (allocatedExtraTiers > expectedExtraTiersInUse) {
                firstFreeTierIndex = h.extraTierIndexToTierIndex(expectedExtraTiersInUse);
            } else {
                firstFreeTierIndex = 0;
            }
        } else {
            firstFreeTierIndex = h.extraTierIndexToTierIndex(firstFreeExtraTierIndex);
        }
        if (firstFreeTierIndex > 0) {
            long lastTierIndex = h.extraTierIndexToTierIndex((allocatedExtraTiers - 1));
            h.linkAndZeroOutFreeTiers(firstFreeTierIndex, lastTierIndex);
        } 
        long storedFirstFreeTierIndex = globalMutableState.getFirstFreeTierIndex();
        if (storedFirstFreeTierIndex != firstFreeTierIndex) {
            ChronicleHashCorruptionImpl.report(corruptionListener, corruption, -1, () -> ChronicleHashCorruptionImpl.format((("wrong first free tier index in global mutable state, stored: {}, " + "should be: ") + storedFirstFreeTierIndex), firstFreeTierIndex));
            globalMutableState.setFirstFreeTierIndex(firstFreeTierIndex);
        } 
        removeDuplicatesInSegments(corruptionListener, corruption);
    }

    @NotNull
    @Override
    public InterProcessLock readLock() {
        this.checkOnEachPublicOperation();
        return this.innerReadLock;
    }

    @NotNull
    @Override
    public InterProcessLock updateLock() {
        this.checkOnEachPublicOperation();
        return this.innerUpdateLock;
    }

    @Override
    public R insert(@NotNull
    MapAbsentEntry<K, V> absentEntry, Data<V> value) {
        this.checkOnEachPublicOperation();
        return this.m().entryOperations.insert(absentEntry, value);
    }

    public boolean used;

    private boolean firstContextLockedInThisThread;

    @Override
    public boolean usedInit() {
        return used;
    }

    @Override
    public void initUsed(boolean used, VanillaChronicleMap map) {
        boolean wasUsedInit = this.usedInit();
        assert used;
        firstContextLockedInThisThread = rootContextInThisThread.lockContextLocally(map);
        initMap(map);
        this.used = true;
        if (wasUsedInit)
            this.closeUsedDependants();
        
    }

    public boolean used() {
        assert this.usedInit() : "Used should be init";
        return this.used;
    }

    @SuppressWarnings(value = "unused")
    void closeUsed() {
        if (!(this.usedInit()))
            return ;
        
        this.closeUsedDependants();
        used = false;
        if (firstContextLockedInThisThread)
            rootContextInThisThread.unlockContextLocally();
        
    }

    public void closeUsedDependants() {
        this.closeLocks();
    }

    int totalReadLockCount;

    int totalUpdateLockCount;

    int totalWriteLockCount;

    public int latestSameThreadSegmentModCount;

    public int contextModCount;

    public boolean nestedContextsLockedOnSameSegment;

    LocksInterface nextNode;

    LocalLockState localLockState;

    public LocksInterface rootContextLockedOnThisSegment = null;

    public boolean locksInit() {
        return (this.rootContextLockedOnThisSegment) != null;
    }

    void initLocks() {
        boolean wasLocksInit = this.locksInit();
        assert this.used();
        if ((segmentHeader()) == null)
            throw new AssertionError();
        
        localLockState = LocalLockState.UNLOCKED;
        int indexOfThisContext = this.indexInContextChain;
        for (int i = indexOfThisContext - 1 ; i >= 0 ; i--) {
            if (tryFindInitLocksOfThisSegment(i))
                return ;
            
        }
        for (int i = indexOfThisContext + 1, size = this.contextChain.size() ; i < size ; i++) {
            if (tryFindInitLocksOfThisSegment(i))
                return ;
            
        }
        rootContextLockedOnThisSegment = this;
        nestedContextsLockedOnSameSegment = false;
        latestSameThreadSegmentModCount = 0;
        contextModCount = 0;
        totalReadLockCount = 0;
        totalUpdateLockCount = 0;
        totalWriteLockCount = 0;
        if (wasLocksInit)
            this.closeLocksDependants();
        
    }

    public int latestSameThreadSegmentModCount() {
        if (!(this.locksInit()))
            this.initLocks();
        
        return this.latestSameThreadSegmentModCount;
    }

    public int totalReadLockCount() {
        if (!(this.locksInit()))
            this.initLocks();
        
        return this.totalReadLockCount;
    }

    public int totalUpdateLockCount() {
        if (!(this.locksInit()))
            this.initLocks();
        
        return this.totalUpdateLockCount;
    }

    public int totalWriteLockCount() {
        if (!(this.locksInit()))
            this.initLocks();
        
        return this.totalWriteLockCount;
    }

    public LocalLockState localLockState() {
        if (!(this.locksInit()))
            this.initLocks();
        
        return this.localLockState;
    }

    public LocksInterface nextNode() {
        if (!(this.locksInit()))
            this.initLocks();
        
        return this.nextNode;
    }

    public LocksInterface rootContextLockedOnThisSegment() {
        if (!(this.locksInit()))
            this.initLocks();
        
        return this.rootContextLockedOnThisSegment;
    }

    void closeLocks() {
        if (!(this.locksInit()))
            return ;
        
        this.closeLocksDependants();
        if ((rootContextLockedOnThisSegment) == (this)) {
            closeRootLocks();
        } else {
            closeNestedLocks();
        }
        deregisterIterationContextLockedInThisThread();
        localLockState = null;
        rootContextLockedOnThisSegment = null;
    }

    public void closeLocksDependants() {
        this.innerReadLock.closeReadLockLockDependants();
        this.closeDelayedUpdateChecksum();
    }

    public long hashLookupPos = -1;

    public boolean hashLookupPosInit() {
        return (this.hashLookupPos) != (-1);
    }

    public void initHashLookupPos() {
        boolean wasHashLookupPosInit = this.hashLookupPosInit();
        if ((this.tier()) < 0)
            throw new AssertionError();
        
        this.innerReadLock.lock();
        this.hashLookupPos = this.searchStartPos();
        if (wasHashLookupPosInit)
            this.closeHashLookupPosDependants();
        
    }

    public void initHashLookupPos(long hashLookupPos) {
        boolean wasHashLookupPosInit = this.hashLookupPosInit();
        this.hashLookupPos = hashLookupPos;
        if (wasHashLookupPosInit)
            this.closeHashLookupPosDependants();
        
    }

    public long hashLookupPos() {
        if (!(this.hashLookupPosInit()))
            this.initHashLookupPos();
        
        return this.hashLookupPos;
    }

    public void closeHashLookupPos() {
        if (!(this.hashLookupPosInit()))
            return ;
        
        this.closeHashLookupPosDependants();
        this.hashLookupPos = -1;
    }

    public void closeHashLookupPosDependants() {
        this.closeHashLookupSearchNextPosDependants();
        this.closeHashLookupSearchFoundDependants();
    }

    public long nextPos() {
        long pos = this.hashLookupPos();
        CompactOffHeapLinearHashTable hl = hl();
        while (true) {
            long entry = hl.readEntryVolatile(addr(), pos);
            if (hl.empty(entry)) {
                this.setHashLookupPosGuarded(pos);
                return -1L;
            } 
            pos = hl.step(pos);
            if (pos == (searchStartPos()))
                break;
            
            if ((hl.key(entry)) == (searchKey())) {
                this.setHashLookupPosGuarded(pos);
                return hl.value(entry);
            } 
        }
        throw new IllegalStateException(((this.h().toIdentityString()) + ": HashLookup overflow should never occur"));
    }

    public void closeHashLookupSearchNextPosDependants() {
        this.closeKeySearch();
    }

    public void found() {
        this.setHashLookupPosGuarded(hl().stepBack(this.hashLookupPos()));
    }

    public void closeHashLookupSearchFoundDependants() {
        this.closeKeySearch();
    }

    protected CompiledMapIterationContext.SearchState searchState = null;

    public boolean keySearchInit() {
        return (this.searchState) != null;
    }

    public void initKeySearch() {
        for (long pos ; (pos = this.nextPos()) >= 0L ; ) {
            if (inputKeyInit()) {
                long keySizeOffset = (this.entrySpaceOffset()) + (pos * (this.m().chunkSize));
                Bytes segmentBytes = this.segmentBytesForReadGuarded();
                segmentBytes.readPosition(keySizeOffset);
                long keySize = this.h().keySizeMarshaller.readSize(segmentBytes);
                long keyOffset = segmentBytes.readPosition();
                if (!(keyEquals(keySize, keyOffset)))
                    continue;
                
                this.found();
                this.readFoundEntry(pos, keySizeOffset, keySize, keyOffset);
                searchState = CompiledMapIterationContext.SearchState.PRESENT;
                return ;
            } 
        }
        searchState = CompiledMapIterationContext.SearchState.ABSENT;
    }

    public CompiledMapIterationContext.SearchState searchState() {
        if (!(this.keySearchInit()))
            this.initKeySearch();
        
        return this.searchState;
    }

    public void closeKeySearch() {
        this.searchState = null;
    }

    public boolean searchStateAbsent() {
        return (searchState()) == (CompiledMapIterationContext.SearchState.ABSENT);
    }

    public boolean searchStatePresent() {
        return (searchState()) == (CompiledMapIterationContext.SearchState.PRESENT);
    }

    public void putNewVolatile(long entryPos) {
        boolean keySearchReInit = !(this.keySearchInit());
        if (this.searchStatePresent())
            throw new AssertionError();
        
        if (keySearchReInit) {
            this.readExistingEntry(entryPos);
        } 
        hl().checkValueForPut(entryPos);
        hl().writeEntryVolatile(addr(), this.hashLookupPos(), searchKey(), entryPos);
    }

    public boolean checkSlotContainsExpectedKeyAndValue(long value) {
        long entry = hl().readEntry(addr(), this.hashLookupPos());
        return ((hl().key(entry)) == (searchKey())) && ((hl().value(entry)) == value);
    }

    public void remove() {
        this.setHashLookupPosGuarded(hl().remove(addr(), this.hashLookupPos()));
    }

    @Override
    public String debugLocksState() {
        String s = (this) + ": ";
        if (!(this.usedInit())) {
            s += "unused";
            return s;
        } 
        s += "used, ";
        if (!(segmentIndexInit())) {
            s += "segment uninitialized";
            return s;
        } 
        s += ("segment " + (segmentIndex())) + ", ";
        if (!(locksInit())) {
            s += "locks uninitialized";
            return s;
        } 
        s += ("local state: " + (localLockState())) + ", ";
        s += ("read lock count: " + (rootContextLockedOnThisSegment().totalReadLockCount())) + ", ";
        s += ("update lock count: " + (rootContextLockedOnThisSegment().totalUpdateLockCount())) + ", ";
        s += "write lock count: " + (rootContextLockedOnThisSegment().totalWriteLockCount());
        return s;
    }

    @Override
    public boolean checkSum() {
        this.checkOnEachPublicOperation();
        if (!(this.h().checksumEntries)) {
            throw new UnsupportedOperationException(((this.h().toIdentityString()) + ": Checksum is not stored in this Chronicle Hash"));
        } 
        this.innerUpdateLock.lock();
        return (delayedUpdateChecksumInit()) || (checksumStrategy.innerCheckSum());
    }

    @Override
    public void updateChecksum() {
        this.checkOnEachPublicOperation();
        if (!(this.h().checksumEntries)) {
            throw new UnsupportedOperationException(((this.h().toIdentityString()) + ": Checksum is not stored in this Chronicle Hash"));
        } 
        this.innerUpdateLock.lock();
        initDelayedUpdateChecksum(true);
    }

    public <T>boolean forEachTierEntryWhile(Predicate<? super T> predicate, int currentTier, long currentTierBaseAddr, long tierIndex) {
        long leftEntries = tierEntriesForIteration();
        boolean interrupted = false;
        long startPos = 0L;
        CompactOffHeapLinearHashTable hashLookup = this.h().hashLookup;
        while (!(hashLookup.empty(hashLookup.readEntry(currentTierBaseAddr, startPos)))) {
            startPos = hashLookup.step(startPos);
        }
        this.initHashLookupPos(startPos);
        long currentHashLookupPos;
        int steps = 0;
        do {
            currentHashLookupPos = hashLookup.step(this.hashLookupPos());
            steps++;
            this.setHashLookupPosGuarded(currentHashLookupPos);
            long entry = hashLookup.readEntry(currentTierBaseAddr, currentHashLookupPos);
            initHashLookupEntry(entry);
            if (!(hashLookup.empty(entry))) {
                this.readExistingEntry(hashLookup.value(entry));
                if (shouldTestEntry()) {
                    initEntryRemovedOnThisIteration(false);
                    try {
                        if (!(predicate.test(((T)(entryForIteration()))))) {
                            interrupted = true;
                            break;
                        } else {
                            if ((--leftEntries) == 0)
                                break;
                            
                        }
                    } finally {
                        hookAfterEachIteration();
                        if ((this.tier()) != currentTier) {
                            this.initSegmentTier_WithBaseAddr(currentTier, currentTierBaseAddr, tierIndex);
                            currentHashLookupPos = hashLookup.stepBack(currentHashLookupPos);
                            steps--;
                            this.initHashLookupPos(currentHashLookupPos);
                        } 
                        this.innerWriteLock.unlock();
                        this.closeKeyOffset();
                    }
                } 
            } 
        } while ((currentHashLookupPos != startPos) || (steps == 0) );
        if ((!interrupted) && (leftEntries > 0)) {
            throw new IllegalStateException((((((this.h().toIdentityString()) + ": We went through a tier without interruption, ") + "but according to tier counters there should be ") + leftEntries) + " more entries. Size diverged?"));
        } 
        return interrupted;
    }

    public <T>boolean innerForEachSegmentEntryWhile(Predicate<? super T> predicate) {
        try {
            this.goToLastTier();
            while (true) {
                int currentTier = this.tier();
                long currentTierBaseAddr = this.tierBaseAddr();
                long currentTierIndex = this.tierIndex();
                boolean interrupted = forEachTierEntryWhile(predicate, currentTier, currentTierBaseAddr, currentTierIndex);
                if (interrupted)
                    return false;
                
                if (currentTier == 0)
                    return true;
                
                this.prevTier();
            }
        } finally {
            closeHashLookupEntry();
            this.innerReadLock.unlock();
            initEntryRemovedOnThisIteration(false);
        }
    }

    @Override
    public boolean forEachSegmentEntryWhile(Predicate<? super MapEntry<K, V>> predicate) {
        throwExceptionIfClosed();
        this.checkOnEachPublicOperation();
        this.innerUpdateLock.lock();
        return innerForEachSegmentEntryWhile(predicate);
    }

    @Override
    public void forEachSegmentEntry(Consumer<? super MapEntry<K, V>> action) {
        throwExceptionIfClosed();
        forEachSegmentEntryWhile((MapEntry<K, V> e) -> {
            action.accept(e);
            return true;
        });
    }

    protected void relocation(Data<V> newValue, long newEntrySize) {
        long oldHashLookupPos = this.hashLookupPos();
        long oldHashLookupAddr = this.tierBaseAddr();
        boolean tierHasChanged = this.initEntryAndKeyCopying(newEntrySize, ((valueSizeOffset()) - (keySizeOffset())), pos(), entrySizeInChunks());
        if (tierHasChanged) {
            if (!(this.searchStateAbsent()))
                throw new AssertionError();
            
        } 
        initValue(newValue);
        freeExtraAllocatedChunks();
        CompactOffHeapLinearHashTable hl = this.h().hashLookup;
        long hashLookupKey = hl.key(hl.readEntry(oldHashLookupAddr, oldHashLookupPos));
        hl.checkValueForPut(pos());
        hl.writeEntryVolatile(this.tierBaseAddr(), this.hashLookupPos(), hashLookupKey, pos());
        this.innerWriteLock.lock();
        if (tierHasChanged)
            hl.remove(oldHashLookupAddr, oldHashLookupPos);
        
    }

    public void innerDefaultReplaceValue(Data<V> newValue) {
        assert this.innerUpdateLock.isHeldByCurrentThread();
        boolean newValueSizeIsDifferent = (newValue.size()) != (this.valueSize());
        if (newValueSizeIsDifferent) {
            long newSizeOfEverythingBeforeValue = newSizeOfEverythingBeforeValue(newValue);
            long entryStartOffset = keySizeOffset();
            VanillaChronicleMap<K, ?, ?> m = this.m();
            long newValueOffset = VanillaChronicleMap.alignAddr((entryStartOffset + newSizeOfEverythingBeforeValue), this.m().alignment);
            long newEntrySize = newEntrySize(newValue, entryStartOffset, newValueOffset);
            int newSizeInChunks = m.inChunks(newEntrySize);
            newValueDoesNotFit : if (newSizeInChunks > (entrySizeInChunks())) {
                if (newSizeInChunks > (m.maxChunksPerEntry)) {
                    throw new IllegalArgumentException(((((((m.toIdentityString()) + ": Value too large: entry takes ") + newSizeInChunks) + " chunks, ") + (m.maxChunksPerEntry)) + " is maximum."));
                } 
                if (this.reallocGuarded(pos(), entrySizeInChunks(), newSizeInChunks)) {
                    break newValueDoesNotFit;
                } 
                relocation(newValue, newEntrySize);
                return ;
            } else if (newSizeInChunks < (entrySizeInChunks())) {
                this.freeExtraGuarded(pos(), entrySizeInChunks(), newSizeInChunks);
            } 
        }
        this.innerWriteLock.lock();
        if (newValueSizeIsDifferent) {
            initValue(newValue);
        } else {
            writeValue(newValue);
        }
    }

    @Override
    public void doReplaceValue(Data<V> newValue) {
        throwExceptionIfClosed();
        this.checkOnEachPublicOperation();
        try {
            this.innerDefaultReplaceValue(newValue);
        } finally {
            this.innerWriteLock.unlock();
        }
    }

    public void innerRemoveEntryExceptHashLookupUpdate() {
        this.freeGuarded(pos(), entrySizeInChunks());
        this.incrementModCountGuarded();
    }

    public void iterationRemove() {
        throwExceptionIfClosed();
        if ((this.h().hashLookup.remove(this.tierBaseAddr(), this.hashLookupPos())) != (this.hashLookupPos())) {
            this.setHashLookupPosGuarded(this.h().hashLookup.stepBack(this.hashLookupPos()));
        } 
        this.innerRemoveEntryExceptHashLookupUpdate();
    }

    @Override
    public void doRemove() {
        throwExceptionIfClosed();
        this.checkOnEachPublicOperation();
        this.innerWriteLock.lock();
        try {
            iterationRemove();
        } finally {
            this.innerWriteLock.unlock();
        }
        initEntryRemovedOnThisIteration(true);
    }

    public boolean delayedUpdateChecksum = false;

    boolean delayedUpdateChecksumInit() {
        return this.delayedUpdateChecksum;
    }

    public void initDelayedUpdateChecksum(boolean delayedUpdateChecksum) {
        assert (entryOffsetInit()) && ((keySizeOffset()) >= 0);
        assert (this.locksInit()) && ((this.localLockState()) != (LocalLockState.UNLOCKED));
        assert delayedUpdateChecksum;
        this.delayedUpdateChecksum = true;
    }

    public void closeDelayedUpdateChecksum() {
        if (!(this.delayedUpdateChecksumInit()))
            return ;
        
        if (this.h().checksumEntries)
            this.hashEntryChecksumStrategy.computeAndStoreChecksum();
        
        delayedUpdateChecksum = false;
    }
}