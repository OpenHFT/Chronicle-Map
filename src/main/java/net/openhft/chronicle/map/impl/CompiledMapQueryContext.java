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
import net.openhft.chronicle.map.impl.ret.InstanceReturnValue;
import net.openhft.chronicle.map.impl.ret.UsableReturnValue;
import net.openhft.chronicle.map.impl.stage.data.ZeroBytesStore;
import net.openhft.chronicle.map.impl.stage.query.Absent;
import net.openhft.chronicle.map.impl.stage.query.MapAndSetContext;
import net.openhft.chronicle.set.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import static net.openhft.chronicle.hash.impl.LocalLockState.UNLOCKED;

/**
 * Generated code
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class CompiledMapQueryContext<K, V, R> extends ChainingInterface implements AutoCloseable , ChecksumEntry , HashEntry<K> , SegmentLock , Alloc , KeyHashCode , LocksInterface , ExternalMapQueryContext<K, V, R> , MapContext<K, V, R> , MapEntry<K, V> , QueryContextInterface<K, V, R> , VanillaChronicleMapHolder<K, V, R> , Absent<K, V> , MapAndSetContext<K, V, R> , ExternalSetQueryContext<K, R> , SetContext<K, R> , SetEntry<K> {
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
        this.entryKey.doCloseCachedEntryKey();
        this.entryValue.doCloseCachedEntryValue();
        this.doClosePresenceOfEntry();
        this.doCloseKeySearch();
        this.doCloseHashLookupPos();
        this.doCloseDelayedUpdateChecksum();
        this.doCloseLocks();
        this.doCloseUsed();
        this.usingReturnValue.doCloseReturnedValue();
        this.usingReturnValue.doCloseUsingReturnValue();
        this.doCloseEntrySizeInChunks();
        this.doCloseEntryOffset();
        this.doCloseValueSize();
        this.doCloseSegment();
        this.doCloseSegmentTier();
        this.doCloseSegmentHeader();
        this.doCloseSegmentIndex();
        this.doCloseSearchKey();
        this.wrappedValueInstanceDataHolder.doCloseWrappedData();
        this.wrappedValueInstanceDataHolder.doCloseValue();
        this.doCloseMap();
        this.defaultReturnValue.doCloseDefaultReturnedValue();
        this.doCloseAllocatedChunks();
        this.doCloseValueSizeOffset();
        this.doCloseKeyOffset();
        this.doClosePos();
        this.wrappedValueBytesData.doCloseCachedWrappedValue();
        this.wrappedValueBytesData.doCloseWrappedValueBytes();
        this.wrappedValueBytesData.doCloseWrappedValueBytesStore();
        this.wrappedValueBytesData.doCloseNext();
        this.wrappedValueInstanceDataHolder.doCloseNext();
        this.doCloseKeyHash();
        this.doCloseInputKey();
        this.inputKeyBytesData.doCloseCachedInputKey();
        this.inputKeyBytesData.doCloseInputKeyBytes();
        this.inputKeyBytesData.doCloseInputKeyBytesStore();
        this.doCloseInputValueDataAccess();
        this.doCloseKeySize();
        this.doCloseInputKeyDataAccess();
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

    public void doCloseEntrySizeInChunks() {
        this.entrySizeInChunks = 0;
    }

    public void doCloseHashLookupPos() {
        this.hashLookupPos = -1;
    }

    public void doCloseInputKey() {
        this.inputKey = null;
    }

    public void doCloseInputKeyDataAccess() {
        if (!(this.inputKeyDataAccessInit()))
            return ;
        
        innerInputKeyDataAccess.uninit();
        inputKeyDataAccessInitialized = false;
    }

    public void doCloseInputValueDataAccess() {
        if (!(this.inputValueDataAccessInit()))
            return ;
        
        innerInputValueDataAccess.uninit();
        inputValueDataAccessInitialized = false;
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

    public void doClosePresenceOfEntry() {
        this.entryPresence = null;
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

    public void setSearchStateGuarded(CompiledMapQueryContext.SearchState newSearchState) {
        if (!(this.keySearchInit()))
            this.initKeySearch();
        
        setSearchState(newSearchState);
    }

    public CompiledMapQueryContext(ChainingInterface rootContextInThisThread ,VanillaChronicleMap map) {
        contextChain = rootContextInThisThread.getContextChain();
        indexInContextChain = contextChain.size();
        contextChain.add(this);
        this.rootContextInThisThread = rootContextInThisThread;
        initMap(map);
        this.inputKeyBytesData = new InputKeyBytesData();
        this.innerInputValueDataAccess = this.m().valueDataAccess.copy();
        this.defaultReturnValue = new DefaultReturnValue();
        this.owner = Thread.currentThread();
        this.zeroValueData = new DummyValueZeroData();
        this.entryKey = new EntryKeyBytesData();
        this.usingReturnValue = new UsingReturnValue();
        this.valueReader = net.openhft.chronicle.hash.serialization.StatefulCopyable.copyIfNeeded(this.m().valueReader);
        this.acquireHandle = new AcquireHandle();
        this.entryValue = new EntryValueBytesData();
        this.wrappedValueInstanceDataHolder = new WrappedValueInstanceDataHolder();
        this.innerReadLock = new ReadLock();
        this.keyReader = net.openhft.chronicle.hash.serialization.StatefulCopyable.copyIfNeeded(this.h().keyReader);
        this.innerInputKeyDataAccess = this.h().keyDataAccess.copy();
        this.innerWriteLock = new WriteLock();
        this.wrappedValueBytesData = new WrappedValueBytesData();
        this.segmentBS = new PointerBytesStore();
        this.segmentBytes = CompiledMapQueryContext.unmonitoredVanillaBytes(segmentBS);
        this.hashEntryChecksumStrategy = new HashEntryChecksumStrategy();
        this.checksumStrategy = this.h().checksumEntries ? this.hashEntryChecksumStrategy : NoChecksumStrategy.INSTANCE;
        this.innerUpdateLock = new UpdateLock();
        this.freeList = new ReusableBitSet(new SingleThreadedFlatBitSetFrame(MemoryUnit.LONGS.align(this.h().actualChunksPerSegmentTier, MemoryUnit.BITS)) , Access.nativeAccess() , null , 0);
    }

    public CompiledMapQueryContext(VanillaChronicleMap map) {
        contextChain = new ArrayList<>();
        contextChain.add(this);
        indexInContextChain = 0;
        rootContextInThisThread = this;
        initMap(map);
        this.inputKeyBytesData = new InputKeyBytesData();
        this.innerInputValueDataAccess = this.m().valueDataAccess.copy();
        this.defaultReturnValue = new DefaultReturnValue();
        this.owner = Thread.currentThread();
        this.zeroValueData = new DummyValueZeroData();
        this.entryKey = new EntryKeyBytesData();
        this.usingReturnValue = new UsingReturnValue();
        this.valueReader = net.openhft.chronicle.hash.serialization.StatefulCopyable.copyIfNeeded(this.m().valueReader);
        this.acquireHandle = new AcquireHandle();
        this.entryValue = new EntryValueBytesData();
        this.wrappedValueInstanceDataHolder = new WrappedValueInstanceDataHolder();
        this.innerReadLock = new ReadLock();
        this.keyReader = net.openhft.chronicle.hash.serialization.StatefulCopyable.copyIfNeeded(this.h().keyReader);
        this.innerInputKeyDataAccess = this.h().keyDataAccess.copy();
        this.innerWriteLock = new WriteLock();
        this.wrappedValueBytesData = new WrappedValueBytesData();
        this.segmentBS = new PointerBytesStore();
        this.segmentBytes = CompiledMapQueryContext.unmonitoredVanillaBytes(segmentBS);
        this.hashEntryChecksumStrategy = new HashEntryChecksumStrategy();
        this.checksumStrategy = this.h().checksumEntries ? this.hashEntryChecksumStrategy : NoChecksumStrategy.INSTANCE;
        this.innerUpdateLock = new UpdateLock();
        this.freeList = new ReusableBitSet(new SingleThreadedFlatBitSetFrame(MemoryUnit.LONGS.align(this.h().actualChunksPerSegmentTier, MemoryUnit.BITS)) , Access.nativeAccess() , null , 0);
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

    public class AcquireHandle implements MapClosable {
        @Override
        public void close() {
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            CompiledMapQueryContext.this.replaceValue(CompiledMapQueryContext.this.entry(), CompiledMapQueryContext.this.wrapValueAsData(CompiledMapQueryContext.this.usingReturnValue.returnValue()));
            CompiledMapQueryContext.this.close();
        }
    }

    public class DummyValueZeroData extends AbstractData<V> {
        public DummyValueZeroData() {
            this.zeroBytes = ZeroBytesStore.INSTANCE.bytesForRead();
        }

        private final Bytes zeroBytes;

        public Bytes zeroBytes() {
            return this.zeroBytes;
        }

        private IllegalStateException zeroReadException(Exception cause) {
            return new IllegalStateException((((((CompiledMapQueryContext.this.h().toIdentityString()) + ": Most probable cause of this exception - zero bytes of\n") + "the minimum positive encoding length, supported by the specified or default\n") + "valueSizeMarshaller() is not correct serialized form of any value. You should\n") + "configure defaultValueProvider() in ChronicleMapBuilder") , cause);
        }

        @Override
        public RandomDataInput bytes() {
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            return ZeroBytesStore.INSTANCE;
        }

        @Override
        public long size() {
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            return Math.max(0, CompiledMapQueryContext.this.m().valueSizeMarshaller.minStorableSize());
        }

        @Override
        public V getUsing(V using) {
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            zeroBytes.readPosition(0);
            try {
                return CompiledMapQueryContext.this.valueReader.read(zeroBytes, size(), using);
            } catch (Exception e) {
                throw zeroReadException(e);
            }
        }

        @Override
        public V get() {
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            return getUsing(null);
        }

        @Override
        public long offset() {
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            return 0;
        }
    }

    public class EntryKeyBytesData extends AbstractData<K> {
        public void doCloseCachedEntryKey() {
            this.cachedEntryKeyRead = false;
        }

        @Override
        public long offset() {
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            return CompiledMapQueryContext.this.keyOffset();
        }

        @Override
        public long hash(LongHashFunction f) {
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            return super.hash(f);
        }

        @Override
        public long size() {
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            return CompiledMapQueryContext.this.keySize();
        }

        public void closeEntryKeyBytesDataSizeDependants() {
            this.closeEntryKeyBytesDataInnerGetUsingDependants();
        }

        private K innerGetUsing(K usingKey) {
            Bytes bytes = CompiledMapQueryContext.this.segmentBytesForReadGuarded();
            bytes.readPosition(CompiledMapQueryContext.this.keyOffset());
            return CompiledMapQueryContext.this.keyReader.read(bytes, size(), usingKey);
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
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            return cachedEntryKey();
        }

        @Override
        public K getUsing(K using) {
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            return innerGetUsing(using);
        }

        @Override
        public RandomDataInput bytes() {
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            return CompiledMapQueryContext.this.segmentBS();
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
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            return CompiledMapQueryContext.this.valueOffset();
        }

        @Override
        public long size() {
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            return CompiledMapQueryContext.this.valueSize();
        }

        public void closeEntryValueBytesDataSizeDependants() {
            this.closeEntryValueBytesDataInnerGetUsingDependants();
        }

        private V innerGetUsing(V usingValue) {
            Bytes segmentBytes = CompiledMapQueryContext.this.segmentBytesForReadGuarded();
            segmentBytes.readPosition(CompiledMapQueryContext.this.valueOffset());
            return CompiledMapQueryContext.this.valueReader.read(segmentBytes, size(), usingValue);
        }

        public void closeEntryValueBytesDataInnerGetUsingDependants() {
            this.closeCachedEntryValue();
        }

        private V cachedEntryValue = (CompiledMapQueryContext.this.m().valueType()) == (CharSequence.class) ? ((V)(new StringBuilder())) : null;

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
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            return cachedEntryValue();
        }

        @Override
        public V getUsing(V using) {
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            return innerGetUsing(using);
        }

        @Override
        public RandomDataInput bytes() {
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            return CompiledMapQueryContext.this.segmentBS();
        }
    }

    public class HashEntryChecksumStrategy implements ChecksumStrategy {
        @Override
        public long extraEntryBytes() {
            return ChecksumStrategy.CHECKSUM_STORED_BYTES;
        }

        @Override
        public int computeChecksum() {
            long keyHashCode = CompiledMapQueryContext.this.keyHashCode();
            long keyEnd = CompiledMapQueryContext.this.keyEnd();
            long len = (CompiledMapQueryContext.this.entryEnd()) - keyEnd;
            long checksum;
            if (len > 0) {
                long addr = (CompiledMapQueryContext.this.tierBaseAddr()) + keyEnd;
                long payloadChecksum = LongHashFunction.xx_r39().hashMemory(addr, len);
                checksum = net.openhft.chronicle.hash.impl.stage.entry.ChecksumHashing.hash8To16Bytes(CompiledMapQueryContext.this.keySize(), keyHashCode, payloadChecksum);
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
            CompiledMapQueryContext.this.segmentBS().writeInt(CompiledMapQueryContext.this.entryEnd(), checksum);
        }

        public void closeHashEntryChecksumStrategyComputeAndStoreChecksumDependants() {
            CompiledMapQueryContext.this.closeDelayedUpdateChecksum();
        }

        @Override
        public int storedChecksum() {
            return CompiledMapQueryContext.this.segmentBS().readInt(CompiledMapQueryContext.this.entryEnd());
        }

        @Override
        public boolean innerCheckSum() {
            int oldChecksum = storedChecksum();
            int checksum = computeChecksum();
            return oldChecksum == checksum;
        }
    }

    public class InputKeyBytesData extends AbstractData<K> {
        public void doCloseInputKeyBytesStore() {
            this.inputKeyBytesStore = null;
        }

        public void doCloseCachedInputKey() {
            this.cachedInputKeyRead = false;
        }

        public void doCloseInputKeyBytes() {
            if (!(this.inputKeyBytesInit()))
                return ;

            inputKeyBytes.bytesStore(BytesStore.empty(), 0, 0);
            inputKeyBytesUsed = false;
        }

        public InputKeyBytesData() {
            this.inputKeyBytes = VanillaBytes.vanillaBytes();
        }

        private final VanillaBytes inputKeyBytes;

        private long inputKeyBytesOffset;

        private long inputKeyBytesSize;

        private BytesStore<?, ?> inputKeyBytesStore = null;

        public boolean inputKeyBytesStoreInit() {
            return (this.inputKeyBytesStore) != null;
        }

        public void initInputKeyBytesStore(BytesStore<?, ?> bytesStore, long offset, long size) {
            boolean wasInputKeyBytesStoreInit = this.inputKeyBytesStoreInit();
            inputKeyBytesStore = bytesStore;
            inputKeyBytesOffset = offset;
            inputKeyBytesSize = size;
            if (wasInputKeyBytesStoreInit)
                this.closeInputKeyBytesStoreDependants();
            
        }

        public long inputKeyBytesSize() {
            assert this.inputKeyBytesStoreInit() : "InputKeyBytesStore should be init";
            return this.inputKeyBytesSize;
        }

        public long inputKeyBytesOffset() {
            assert this.inputKeyBytesStoreInit() : "InputKeyBytesStore should be init";
            return this.inputKeyBytesOffset;
        }

        public BytesStore<?, ?> inputKeyBytesStore() {
            assert this.inputKeyBytesStoreInit() : "InputKeyBytesStore should be init";
            return this.inputKeyBytesStore;
        }

        public void closeInputKeyBytesStore() {
            if (!(this.inputKeyBytesStoreInit()))
                return ;
            
            this.closeInputKeyBytesStoreDependants();
            this.inputKeyBytesStore = null;
        }

        public void closeInputKeyBytesStoreDependants() {
            this.closeInputKeyBytes();
            this.closeInputKeyBytesDataInnerGetUsingDependants();
        }

        private boolean inputKeyBytesUsed = false;

        boolean inputKeyBytesInit() {
            return inputKeyBytesUsed;
        }

        void initInputKeyBytes() {
            boolean wasInputKeyBytesInit = this.inputKeyBytesInit();
            inputKeyBytes.bytesStore(inputKeyBytesStore(), inputKeyBytesOffset(), inputKeyBytesSize());
            inputKeyBytesUsed = true;
            if (wasInputKeyBytesInit)
                this.closeInputKeyBytesDependants();
            
        }

        public VanillaBytes inputKeyBytes() {
            if (!(this.inputKeyBytesInit()))
                this.initInputKeyBytes();
            
            return this.inputKeyBytes;
        }

        void closeInputKeyBytes() {
            if (!(this.inputKeyBytesInit()))
                return ;
            
            this.closeInputKeyBytesDependants();
            inputKeyBytes.bytesStore(BytesStore.empty(), 0, 0);
            inputKeyBytesUsed = false;
        }

        public void closeInputKeyBytesDependants() {
            this.closeInputKeyBytesDataInnerGetUsingDependants();
        }

        private K innerGetUsing(K usingKey) {
            inputKeyBytes().readPosition(inputKeyBytesOffset());
            return CompiledMapQueryContext.this.keyReader.read(inputKeyBytes(), inputKeyBytesSize(), usingKey);
        }

        public void closeInputKeyBytesDataInnerGetUsingDependants() {
            this.closeCachedInputKey();
        }

        private K cachedInputKey;

        private boolean cachedInputKeyRead = false;

        public boolean cachedInputKeyInit() {
            return this.cachedInputKeyRead;
        }

        private void initCachedInputKey() {
            cachedInputKey = innerGetUsing(cachedInputKey);
            cachedInputKeyRead = true;
        }

        public K cachedInputKey() {
            if (!(this.cachedInputKeyInit()))
                this.initCachedInputKey();
            
            return this.cachedInputKey;
        }

        public void closeCachedInputKey() {
            this.cachedInputKeyRead = false;
        }

        @Override
        public K get() {
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            return cachedInputKey();
        }

        @Override
        public long offset() {
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            return inputKeyBytesOffset();
        }

        @Override
        public RandomDataInput bytes() {
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            return inputKeyBytes().bytesStore();
        }

        @Override
        public K getUsing(K using) {
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            return innerGetUsing(using);
        }

        @Override
        public long size() {
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            return inputKeyBytesSize();
        }
    }

    public class ReadLock implements InterProcessLock {
        @Override
        public boolean isHeldByCurrentThread() {
            CompiledMapQueryContext.this.checkOnEachLockOperation();
            return CompiledMapQueryContext.this.localLockState().read;
        }

        @Override
        public void lockInterruptibly() throws InterruptedException {
            CompiledMapQueryContext.this.checkOnEachLockOperation();
            if (Thread.interrupted())
                throw new InterruptedException();
            
            if ((CompiledMapQueryContext.this.localLockState()) == (LocalLockState.UNLOCKED)) {
                if (((CompiledMapQueryContext.this.readZeroGuarded()) && (CompiledMapQueryContext.this.updateZeroGuarded())) && (CompiledMapQueryContext.this.writeZeroGuarded())) {
                    try {
                        CompiledMapQueryContext.this.segmentHeader().readLockInterruptibly(CompiledMapQueryContext.this.segmentHeaderAddress());
                    } catch (InterProcessDeadLockException e) {
                        throw CompiledMapQueryContext.this.debugContextsAndLocksGuarded(e);
                    }
                } 
                CompiledMapQueryContext.this.incrementReadGuarded();
                CompiledMapQueryContext.this.setLocalLockStateGuarded(LocalLockState.READ_LOCKED);
            } 
        }

        @Override
        public void lock() {
            CompiledMapQueryContext.this.checkOnEachLockOperation();
            if ((CompiledMapQueryContext.this.localLockState()) == (LocalLockState.UNLOCKED)) {
                if (((CompiledMapQueryContext.this.readZeroGuarded()) && (CompiledMapQueryContext.this.updateZeroGuarded())) && (CompiledMapQueryContext.this.writeZeroGuarded())) {
                    try {
                        CompiledMapQueryContext.this.segmentHeader().readLock(CompiledMapQueryContext.this.segmentHeaderAddress());
                    } catch (InterProcessDeadLockException e) {
                        throw CompiledMapQueryContext.this.debugContextsAndLocksGuarded(e);
                    }
                } 
                CompiledMapQueryContext.this.incrementReadGuarded();
                CompiledMapQueryContext.this.setLocalLockStateGuarded(LocalLockState.READ_LOCKED);
            } 
        }

        public void closeReadLockLockDependants() {
            CompiledMapQueryContext.this.closeHashLookupPos();
        }

        @Override
        public void unlock() {
            CompiledMapQueryContext.this.checkOnEachLockOperation();
            if ((CompiledMapQueryContext.this.localLockState()) != (LocalLockState.UNLOCKED)) {
                CompiledMapQueryContext.this.closeHashLookupPos();
                CompiledMapQueryContext.this.closeEntry();
            } 
            CompiledMapQueryContext.this.readUnlockAndDecrementCountGuarded();
            CompiledMapQueryContext.this.setLocalLockStateGuarded(LocalLockState.UNLOCKED);
        }

        @Override
        public boolean tryLock() {
            CompiledMapQueryContext.this.checkOnEachLockOperation();
            if ((CompiledMapQueryContext.this.localLockState()) == (LocalLockState.UNLOCKED)) {
                if ((((!(CompiledMapQueryContext.this.readZeroGuarded())) || (!(CompiledMapQueryContext.this.updateZeroGuarded()))) || (!(CompiledMapQueryContext.this.writeZeroGuarded()))) || (CompiledMapQueryContext.this.segmentHeader().tryReadLock(CompiledMapQueryContext.this.segmentHeaderAddress()))) {
                    CompiledMapQueryContext.this.incrementReadGuarded();
                    CompiledMapQueryContext.this.setLocalLockStateGuarded(LocalLockState.READ_LOCKED);
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
            CompiledMapQueryContext.this.checkOnEachLockOperation();
            if (Thread.interrupted())
                throw new InterruptedException();
            
            if ((CompiledMapQueryContext.this.localLockState()) == (LocalLockState.UNLOCKED)) {
                if ((((!(CompiledMapQueryContext.this.readZeroGuarded())) || (!(CompiledMapQueryContext.this.updateZeroGuarded()))) || (!(CompiledMapQueryContext.this.writeZeroGuarded()))) || (CompiledMapQueryContext.this.segmentHeader().tryReadLock(CompiledMapQueryContext.this.segmentHeaderAddress(), time, unit))) {
                    CompiledMapQueryContext.this.incrementReadGuarded();
                    CompiledMapQueryContext.this.setLocalLockStateGuarded(LocalLockState.READ_LOCKED);
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
            return  CompiledMapQueryContext.this.m != null &&
                    CompiledMapQueryContext.this.localLockState != null &&
                    CompiledMapQueryContext.this.localLockState != UNLOCKED;
        }
    }

    public class UpdateLock implements InterProcessLock {
        @NotNull
        private IllegalStateException forbiddenUpdateLockWhenOuterContextReadLocked() {
            return new IllegalStateException((((CompiledMapQueryContext.this.h().toIdentityString()) + ": Cannot acquire update lock, because outer context holds read lock. ") + "In this case you should acquire update lock in the outer context up front"));
        }

        @NotNull
        private IllegalMonitorStateException forbiddenUpgrade() {
            return new IllegalMonitorStateException(((CompiledMapQueryContext.this.h().toIdentityString()) + ": Cannot upgrade from read to update lock"));
        }

        @Override
        public boolean isHeldByCurrentThread() {
            CompiledMapQueryContext.this.checkOnEachLockOperation();
            return CompiledMapQueryContext.this.localLockState().update;
        }

        @Override
        public boolean tryLock(long time, @NotNull
        TimeUnit unit) throws InterruptedException {
            CompiledMapQueryContext.this.checkOnEachLockOperation();
            if (Thread.interrupted())
                throw new InterruptedException();
            
            switch (CompiledMapQueryContext.this.localLockState()) {
                case UNLOCKED :
                    CompiledMapQueryContext.this.checkIterationContextNotLockedInThisThread();
                    if ((CompiledMapQueryContext.this.updateZeroGuarded()) && (CompiledMapQueryContext.this.writeZeroGuarded())) {
                        if (!(CompiledMapQueryContext.this.readZeroGuarded()))
                            throw forbiddenUpdateLockWhenOuterContextReadLocked();
                        
                        if (CompiledMapQueryContext.this.segmentHeader().tryUpdateLock(CompiledMapQueryContext.this.segmentHeaderAddress(), time, unit)) {
                            CompiledMapQueryContext.this.incrementUpdateGuarded();
                            CompiledMapQueryContext.this.setLocalLockStateGuarded(LocalLockState.UPDATE_LOCKED);
                            return true;
                        } else {
                            return false;
                        }
                    } else {
                        CompiledMapQueryContext.this.incrementUpdateGuarded();
                        CompiledMapQueryContext.this.setLocalLockStateGuarded(LocalLockState.UPDATE_LOCKED);
                        return true;
                    }
                case READ_LOCKED :
                    throw forbiddenUpgrade();
                case UPDATE_LOCKED :
                case WRITE_LOCKED :
                    return true;
                default :
                    throw new IllegalStateException((((CompiledMapQueryContext.this.h().toIdentityString()) + ": unexpected localLockState=") + (CompiledMapQueryContext.this.localLockState())));
            }
        }

        @Override
        public boolean tryLock() {
            CompiledMapQueryContext.this.checkOnEachLockOperation();
            switch (CompiledMapQueryContext.this.localLockState()) {
                case UNLOCKED :
                    CompiledMapQueryContext.this.checkIterationContextNotLockedInThisThread();
                    if ((CompiledMapQueryContext.this.updateZeroGuarded()) && (CompiledMapQueryContext.this.writeZeroGuarded())) {
                        if (!(CompiledMapQueryContext.this.readZeroGuarded()))
                            throw forbiddenUpdateLockWhenOuterContextReadLocked();
                        
                        if (CompiledMapQueryContext.this.segmentHeader().tryUpdateLock(CompiledMapQueryContext.this.segmentHeaderAddress())) {
                            CompiledMapQueryContext.this.incrementUpdateGuarded();
                            CompiledMapQueryContext.this.setLocalLockStateGuarded(LocalLockState.UPDATE_LOCKED);
                            return true;
                        } else {
                            return false;
                        }
                    } else {
                        CompiledMapQueryContext.this.incrementUpdateGuarded();
                        CompiledMapQueryContext.this.setLocalLockStateGuarded(LocalLockState.UPDATE_LOCKED);
                        return true;
                    }
                case READ_LOCKED :
                    throw forbiddenUpgrade();
                case UPDATE_LOCKED :
                case WRITE_LOCKED :
                    return true;
                default :
                    throw new IllegalStateException((((CompiledMapQueryContext.this.h().toIdentityString()) + ": unexpected localLockState=") + (CompiledMapQueryContext.this.localLockState())));
            }
        }

        @Override
        public void lockInterruptibly() throws InterruptedException {
            CompiledMapQueryContext.this.checkOnEachLockOperation();
            if (Thread.interrupted())
                throw new InterruptedException();
            
            switch (CompiledMapQueryContext.this.localLockState()) {
                case UNLOCKED :
                    CompiledMapQueryContext.this.checkIterationContextNotLockedInThisThread();
                    if ((CompiledMapQueryContext.this.updateZeroGuarded()) && (CompiledMapQueryContext.this.writeZeroGuarded())) {
                        if (!(CompiledMapQueryContext.this.readZeroGuarded()))
                            throw forbiddenUpdateLockWhenOuterContextReadLocked();
                        
                        try {
                            CompiledMapQueryContext.this.segmentHeader().updateLockInterruptibly(CompiledMapQueryContext.this.segmentHeaderAddress());
                        } catch (InterProcessDeadLockException e) {
                            throw CompiledMapQueryContext.this.debugContextsAndLocksGuarded(e);
                        }
                    } 
                    CompiledMapQueryContext.this.incrementUpdateGuarded();
                    CompiledMapQueryContext.this.setLocalLockStateGuarded(LocalLockState.UPDATE_LOCKED);
                    return ;
                case READ_LOCKED :
                    throw forbiddenUpgrade();
                case UPDATE_LOCKED :
                case WRITE_LOCKED :
            }
        }

        @Override
        public void unlock() {
            CompiledMapQueryContext.this.checkOnEachLockOperation();
            switch (CompiledMapQueryContext.this.localLockState()) {
                case UNLOCKED :
                case READ_LOCKED :
                    return ;
                case UPDATE_LOCKED :
                    CompiledMapQueryContext.this.closeDelayedUpdateChecksum();
                    if (((CompiledMapQueryContext.this.decrementUpdateGuarded()) == 0) && (CompiledMapQueryContext.this.writeZeroGuarded())) {
                        CompiledMapQueryContext.this.segmentHeader().downgradeUpdateToReadLock(CompiledMapQueryContext.this.segmentHeaderAddress());
                    } 
                    break;
                case WRITE_LOCKED :
                    CompiledMapQueryContext.this.closeDelayedUpdateChecksum();
                    if ((CompiledMapQueryContext.this.decrementWriteGuarded()) == 0) {
                        if (!(CompiledMapQueryContext.this.updateZeroGuarded())) {
                            CompiledMapQueryContext.this.segmentHeader().downgradeWriteToUpdateLock(CompiledMapQueryContext.this.segmentHeaderAddress());
                        } else {
                            CompiledMapQueryContext.this.segmentHeader().downgradeWriteToReadLock(CompiledMapQueryContext.this.segmentHeaderAddress());
                        }
                    } 
            }
            CompiledMapQueryContext.this.incrementReadGuarded();
            CompiledMapQueryContext.this.setLocalLockStateGuarded(LocalLockState.READ_LOCKED);
        }

        @Override
        public void lock() {
            CompiledMapQueryContext.this.checkOnEachLockOperation();
            switch (CompiledMapQueryContext.this.localLockState()) {
                case UNLOCKED :
                    CompiledMapQueryContext.this.checkIterationContextNotLockedInThisThread();
                    if ((CompiledMapQueryContext.this.updateZeroGuarded()) && (CompiledMapQueryContext.this.writeZeroGuarded())) {
                        if (!(CompiledMapQueryContext.this.readZeroGuarded()))
                            throw forbiddenUpdateLockWhenOuterContextReadLocked();
                        
                        try {
                            CompiledMapQueryContext.this.segmentHeader().updateLock(CompiledMapQueryContext.this.segmentHeaderAddress());
                        } catch (InterProcessDeadLockException e) {
                            throw CompiledMapQueryContext.this.debugContextsAndLocksGuarded(e);
                        }
                    } 
                    CompiledMapQueryContext.this.incrementUpdateGuarded();
                    CompiledMapQueryContext.this.setLocalLockStateGuarded(LocalLockState.UPDATE_LOCKED);
                    return ;
                case READ_LOCKED :
                    throw forbiddenUpgrade();
                case UPDATE_LOCKED :
                case WRITE_LOCKED :
            }
        }

        @Override
        public boolean isHeld() {
            return CompiledMapQueryContext.this.m != null &&
                    CompiledMapQueryContext.this.localLockState != null &&
                    CompiledMapQueryContext.this.localLockState != UNLOCKED;
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
            return CompiledMapQueryContext.this.valueReader.read(wrappedValueBytes(), wrappedValueBytesSize(), usingValue);
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
        public long offset() {
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            return wrappedValueBytesOffset();
        }

        @Override
        public RandomDataInput bytes() {
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            return wrappedValueBytes().bytesStore();
        }

        @Override
        public V get() {
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            return cachedWrappedValue();
        }

        @Override
        public long size() {
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            return wrappedValueBytesSize();
        }

        @Override
        public V getUsing(V using) {
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            return innerGetUsing(using);
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
            this.wrappedValueDataAccess = CompiledMapQueryContext.this.m().valueDataAccess.copy();
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
            CompiledMapQueryContext.this.m().checkValue(value);
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
            return new IllegalMonitorStateException(((CompiledMapQueryContext.this.h().toIdentityString()) + ": Cannot upgrade from read to write lock"));
        }

        @NotNull
        private IllegalStateException forbiddenWriteLockWhenOuterContextReadLocked() {
            return new IllegalStateException((((CompiledMapQueryContext.this.h().toIdentityString()) + ": Cannot acquire write lock, because outer context holds read lock. ") + "In this case you should acquire update lock in the outer context up front"));
        }

        @Override
        public boolean tryLock() {
            CompiledMapQueryContext.this.checkOnEachLockOperation();
            switch (CompiledMapQueryContext.this.localLockState()) {
                case UNLOCKED :
                    CompiledMapQueryContext.this.checkIterationContextNotLockedInThisThread();
                    if (CompiledMapQueryContext.this.writeZeroGuarded()) {
                        if (!(CompiledMapQueryContext.this.updateZeroGuarded())) {
                            if (CompiledMapQueryContext.this.segmentHeader().tryUpgradeUpdateToWriteLock(CompiledMapQueryContext.this.segmentHeaderAddress())) {
                                CompiledMapQueryContext.this.incrementWriteGuarded();
                                CompiledMapQueryContext.this.setLocalLockStateGuarded(LocalLockState.WRITE_LOCKED);
                                return true;
                            } else {
                                return false;
                            }
                        } else {
                            if (!(CompiledMapQueryContext.this.readZeroGuarded()))
                                throw forbiddenWriteLockWhenOuterContextReadLocked();
                            
                            if (CompiledMapQueryContext.this.segmentHeader().tryWriteLock(CompiledMapQueryContext.this.segmentHeaderAddress())) {
                                CompiledMapQueryContext.this.incrementWriteGuarded();
                                CompiledMapQueryContext.this.setLocalLockStateGuarded(LocalLockState.WRITE_LOCKED);
                                return true;
                            } else {
                                return false;
                            }
                        }
                    } else {
                        CompiledMapQueryContext.this.incrementWriteGuarded();
                        CompiledMapQueryContext.this.setLocalLockStateGuarded(LocalLockState.WRITE_LOCKED);
                        return true;
                    }
                case READ_LOCKED :
                    throw forbiddenUpgrade();
                case UPDATE_LOCKED :
                    if (CompiledMapQueryContext.this.writeZeroGuarded()) {
                        assert !(CompiledMapQueryContext.this.updateZeroGuarded());
                        if (CompiledMapQueryContext.this.segmentHeader().tryUpgradeUpdateToWriteLock(CompiledMapQueryContext.this.segmentHeaderAddress())) {
                            CompiledMapQueryContext.this.decrementUpdateGuarded();
                            CompiledMapQueryContext.this.incrementWriteGuarded();
                            CompiledMapQueryContext.this.setLocalLockStateGuarded(LocalLockState.WRITE_LOCKED);
                            return true;
                        } else {
                            return false;
                        }
                    } else {
                        CompiledMapQueryContext.this.decrementUpdateGuarded();
                        CompiledMapQueryContext.this.incrementWriteGuarded();
                        CompiledMapQueryContext.this.setLocalLockStateGuarded(LocalLockState.WRITE_LOCKED);
                        return true;
                    }
                case WRITE_LOCKED :
                    return true;
                default :
                    throw new IllegalStateException((((CompiledMapQueryContext.this.h().toIdentityString()) + ": unexpected localLockState=") + (CompiledMapQueryContext.this.localLockState())));
            }
        }

        @Override
        public boolean tryLock(long time, @NotNull
        TimeUnit unit) throws InterruptedException {
            CompiledMapQueryContext.this.checkOnEachLockOperation();
            if (Thread.interrupted())
                throw new InterruptedException();
            
            switch (CompiledMapQueryContext.this.localLockState()) {
                case UNLOCKED :
                    CompiledMapQueryContext.this.checkIterationContextNotLockedInThisThread();
                    if (CompiledMapQueryContext.this.writeZeroGuarded()) {
                        if (!(CompiledMapQueryContext.this.updateZeroGuarded())) {
                            if (CompiledMapQueryContext.this.segmentHeader().tryUpgradeUpdateToWriteLock(CompiledMapQueryContext.this.segmentHeaderAddress(), time, unit)) {
                                CompiledMapQueryContext.this.incrementWriteGuarded();
                                CompiledMapQueryContext.this.setLocalLockStateGuarded(LocalLockState.WRITE_LOCKED);
                                return true;
                            } else {
                                return false;
                            }
                        } else {
                            if (!(CompiledMapQueryContext.this.readZeroGuarded()))
                                throw forbiddenWriteLockWhenOuterContextReadLocked();
                            
                            if (CompiledMapQueryContext.this.segmentHeader().tryWriteLock(CompiledMapQueryContext.this.segmentHeaderAddress(), time, unit)) {
                                CompiledMapQueryContext.this.incrementWriteGuarded();
                                CompiledMapQueryContext.this.setLocalLockStateGuarded(LocalLockState.WRITE_LOCKED);
                                return true;
                            } else {
                                return false;
                            }
                        }
                    } else {
                        CompiledMapQueryContext.this.incrementWriteGuarded();
                        CompiledMapQueryContext.this.setLocalLockStateGuarded(LocalLockState.WRITE_LOCKED);
                        return true;
                    }
                case READ_LOCKED :
                    throw forbiddenUpgrade();
                case UPDATE_LOCKED :
                    if (CompiledMapQueryContext.this.writeZeroGuarded()) {
                        assert !(CompiledMapQueryContext.this.updateZeroGuarded());
                        if (CompiledMapQueryContext.this.segmentHeader().tryUpgradeUpdateToWriteLock(CompiledMapQueryContext.this.segmentHeaderAddress(), time, unit)) {
                            CompiledMapQueryContext.this.decrementUpdateGuarded();
                            CompiledMapQueryContext.this.incrementWriteGuarded();
                            CompiledMapQueryContext.this.setLocalLockStateGuarded(LocalLockState.WRITE_LOCKED);
                            return true;
                        } else {
                            return false;
                        }
                    } else {
                        CompiledMapQueryContext.this.decrementUpdateGuarded();
                        CompiledMapQueryContext.this.incrementWriteGuarded();
                        CompiledMapQueryContext.this.setLocalLockStateGuarded(LocalLockState.WRITE_LOCKED);
                        return true;
                    }
                case WRITE_LOCKED :
                    return true;
                default :
                    throw new IllegalStateException((((CompiledMapQueryContext.this.h().toIdentityString()) + ": unexpected localLockState=") + (CompiledMapQueryContext.this.localLockState())));
            }
        }

        @Override
        public boolean isHeldByCurrentThread() {
            CompiledMapQueryContext.this.checkOnEachLockOperation();
            return CompiledMapQueryContext.this.localLockState().write;
        }

        @Override
        public void lockInterruptibly() throws InterruptedException {
            CompiledMapQueryContext.this.checkOnEachLockOperation();
            if (Thread.interrupted())
                throw new InterruptedException();
            
            switch (CompiledMapQueryContext.this.localLockState()) {
                case UNLOCKED :
                    CompiledMapQueryContext.this.checkIterationContextNotLockedInThisThread();
                    if (CompiledMapQueryContext.this.writeZeroGuarded()) {
                        if (!(CompiledMapQueryContext.this.updateZeroGuarded())) {
                            CompiledMapQueryContext.this.segmentHeader().upgradeUpdateToWriteLockInterruptibly(CompiledMapQueryContext.this.segmentHeaderAddress());
                        } else {
                            if (!(CompiledMapQueryContext.this.readZeroGuarded()))
                                throw forbiddenWriteLockWhenOuterContextReadLocked();
                            
                            try {
                                CompiledMapQueryContext.this.segmentHeader().writeLockInterruptibly(CompiledMapQueryContext.this.segmentHeaderAddress());
                            } catch (InterProcessDeadLockException e) {
                                throw CompiledMapQueryContext.this.debugContextsAndLocksGuarded(e);
                            }
                        }
                    } 
                    CompiledMapQueryContext.this.incrementWriteGuarded();
                    CompiledMapQueryContext.this.setLocalLockStateGuarded(LocalLockState.WRITE_LOCKED);
                    return ;
                case READ_LOCKED :
                    throw forbiddenUpgrade();
                case UPDATE_LOCKED :
                    if (CompiledMapQueryContext.this.writeZeroGuarded()) {
                        assert !(CompiledMapQueryContext.this.updateZeroGuarded());
                        try {
                            CompiledMapQueryContext.this.segmentHeader().upgradeUpdateToWriteLockInterruptibly(CompiledMapQueryContext.this.segmentHeaderAddress());
                        } catch (InterProcessDeadLockException e) {
                            throw CompiledMapQueryContext.this.debugContextsAndLocksGuarded(e);
                        }
                    } 
                    CompiledMapQueryContext.this.decrementUpdateGuarded();
                    CompiledMapQueryContext.this.incrementWriteGuarded();
                    CompiledMapQueryContext.this.setLocalLockStateGuarded(LocalLockState.WRITE_LOCKED);
                    break;
                case WRITE_LOCKED :
                    break;
            }
        }

        @Override
        public synchronized void unlock() {
            CompiledMapQueryContext.this.checkOnEachLockOperation();
            switch (CompiledMapQueryContext.this.localLockState()) {
                case UNLOCKED:
                case READ_LOCKED:
                case UPDATE_LOCKED:
                    return;
                case WRITE_LOCKED:
                    CompiledMapQueryContext.this.closeDelayedUpdateChecksum();
                    if ((CompiledMapQueryContext.this.decrementWriteGuarded()) == 0)
                        CompiledMapQueryContext.this.segmentHeader().downgradeWriteToUpdateLock(CompiledMapQueryContext.this.segmentHeaderAddress());
                    
                    CompiledMapQueryContext.this.incrementUpdateGuarded();
                    CompiledMapQueryContext.this.setLocalLockStateGuarded(LocalLockState.UPDATE_LOCKED);
            }
        }

        @Override
        public synchronized void lock() {
            CompiledMapQueryContext.this.checkOnEachLockOperation();
            switch (CompiledMapQueryContext.this.localLockState()) {
                case UNLOCKED:
                    CompiledMapQueryContext.this.checkIterationContextNotLockedInThisThread();
                    if (CompiledMapQueryContext.this.writeZeroGuarded()) {
                        if (!(CompiledMapQueryContext.this.updateZeroGuarded())) {
                            CompiledMapQueryContext.this.segmentHeader().upgradeUpdateToWriteLock(CompiledMapQueryContext.this.segmentHeaderAddress());
                        } else {
                            if (!(CompiledMapQueryContext.this.readZeroGuarded()))
                                throw forbiddenWriteLockWhenOuterContextReadLocked();
                            
                            try {
                                CompiledMapQueryContext.this.segmentHeader().writeLock(CompiledMapQueryContext.this.segmentHeaderAddress());
                            } catch (InterProcessDeadLockException e) {
                                throw CompiledMapQueryContext.this.debugContextsAndLocksGuarded(e);
                            }
                        }
                    } 
                    CompiledMapQueryContext.this.incrementWriteGuarded();
                    CompiledMapQueryContext.this.setLocalLockStateGuarded(LocalLockState.WRITE_LOCKED);
                    return ;
                case READ_LOCKED :
                    throw forbiddenUpgrade();
                case UPDATE_LOCKED :
                    if (CompiledMapQueryContext.this.writeZeroGuarded()) {
                        assert !(CompiledMapQueryContext.this.updateZeroGuarded());
                        try {
                            CompiledMapQueryContext.this.segmentHeader().upgradeUpdateToWriteLock(CompiledMapQueryContext.this.segmentHeaderAddress());
                        } catch (InterProcessDeadLockException e) {
                            throw CompiledMapQueryContext.this.debugContextsAndLocksGuarded(e);
                        }
                    } 
                    CompiledMapQueryContext.this.decrementUpdateGuarded();
                    CompiledMapQueryContext.this.incrementWriteGuarded();
                    CompiledMapQueryContext.this.setLocalLockStateGuarded(LocalLockState.WRITE_LOCKED);
                    break;
                case WRITE_LOCKED :
                    break;
            }
        }

        @Override
        public boolean isHeld() {
            return CompiledMapQueryContext.this.m != null &&
                    CompiledMapQueryContext.this.localLockState != null &&
                    CompiledMapQueryContext.this.localLockState != UNLOCKED;
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

    public enum EntryPresence {
PRESENT, ABSENT;    }

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

    public void setSearchState(CompiledMapQueryContext.SearchState newSearchState) {
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

    public class DefaultReturnValue implements InstanceReturnValue<V> {
        public void doCloseDefaultReturnedValue() {
            this.defaultReturnedValue = null;
        }

        @Override
        public void returnValue(@NotNull
        Data<V> value) {
            initDefaultReturnedValue(value);
        }

        private V defaultReturnedValue = null;

        boolean defaultReturnedValueInit() {
            return (this.defaultReturnedValue) != null;
        }

        private void initDefaultReturnedValue(@NotNull
        Data<V> value) {
            defaultReturnedValue = value.getUsing(null);
        }

        public V defaultReturnedValue() {
            assert this.defaultReturnedValueInit() : "DefaultReturnedValue should be init";
            return this.defaultReturnedValue;
        }

        public void closeDefaultReturnedValue() {
            this.defaultReturnedValue = null;
        }

        @Override
        public V returnValue() {
            if (defaultReturnedValueInit()) {
                return defaultReturnedValue();
            } else {
                return null;
            }
        }
    }

    public class UsingReturnValue implements UsableReturnValue<V> {
        public void doCloseUsingReturnValue() {
            this.usingReturnValue = ((V)(UsableReturnValue.USING_RETURN_VALUE_UNINIT));
        }

        public void doCloseReturnedValue() {
            this.returnedValue = null;
        }

        @Override
        public void returnValue(@NotNull
        Data<V> value) {
            initReturnedValue(value);
        }

        private V usingReturnValue = ((V)(UsableReturnValue.USING_RETURN_VALUE_UNINIT));

        public boolean usingReturnValueInit() {
            return (this.usingReturnValue) != ((V)(UsableReturnValue.USING_RETURN_VALUE_UNINIT));
        }

        @Override
        public void initUsingReturnValue(V usingReturnValue) {
            boolean wasUsingReturnValueInit = this.usingReturnValueInit();
            this.usingReturnValue = usingReturnValue;
            if (wasUsingReturnValueInit)
                this.closeUsingReturnValueDependants();
            
        }

        public V usingReturnValue() {
            assert this.usingReturnValueInit() : "UsingReturnValue should be init";
            return this.usingReturnValue;
        }

        public void closeUsingReturnValue() {
            if (!(this.usingReturnValueInit()))
                return ;
            
            this.closeUsingReturnValueDependants();
            this.usingReturnValue = ((V)(UsableReturnValue.USING_RETURN_VALUE_UNINIT));
        }

        public void closeUsingReturnValueDependants() {
            this.closeReturnedValue();
        }

        private V returnedValue = null;

        boolean returnedValueInit() {
            return (this.returnedValue) != null;
        }

        private void initReturnedValue(@NotNull
        Data<V> value) {
            returnedValue = value.getUsing(usingReturnValue());
        }

        public V returnedValue() {
            assert this.returnedValueInit() : "ReturnedValue should be init";
            return this.returnedValue;
        }

        public void closeReturnedValue() {
            this.returnedValue = null;
        }

        @Override
        public V returnValue() {
            if (returnedValueInit()) {
                return returnedValue();
            } else {
                return null;
            }
        }
    }

    public final UpdateLock innerUpdateLock;

    public UpdateLock innerUpdateLock() {
        return this.innerUpdateLock;
    }

    final DataAccess<V> innerInputValueDataAccess;

    final DataAccess<K> innerInputKeyDataAccess;

    public DataAccess<K> innerInputKeyDataAccess() {
        return this.innerInputKeyDataAccess;
    }

    public DataAccess<V> innerInputValueDataAccess() {
        return this.innerInputValueDataAccess;
    }

    final InputKeyBytesData inputKeyBytesData;

    public InputKeyBytesData inputKeyBytesData() {
        return this.inputKeyBytesData;
    }

    final DummyValueZeroData zeroValueData;

    public DummyValueZeroData zeroValueData() {
        return this.zeroValueData;
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

    public final AcquireHandle acquireHandle;

    public final UsingReturnValue usingReturnValue;

    public UsingReturnValue usingReturnValue() {
        return this.usingReturnValue;
    }

    public AcquireHandle acquireHandle() {
        return this.acquireHandle;
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

    public ReusableBitSet freeList() {
        return this.freeList;
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

    public final DefaultReturnValue defaultReturnValue;

    public DefaultReturnValue defaultReturnValue() {
        return this.defaultReturnValue;
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

    public Bytes segmentBytes() {
        return this.segmentBytes;
    }

    public final ChecksumStrategy checksumStrategy;

    public ChecksumStrategy checksumStrategy() {
        return this.checksumStrategy;
    }

    public <T>T contextAtIndexInChain(int index) {
        return ((T)(contextChain.get(index)));
    }

    public void closeVanillaChronicleMapHolderImplContextAtIndexInChainDependants() {
        this.closeLocks();
    }

    @Override
    public <T extends ChainingInterface>T getContext(Class<? extends T> contextClass, BiFunction<ChainingInterface, VanillaChronicleMap, T> createChaining, VanillaChronicleMap map) {
        for (int i = 0 ; i < (contextChain.size()) ; i++) {
            ChainingInterface context = contextChain.get(i);
            if (((context.getClass()) == contextClass) && (!(context.usedInit()))) {
                return CompiledMapQueryContext.initUsedAndReturn(map, context);
            } 
        }
        int maxNestedContexts = 1 << 10;
        if ((contextChain.size()) > maxNestedContexts) {
            throw new IllegalStateException(((((((((map.toIdentityString()) + ": More than ") + maxNestedContexts) + " nested ChronicleHash contexts\n") + "are not supported. Very probable that you simply forgot to close context\n") + "somewhere (recommended to use try-with-resources statement).\n") + "Otherwise this is a bug, please report with this\n") + "stack trace on https://github.com/OpenHFT/Chronicle-Map/issues"));
        } 
        T context = createChaining.apply(this, map);
        return CompiledMapQueryContext.initUsedAndReturn(map, context);
    }

    public DataAccess<K> inputKeyDataAccess() {
        initInputKeyDataAccess();
        return innerInputKeyDataAccess;
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

    @Override
    public DataAccess<V> inputValueDataAccess() {
        initInputValueDataAccess();
        return innerInputValueDataAccess;
    }

    public long newEntrySize(Data<V> newValue, long entryStartOffset, long newValueOffset) {
        return (((checksumStrategy.extraEntryBytes()) + newValueOffset) + (newValue.size())) - entryStartOffset;
    }

    private void registerIterationContextLockedInThisThread() {
        if ((this) instanceof IterationContext) {
            this.rootContextInThisThread.iterationContextLockedInThisThread = true;
        } 
    }

    public void closeQuerySegmentStagesRegisterIterationContextLockedInThisThreadDependants() {
        this.closeLocks();
    }

    @Override
    public Data<K> getInputKeyBytesAsData(BytesStore<?, ?> bytesStore, long offset, long size) {
        this.inputKeyBytesData.initInputKeyBytesStore(bytesStore, offset, size);
        return this.inputKeyBytesData;
    }

    @Override
    public List<ChainingInterface> getContextChain() {
        return contextChain;
    }

    private void deregisterIterationContextLockedInThisThread() {
        if ((this) instanceof IterationContext) {
            this.rootContextInThisThread.iterationContextLockedInThisThread = false;
        } 
    }

    public void closeQuerySegmentStagesDeregisterIterationContextLockedInThisThreadDependants() {
        this.closeLocks();
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

    private boolean inputKeyDataAccessInitialized = false;

    public boolean inputKeyDataAccessInit() {
        return (this.inputKeyDataAccessInitialized);
    }

    void initInputKeyDataAccess() {
        inputKeyDataAccessInitialized = true;
    }

    void closeInputKeyDataAccess() {
        if (!(this.inputKeyDataAccessInit()))
            return ;
        
        innerInputKeyDataAccess.uninit();
        inputKeyDataAccessInitialized = false;
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
        this.hashEntryChecksumStrategy.closeHashEntryChecksumStrategyComputeChecksumDependants();
    }

    private boolean inputValueDataAccessInitialized = false;

    public boolean inputValueDataAccessInit() {
        return (this.inputValueDataAccessInitialized);
    }

    void initInputValueDataAccess() {
        inputValueDataAccessInitialized = true;
    }

    void closeInputValueDataAccess() {
        if (!(this.inputValueDataAccessInit()))
            return ;
        
        innerInputValueDataAccess.uninit();
        inputValueDataAccessInitialized = false;
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
        this.closeQuerySegmentStagesCheckNestedContextsQueryDifferentKeysDependants();
        this.closeKeyHash();
        this.closeKeySearchKeyEqualsDependants();
    }

    public long keyHash = 0;

    public boolean keyHashInit() {
        return (this.keyHash) != 0;
    }

    void initKeyHash() {
        boolean wasKeyHashInit = this.keyHashInit();
        keyHash = this.inputKey().hash(LongHashFunction.xx_r39());
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
        this.closeInputKeyHashCodeKeyHashCodeDependants();
    }

    @Override
    public long keyHashCode() {
        return keyHash();
    }

    public void closeInputKeyHashCodeKeyHashCodeDependants() {
        this.closeSegmentIndex();
        this.closeSearchKey();
        this.hashEntryChecksumStrategy.closeHashEntryChecksumStrategyComputeChecksumDependants();
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
        this.closeMapQueryDropSearchIfNestedContextsAndPresentHashLookupSlotCheckFailedDependants();
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
        this.closeQuerySegmentStagesCheckNestedContextsQueryDifferentKeysDependants();
        this.closeQueryHashLookupSearchHlDependants();
        this.closeSearchKey();
        this.closeSegmentIndex();
        this.closeSegmentHeader();
        this.closeLocks();
        this.closeSegmentTier();
        this.closeQuerySegmentStagesTierCountersAreaAddrDependants();
        this.closeQuerySegmentStagesNextTierDependants();
        this.closeSegment();
        this.closeEntryOffset();
        this.closeEntrySizeInChunks();
        this.closeDelayedUpdateChecksum();
        this.closeOwnerThreadHolderCheckAccessingFromOwnerThreadDependants();
        this.closeQueryHashLookupSearchNextPosDependants();
        this.closeKeySearch();
    }

    public void checkNestedContextsQueryDifferentKeys(LocksInterface innermostContextOnThisSegment) {
        if ((innermostContextOnThisSegment.getClass()) == (getClass())) {
            Data key = ((CompiledMapQueryContext)(innermostContextOnThisSegment)).inputKey();
            if (java.util.Objects.equals(key, ((CompiledMapQueryContext)((Object)(this))).inputKey())) {
                throw new IllegalStateException((((this.h().toIdentityString()) + ": Nested same-thread contexts cannot access the same key ") + key));
            } 
        } 
    }

    public void closeQuerySegmentStagesCheckNestedContextsQueryDifferentKeysDependants() {
        this.closeLocks();
    }

    public CompactOffHeapLinearHashTable hl() {
        return this.h().hashLookup;
    }

    public void closeQueryHashLookupSearchHlDependants() {
        this.closeSearchKey();
        this.closeQueryHashLookupSearchFoundDependants();
        this.closeQueryHashLookupSearchNextPosDependants();
        this.closeQueryHashLookupSearchCheckSlotContainsExpectedKeyAndValueDependants();
    }

    long searchKey = CompactOffHeapLinearHashTable.UNSET_KEY;

    public long searchStartPos;

    public boolean searchKeyInit() {
        return (this.searchKey) != (CompactOffHeapLinearHashTable.UNSET_KEY);
    }

    void initSearchKey() {
        boolean wasSearchKeyInit = this.searchKeyInit();
        initSearchKey(hl().maskUnsetKey(this.h().hashSplitting.segmentHash(this.keyHashCode())));
        if (wasSearchKeyInit)
            this.closeSearchKeyDependants();
        
    }

    public void initSearchKey(long searchKey) {
        boolean wasSearchKeyInit = this.searchKeyInit();
        this.searchKey = searchKey;
        searchStartPos = hl().hlPos(searchKey);
        if (wasSearchKeyInit)
            this.closeSearchKeyDependants();
        
    }

    public long searchKey() {
        if (!(this.searchKeyInit()))
            this.initSearchKey();
        
        return this.searchKey;
    }

    public long searchStartPos() {
        if (!(this.searchKeyInit()))
            this.initSearchKey();
        
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
        this.closeQueryHashLookupSearchNextPosDependants();
        this.closeQueryHashLookupSearchCheckSlotContainsExpectedKeyAndValueDependants();
    }

    public int segmentIndex = -1;

    public boolean segmentIndexInit() {
        return (this.segmentIndex) >= 0;
    }

    void initSegmentIndex() {
        boolean wasSegmentIndexInit = this.segmentIndexInit();
        segmentIndex = this.h().hashSplitting.segmentIndex(this.keyHashCode());
        if (wasSegmentIndexInit)
            this.closeSegmentIndexDependants();
        
    }

    public void initSegmentIndex(int segmentIndex) {
        boolean wasSegmentIndexInit = this.segmentIndexInit();
        this.segmentIndex = segmentIndex;
        if (wasSegmentIndexInit)
            this.closeSegmentIndexDependants();
        
    }

    public int segmentIndex() {
        if (!(this.segmentIndexInit()))
            this.initSegmentIndex();
        
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
        this.closeQuerySegmentStagesNextTierDependants();
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
        this.closeQuerySegmentStagesNextTierIndexDependants();
        this.closeQuerySegmentStagesNextTierIndexDependants();
        this.closeQuerySegmentStagesLowestPossiblyFreeChunkDependants();
        this.closeQuerySegmentStagesLowestPossiblyFreeChunkDependants();
        this.closeLocks();
        this.innerReadLock.closeReadLockLockDependants();
        this.closeQuerySegmentStagesTierEntriesDependants();
        this.closeQuerySegmentStagesTierEntriesDependants();
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
        this.closeQueryHashLookupSearchAddrDependants();
        this.closeQuerySegmentStagesTierCountersAreaAddrDependants();
        this.closeHashLookupPos();
        this.closeQuerySegmentStagesNextTierIndexDependants();
        this.closeQuerySegmentStagesNextTierIndexDependants();
        this.closeQuerySegmentStagesNextTierDependants();
        this.closeQuerySegmentStagesLowestPossiblyFreeChunkDependants();
        this.closeQuerySegmentStagesLowestPossiblyFreeChunkDependants();
        this.closeQuerySegmentStagesTierEntriesDependants();
        this.closeQuerySegmentStagesTierEntriesDependants();
        this.closeSegment();
        this.hashEntryChecksumStrategy.closeHashEntryChecksumStrategyComputeChecksumDependants();
        this.closeMapQueryTieredEntryPresentDependants();
    }

    private long addr() {
        return this.tierBaseAddr();
    }

    public void closeQueryHashLookupSearchAddrDependants() {
        this.closeQueryHashLookupSearchNextPosDependants();
        this.closeQueryHashLookupSearchCheckSlotContainsExpectedKeyAndValueDependants();
    }

    public long tierCountersAreaAddr() {
        return (tierBaseAddr()) + (this.h().tierHashLookupOuterSize);
    }

    public void closeQuerySegmentStagesTierCountersAreaAddrDependants() {
        this.closeQuerySegmentStagesLowestPossiblyFreeChunkDependants();
        this.closeQuerySegmentStagesLowestPossiblyFreeChunkDependants();
        this.closeQuerySegmentStagesPrevTierIndexDependants();
        this.closeQuerySegmentStagesTierEntriesDependants();
        this.closeQuerySegmentStagesTierEntriesDependants();
        this.closeQuerySegmentStagesNextTierIndexDependants();
        this.closeQuerySegmentStagesNextTierIndexDependants();
        this.closeQuerySegmentStagesNextTierDependants();
    }

    public void prevTierIndex(long prevTierIndex) {
        TierCountersArea.prevTierIndex(tierCountersAreaAddr(), prevTierIndex);
    }

    public void closeQuerySegmentStagesPrevTierIndexDependants() {
        this.closeQuerySegmentStagesNextTierDependants();
    }

    public long prevTierIndex() {
        return TierCountersArea.prevTierIndex(tierCountersAreaAddr());
    }

    public void tierDeleted(long tierDeleted) {
        if ((tier()) == 0) {
            segmentHeader().deleted(segmentHeaderAddress(), tierDeleted);
        } else {
            TierCountersArea.deleted(tierCountersAreaAddr(), tierDeleted);
        }
    }

    public long tierDeleted() {
        if ((tier()) == 0) {
            return segmentHeader().deleted(segmentHeaderAddress());
        } else {
            return TierCountersArea.deleted(tierCountersAreaAddr());
        }
    }

    public void nextTierIndex(long nextTierIndex) {
        if ((tier()) == 0) {
            segmentHeader().nextTierIndex(segmentHeaderAddress(), nextTierIndex);
        } else {
            TierCountersArea.nextTierIndex(tierCountersAreaAddr(), nextTierIndex);
        }
    }

    public void closeQuerySegmentStagesNextTierIndexDependants() {
        this.closeQuerySegmentStagesNextTierDependants();
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

    public void closeQuerySegmentStagesHasNextTierDependants() {
        this.closeMapQueryTieredEntryPresentDependants();
    }

    public void nextTier() {
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

    public void closeQuerySegmentStagesNextTierDependants() {
        this.closeMapQueryTieredEntryPresentDependants();
    }

    public void goToLastTier() {
        while (hasNextTier()) {
            nextTier();
        }
    }

    public void lowestPossiblyFreeChunk(long lowestPossiblyFreeChunk) {
        if ((tier()) == 0) {
            segmentHeader().lowestPossiblyFreeChunk(segmentHeaderAddress(), lowestPossiblyFreeChunk);
        } else {
            TierCountersArea.lowestPossiblyFreeChunkTiered(tierCountersAreaAddr(), lowestPossiblyFreeChunk);
        }
    }

    public long lowestPossiblyFreeChunk() {
        if ((tier()) == 0) {
            return segmentHeader().lowestPossiblyFreeChunk(segmentHeaderAddress());
        } else {
            return TierCountersArea.lowestPossiblyFreeChunkTiered(tierCountersAreaAddr());
        }
    }

    public void closeQuerySegmentStagesLowestPossiblyFreeChunkDependants() {
        this.closeSegment();
    }

    public void tierEntries(long tierEntries) {
        if ((tier()) == 0) {
            segmentHeader().entries(segmentHeaderAddress(), tierEntries);
        } else {
            TierCountersArea.entries(tierCountersAreaAddr(), tierEntries);
        }
    }

    public long tierEntries() {
        if ((tier()) == 0) {
            return segmentHeader().entries(segmentHeaderAddress());
        } else {
            return TierCountersArea.entries(tierCountersAreaAddr());
        }
    }

    public void closeQuerySegmentStagesTierEntriesDependants() {
        this.closeSegment();
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

    public void prevTier() {
        if ((tier()) == 0) {
            throw new IllegalStateException(((this.h().toIdentityString()) + ": first tier doesn\'t have previous"));
        } 
        initSegmentTier(((tier()) - 1), prevTierIndex());
    }

    public void goToFirstTier() {
        while ((tier()) != 0) {
            prevTier();
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
        
        int alreadyAttemptedTier = this.tier();
        this.goToFirstTier();
        while (true) {
            if ((this.tier()) != alreadyAttemptedTier) {
                ret = this.allocReturnCodeGuarded(chunks);
                if (ret >= 0)
                    return ret;
                
            } 
            this.nextTier();
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

    public boolean initEntryAndKey(long entrySize) {
        initAllocatedChunks(this.h().inChunks(entrySize));
        int tierBeforeAllocation = this.tier();
        long pos = this.alloc(allocatedChunks(), -1, 0);
        this.writeNewEntry(pos, this.inputKey());
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

    public void checkIterationContextNotLockedInThisThread() {
        if (this.rootContextInThisThread.iterationContextLockedInThisThread) {
            throw new IllegalStateException((((this.h().toIdentityString()) + ": Update or Write ") + "locking is forbidden in the context of locked iteration context"));
        } 
    }

    public void checkAccessingFromOwnerThread() {
        if ((owner) != (Thread.currentThread())) {
            throw new ConcurrentModificationException(((this.h().toIdentityString()) + ": Context shouldn\'t be accessed from multiple threads"));
        } 
    }

    public void closeOwnerThreadHolderCheckAccessingFromOwnerThreadDependants() {
        this.closeQueryCheckOnEachPublicOperationCheckOnEachLockOperationDependants();
    }

    public void checkOnEachLockOperation() {
        this.checkAccessingFromOwnerThread();
    }

    public void closeQueryCheckOnEachPublicOperationCheckOnEachLockOperationDependants() {
        this.innerReadLock.closeReadLockLockDependants();
        this.closeQueryCheckOnEachPublicOperationCheckOnEachPublicOperationDependants();
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

    public boolean nestedContextsLockedOnSameSegment() {
        if (!(this.locksInit()))
            this.initLocks();
        
        return this.nestedContextsLockedOnSameSegment;
    }

    public int contextModCount() {
        if (!(this.locksInit()))
            this.initLocks();
        
        return this.contextModCount;
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
        this.closeDelayedUpdateChecksum();
        this.innerReadLock.closeReadLockLockDependants();
        this.closeMapQueryDropSearchIfNestedContextsAndPresentHashLookupSlotCheckFailedDependants();
    }

    public boolean delayedUpdateChecksum = false;

    boolean delayedUpdateChecksumInit() {
        return (this.delayedUpdateChecksum);
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
        this.closeQueryHashLookupSearchFoundDependants();
        this.closeQueryHashLookupSearchNextPosDependants();
        this.closeQueryHashLookupSearchCheckSlotContainsExpectedKeyAndValueDependants();
    }

    public void found() {
        this.setHashLookupPosGuarded(hl().stepBack(this.hashLookupPos()));
    }

    public void closeQueryHashLookupSearchFoundDependants() {
        this.closeKeySearch();
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

    public void closeQueryHashLookupSearchNextPosDependants() {
        this.closeKeySearch();
    }

    protected CompiledMapQueryContext.SearchState searchState = null;

    public boolean keySearchInit() {
        return (this.searchState) != null;
    }

    public void initKeySearch() {
        boolean wasKeySearchInit = this.keySearchInit();
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
                searchState = CompiledMapQueryContext.SearchState.PRESENT;
                return ;
            } 
        }
        searchState = CompiledMapQueryContext.SearchState.ABSENT;
        if (wasKeySearchInit)
            this.closeKeySearchDependants();
        
    }

    public CompiledMapQueryContext.SearchState searchState() {
        if (!(this.keySearchInit()))
            this.initKeySearch();
        
        return this.searchState;
    }

    public void closeKeySearch() {
        if (!(this.keySearchInit()))
            return ;
        
        this.closeKeySearchDependants();
        this.searchState = null;
    }

    public void closeKeySearchDependants() {
        this.closeKeySearchSearchStatePresentDependants();
    }

    public boolean searchStateAbsent() {
        return (searchState()) == (CompiledMapQueryContext.SearchState.ABSENT);
    }

    public boolean searchStatePresent() {
        return (searchState()) == (CompiledMapQueryContext.SearchState.PRESENT);
    }

    public void closeKeySearchSearchStatePresentDependants() {
        this.closeMapQueryTieredEntryPresentDependants();
        this.closePresenceOfEntry();
        this.closeMapQueryDropSearchIfNestedContextsAndPresentHashLookupSlotCheckFailedDependants();
    }

    private boolean tieredEntryPresent() {
        int firstTier = this.tier();
        long firstTierBaseAddr = this.tierBaseAddr();
        while (true) {
            if (this.hasNextTier()) {
                this.nextTier();
            } else {
                if ((this.tier()) != 0)
                    this.initSegmentTier();
                
            }
            if ((this.tierBaseAddr()) == firstTierBaseAddr)
                break;
            
            if (this.searchStatePresent())
                return true;
            
        }
        if (firstTier != 0) {
            this.initSegmentTier();
        } 
        return false;
    }

    public void closeMapQueryTieredEntryPresentDependants() {
        this.closePresenceOfEntry();
    }

    private CompiledMapQueryContext.EntryPresence entryPresence = null;

    public boolean presenceOfEntryInit() {
        return (this.entryPresence) != null;
    }

    public void initPresenceOfEntry(CompiledMapQueryContext.EntryPresence entryPresence) {
        this.entryPresence = entryPresence;
    }

    private void initPresenceOfEntry() {
        if ((this.searchStatePresent()) || (tieredEntryPresent())) {
            entryPresence = CompiledMapQueryContext.EntryPresence.PRESENT;
        } else {
            entryPresence = CompiledMapQueryContext.EntryPresence.ABSENT;
        }
    }

    public CompiledMapQueryContext.EntryPresence entryPresence() {
        if (!(this.presenceOfEntryInit()))
            this.initPresenceOfEntry();
        
        return this.entryPresence;
    }

    public void closePresenceOfEntry() {
        this.entryPresence = null;
    }

    public boolean entryPresent() {
        return (entryPresence()) == (CompiledMapQueryContext.EntryPresence.PRESENT);
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

    void putEntry(Data<V> value) {
        assert this.searchStateAbsent();
        long entrySize = this.entrySize(this.inputKey().size(), value.size());
        this.initEntryAndKey(entrySize);
        this.initValue(value);
        this.freeExtraAllocatedChunks();
        this.putNewVolatile(this.pos());
    }

    public boolean checkSlotContainsExpectedKeyAndValue(long value) {
        long entry = hl().readEntry(addr(), this.hashLookupPos());
        return ((hl().key(entry)) == (searchKey())) && ((hl().value(entry)) == value);
    }

    public void closeQueryHashLookupSearchCheckSlotContainsExpectedKeyAndValueDependants() {
        this.closeMapQueryDropSearchIfNestedContextsAndPresentHashLookupSlotCheckFailedDependants();
    }

    public void remove() {
        this.setHashLookupPosGuarded(hl().remove(addr(), this.hashLookupPos()));
    }

    public void dropSearchIfNestedContextsAndPresentHashLookupSlotCheckFailed() {
        if (this.locksInit()) {
            if ((this.nestedContextsLockedOnSameSegment()) && ((this.rootContextLockedOnThisSegment().latestSameThreadSegmentModCount()) != (this.contextModCount()))) {
                if (((this.keySearchInit()) && (this.searchStatePresent())) && (!(this.checkSlotContainsExpectedKeyAndValue(this.pos())))) {
                    this.closeHashLookupPos();
                } 
            } 
        } 
    }

    public void closeMapQueryDropSearchIfNestedContextsAndPresentHashLookupSlotCheckFailedDependants() {
        this.closeQueryCheckOnEachPublicOperationCheckOnEachPublicOperationDependants();
    }

    public void checkOnEachPublicOperation() {
        _CheckOnEachPublicOperation_checkOnEachPublicOperation();
        this.dropSearchIfNestedContextsAndPresentHashLookupSlotCheckFailed();
    }

    public void closeQueryCheckOnEachPublicOperationCheckOnEachPublicOperationDependants() {
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
    public InterProcessLock updateLock() {
        this.checkOnEachPublicOperation();
        return this.innerUpdateLock;
    }

    @NotNull
    @Override
    public InterProcessLock readLock() {
        this.checkOnEachPublicOperation();
        return this.innerReadLock;
    }

    @NotNull
    @Override
    public Data<V> value() {
        this.checkOnEachPublicOperation();
        return this.entryValue;
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
    public InterProcessLock writeLock() {
        // The write-lock is final and thread-safe
        return this.innerWriteLock;
    }

    @Override
    public CompiledMapQueryContext<K, V, R> entry() {
        this.checkOnEachPublicOperation();
        return entryPresent() ? this : null;
    }

    @NotNull
    @Override
    public CompiledMapQueryContext<K, V, R> context() {
        this.checkOnEachPublicOperation();
        return this;
    }

    @Override
    public R insert(@NotNull
    MapAbsentEntry<K, V> absentEntry, Data<V> value) {
        this.checkOnEachPublicOperation();
        return this.m().entryOperations.insert(absentEntry, value);
    }

    @Override
    public Data<V> wrapValueAsData(V value) {
        this.checkOnEachPublicOperation();
        WrappedValueInstanceDataHolder wrapped = this.wrappedValueInstanceDataHolder;
        wrapped = wrapped.getUnusedWrappedValueHolderGuarded();
        wrapped.initValue(value);
        return wrapped.wrappedData();
    }

    public Data<K> queriedKey() {
        this.checkOnEachPublicOperation();
        return this.inputKey();
    }

    @Override
    public Data<V> defaultValue(@NotNull
    MapAbsentEntry<K, V> absentEntry) {
        this.checkOnEachPublicOperation();
        return this.m().defaultValueProvider.defaultValue(absentEntry);
    }

    @NotNull
    @Override
    public Data<K> absentKey() {
        this.checkOnEachPublicOperation();
        return this.inputKey();
    }

    @Nullable
    @Override
    public Absent<K, V> absentEntry() {
        this.checkOnEachPublicOperation();
        return entryPresent() ? null : this;
    }

    @NotNull
    @Override
    public Data<K> key() {
        this.checkOnEachPublicOperation();
        return this.entryKey;
    }

    @NotNull
    public Data<V> defaultValue() {
        this.checkOnEachPublicOperation();
        return this.zeroValueData;
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

    protected void putPrefix() {
        this.checkOnEachPublicOperation();
        if (!(this.innerUpdateLock.isHeldByCurrentThread()))
            this.innerUpdateLock.lock();
        
        if ((this.nestedContextsLockedOnSameSegment()) && ((this.rootContextLockedOnThisSegment().latestSameThreadSegmentModCount()) != (this.contextModCount()))) {
            if ((this.hashLookupPosInit()) && (this.searchStateAbsent()))
                this.closeHashLookupPos();
            
        } 
    }

    @Override
    public void doInsert(Data<V> value) {
        this.putPrefix();
        if (!(this.entryPresent())) {
            putEntry(value);
            this.incrementModCountGuarded();
            this.setSearchStateGuarded(CompiledMapQueryContext.SearchState.PRESENT);
            this.initPresenceOfEntry(CompiledMapQueryContext.EntryPresence.PRESENT);
        } else {
            throw new IllegalStateException(((this.h().toIdentityString()) + ": Entry is present in the map when doInsert() is called"));
        }
    }

    @Override
    public void doInsert() {
        if ((this.set()) == null)
            throw new IllegalStateException(((this.h().toIdentityString()) + ": Called SetAbsentEntry.doInsert() from Map context"));
        
        doInsert(((Data<V>)(DummyValueData.INSTANCE)));
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
            VanillaChronicleMap<?, ?, ?> m = this.m();
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
        } else {
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
        putPrefix();
        if (entryPresent()) {
            this.innerDefaultReplaceValue(newValue);
            this.incrementModCountGuarded();
            this.setSearchStateGuarded(CompiledMapQueryContext.SearchState.PRESENT);
            initPresenceOfEntry(CompiledMapQueryContext.EntryPresence.PRESENT);
        } else {
            throw new IllegalStateException(((this.h().toIdentityString()) + ": Entry is absent in the map when doReplaceValue() is called"));
        }
    }

    public void innerRemoveEntryExceptHashLookupUpdate() {
        this.freeGuarded(pos(), entrySizeInChunks());
        this.incrementModCountGuarded();
    }

    @Override
    public void doRemove() {
        this.checkOnEachPublicOperation();
        this.innerWriteLock.lock();
        if (this.searchStatePresent()) {
            this.innerRemoveEntryExceptHashLookupUpdate();
            this.remove();
            this.setSearchStateGuarded(CompiledMapQueryContext.SearchState.ABSENT);
            initPresenceOfEntry(CompiledMapQueryContext.EntryPresence.ABSENT);
        } else {
            throw new IllegalStateException(((this.h().toIdentityString()) + ": Entry is absent when doRemove() is called"));
        }
    }
}