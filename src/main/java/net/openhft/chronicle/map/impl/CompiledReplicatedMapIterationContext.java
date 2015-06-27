package net.openhft.chronicle.map.impl;

import net.openhft.chronicle.bytes.Access;
import net.openhft.chronicle.bytes.Accessor;
import net.openhft.chronicle.bytes.Accessor.Full;
import net.openhft.chronicle.bytes.ReadAccess;
import net.openhft.chronicle.hash.AbstractData;
import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.HashEntry;
import net.openhft.chronicle.hash.impl.BigSegmentHeader;
import net.openhft.chronicle.hash.impl.CopyingInstanceData;
import net.openhft.chronicle.hash.impl.JavaLangBytesAccessors;
import net.openhft.chronicle.hash.impl.LocalLockState;
import net.openhft.chronicle.hash.impl.SegmentHeader;
import net.openhft.chronicle.hash.impl.VanillaChronicleHash;
import net.openhft.chronicle.hash.impl.hashlookup.EntryConsumer;
import net.openhft.chronicle.hash.locks.InterProcessLock;
import net.openhft.chronicle.hash.locks.InterProcessReadWriteUpdateLock;
import net.openhft.chronicle.hash.replication.RemoteOperationContext;
import net.openhft.chronicle.hash.replication.ReplicableEntry;
import net.openhft.chronicle.hash.replication.TimeProvider;
import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.chronicle.hash.serialization.internal.MetaBytesInterop;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.MapAbsentEntry;
import net.openhft.chronicle.map.MapContext;
import net.openhft.chronicle.map.MapEntry;
import net.openhft.chronicle.map.MapKeyContext;
import net.openhft.chronicle.map.ReplicatedChronicleMap;
import net.openhft.chronicle.map.VanillaChronicleMap;
import net.openhft.chronicle.map.replication.MapReplicableEntry;
import net.openhft.lang.Maths;
import net.openhft.lang.MemoryUnit;
import net.openhft.lang.collection.DirectBitSet;
import net.openhft.lang.collection.SingleThreadedDirectBitSet;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.DirectBytes;
import net.openhft.lang.io.MultiStoreBytes;
import net.openhft.lang.io.NativeBytes;
import net.openhft.lang.threadlocal.ThreadLocalCopies;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;

public class CompiledReplicatedMapIterationContext<K, KI, MKI extends MetaBytesInterop<K, ? super KI>, V, VI, MVI extends MetaBytesInterop<V, ? super VI>, R, T> implements AutoCloseable , HashEntry<K> , InterProcessReadWriteUpdateLock , RemoteOperationContext<K> , ReplicableEntry , MapContext<K, V, R> , MapEntry<K, V> , IterationContextInterface<K, V> , ReplicatedChronicleMapHolder<K, KI, MKI, V, VI, MVI, R> , ReplicatedIterationContextInterface<K, V> , MapReplicableEntry<K, V> {
    public void close() {
        CompiledReplicatedMapIterationContext.this.closePos();
        CompiledReplicatedMapIterationContext.this.wrappedValueInstanceValue.closeValue();
        CompiledReplicatedMapIterationContext.this.closeReplicationUpdate();
        CompiledReplicatedMapIterationContext.this.wrappedValueInstanceValue.closeNext();
        CompiledReplicatedMapIterationContext.this.closeKeyOffset();
        CompiledReplicatedMapIterationContext.this.closeEntryRemovedOnThisIteration();
        CompiledReplicatedMapIterationContext.this.closeHashLookupPos();
        CompiledReplicatedMapIterationContext.this.closeUsed();
        CompiledReplicatedMapIterationContext.this.closeKeySize();
        CompiledReplicatedMapIterationContext.this.closeTheSegmentIndex();
        CompiledReplicatedMapIterationContext.this.closeAllocatedChunks();
        CompiledReplicatedMapIterationContext.this.closeReplicatedMapSegmentIterationCheckEntryNotRemovedOnThisIterationDependants();
        CompiledReplicatedMapIterationContext.this.closeOwnerThreadHolderCheckAccessingFromOwnerThreadDependants();
        CompiledReplicatedMapIterationContext.this.closeValueBytesInteropValueMetaInteropDependants();
        CompiledReplicatedMapIterationContext.this.closeReplicatedChronicleMapHolderImplContextAtIndexInChainDependants();
    }

    public void incrementModCountGuarded() {
        if (!(this.locksInit()))
            this.initLocks();

        incrementModCount();
    }

    public void setHashLookupPosGuarded(long hashLookupPos) {
        assert this.hashLookupPosInit() : "HashLookupPos should be init";
        setHashLookupPos(hashLookupPos);
    }

    public void setLocalLockStateGuarded(LocalLockState newState) {
        if (!(this.locksInit()))
            this.initLocks();

        setLocalLockState(newState);
    }

    public CompiledReplicatedMapIterationContext(ReplicatedChronicleMap<K, KI, MKI, V, VI, MVI, R> m) {
        contextChain = new ArrayList<CompiledReplicatedMapIterationContext>();
        contextChain.add(this);
        indexInContextChain = 0;
        this.m = m;
        this.entryBytes = CompiledReplicatedMapIterationContext.this.h().ms.bytes();
        this.entryBytesAccessor = JavaLangBytesAccessors.uncheckedBytesAccessor(entryBytes);
        this.entryBytesAccessHandle = ((T)(entryBytesAccessor.handle(entryBytes)));
        this.entryBytesAccess = ((Access<T>)(entryBytesAccessor.access(entryBytes)));
        this.entryKey = new EntryKeyBytesData();
        this.wrappedValueInstanceValue = new WrappedValueInstanceData();
        this.innerWriteLock = new WriteLock();
        this.owner = Thread.currentThread();
        this.copies = ThreadLocalCopies.get();
        this.valueInterop = CompiledReplicatedMapIterationContext.this.m().valueInteropProvider.get(CompiledReplicatedMapIterationContext.this.copies, CompiledReplicatedMapIterationContext.this.m().originalValueInterop);
        this.valueReader = CompiledReplicatedMapIterationContext.this.m().valueReaderProvider.get(CompiledReplicatedMapIterationContext.this.copies, CompiledReplicatedMapIterationContext.this.m().originalValueReader);
        this.keyInterop = CompiledReplicatedMapIterationContext.this.h().keyInteropProvider.get(CompiledReplicatedMapIterationContext.this.copies, CompiledReplicatedMapIterationContext.this.h().originalKeyInterop);
        this.keyReader = CompiledReplicatedMapIterationContext.this.h().keyReaderProvider.get(CompiledReplicatedMapIterationContext.this.copies, CompiledReplicatedMapIterationContext.this.h().originalKeyReader);
        this.innerUpdateLock = new UpdateLock();
        this.deprecatedMapKeyContextOnIteration = new DeprecatedMapKeyContextOnIteration();
        this.entryValue = new EntryValueBytesData();
        this.dummyValue = new DummyValueZeroData();
        this.innerReadLock = new ReadLock();
    }

    public CompiledReplicatedMapIterationContext(CompiledReplicatedMapIterationContext c) {
        contextChain = c.contextChain;
        indexInContextChain = contextChain.size();
        contextChain.add(this);
        this.m = ((ReplicatedChronicleMap<K, KI, MKI, V, VI, MVI, R>)(c.m));
        this.entryBytes = CompiledReplicatedMapIterationContext.this.h().ms.bytes();
        this.entryBytesAccessor = JavaLangBytesAccessors.uncheckedBytesAccessor(entryBytes);
        this.entryBytesAccessHandle = ((T)(entryBytesAccessor.handle(entryBytes)));
        this.entryBytesAccess = ((Access<T>)(entryBytesAccessor.access(entryBytes)));
        this.entryKey = new EntryKeyBytesData();
        this.wrappedValueInstanceValue = new WrappedValueInstanceData();
        this.innerWriteLock = new WriteLock();
        this.owner = Thread.currentThread();
        this.copies = ThreadLocalCopies.get();
        this.valueInterop = CompiledReplicatedMapIterationContext.this.m().valueInteropProvider.get(CompiledReplicatedMapIterationContext.this.copies, CompiledReplicatedMapIterationContext.this.m().originalValueInterop);
        this.valueReader = CompiledReplicatedMapIterationContext.this.m().valueReaderProvider.get(CompiledReplicatedMapIterationContext.this.copies, CompiledReplicatedMapIterationContext.this.m().originalValueReader);
        this.keyInterop = CompiledReplicatedMapIterationContext.this.h().keyInteropProvider.get(CompiledReplicatedMapIterationContext.this.copies, CompiledReplicatedMapIterationContext.this.h().originalKeyInterop);
        this.keyReader = CompiledReplicatedMapIterationContext.this.h().keyReaderProvider.get(CompiledReplicatedMapIterationContext.this.copies, CompiledReplicatedMapIterationContext.this.h().originalKeyReader);
        this.innerUpdateLock = new UpdateLock();
        this.deprecatedMapKeyContextOnIteration = new DeprecatedMapKeyContextOnIteration();
        this.entryValue = new EntryValueBytesData();
        this.dummyValue = new DummyValueZeroData();
        this.innerReadLock = new ReadLock();
    }

    public class DeprecatedMapKeyContextOnIteration implements MapKeyContext<K, V> {
        @NotNull
        private UnsupportedOperationException unsupportedLocks() {
            return new UnsupportedOperationException("Lock operations are not supported (and not needed!) during iteration");
        }

        @NotNull
        @Override
        public InterProcessLock writeLock() {
            throw unsupportedLocks();
        }

        @NotNull
        @Override
        public InterProcessLock updateLock() {
            throw unsupportedLocks();
        }

        @NotNull
        @Override
        public InterProcessLock readLock() {
            throw unsupportedLocks();
        }

        @Override
        public V get() {
            CompiledReplicatedMapIterationContext.this.checkOnEachPublicOperation();
            return CompiledReplicatedMapIterationContext.this.value().get();
        }

        @Override
        public boolean valueEqualTo(V value) {
            CompiledReplicatedMapIterationContext.this.checkOnEachPublicOperation();
            return Data.bytesEquivalent(CompiledReplicatedMapIterationContext.this.entryValue, CompiledReplicatedMapIterationContext.this.context().wrapValueAsValue(value));
        }

        @Override
        public boolean containsKey() {
            CompiledReplicatedMapIterationContext.this.checkOnEachPublicOperation();
            return true;
        }

        @Override
        public V getUsing(V usingValue) {
            CompiledReplicatedMapIterationContext.this.checkOnEachPublicOperation();
            return CompiledReplicatedMapIterationContext.this.value().getUsing(usingValue);
        }

        @NotNull
        @Override
        public K key() {
            CompiledReplicatedMapIterationContext.this.checkOnEachPublicOperation();
            return CompiledReplicatedMapIterationContext.this.key().get();
        }

        @NotNull
        @Override
        public Bytes entry() {
            CompiledReplicatedMapIterationContext.this.checkOnEachPublicOperation();
            return CompiledReplicatedMapIterationContext.this.entryBytes;
        }

        @Override
        public boolean remove() {
            CompiledReplicatedMapIterationContext.this.checkOnEachPublicOperation();
            CompiledReplicatedMapIterationContext.this.remove(CompiledReplicatedMapIterationContext.this);
            return true;
        }

        @Override
        public boolean put(V newValue) {
            CompiledReplicatedMapIterationContext.this.checkOnEachPublicOperation();
            CompiledReplicatedMapIterationContext.this.replaceValue(CompiledReplicatedMapIterationContext.this, CompiledReplicatedMapIterationContext.this.context().wrapValueAsValue(newValue));
            return true;
        }

        @Override
        public void close() {
            throw new UnsupportedOperationException("close() is not supported during iteration");
        }

        @Override
        public long keySize() {
            CompiledReplicatedMapIterationContext.this.checkOnEachPublicOperation();
            return CompiledReplicatedMapIterationContext.this.keySize();
        }

        @Override
        public long valueSize() {
            CompiledReplicatedMapIterationContext.this.checkOnEachPublicOperation();
            return CompiledReplicatedMapIterationContext.this.valueSize();
        }

        @Override
        public long valueOffset() {
            CompiledReplicatedMapIterationContext.this.checkOnEachPublicOperation();
            return CompiledReplicatedMapIterationContext.this.valueOffset();
        }

        @Override
        public long keyOffset() {
            CompiledReplicatedMapIterationContext.this.checkOnEachPublicOperation();
            return CompiledReplicatedMapIterationContext.this.keyOffset();
        }
    }

    public class DummyValueZeroData extends AbstractData<V, Void> {
        @Override
        public V get() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Void handle() {
            return null;
        }

        @Override
        public long offset() {
            return 0;
        }

        @Override
        public ReadAccess<Void> access() {
            return ReadAccess.zeros();
        }

        @Override
        public V getUsing(V usingInstance) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long size() {
            return CompiledReplicatedMapIterationContext.this.m().valueSizeMarshaller.minEncodableSize();
        }
    }

    public class EntryKeyBytesData extends AbstractData<K, T> {
        @Override
        public ReadAccess<T> access() {
            CompiledReplicatedMapIterationContext.this.checkOnEachPublicOperation();
            return CompiledReplicatedMapIterationContext.this.entryBytesAccess;
        }

        @Override
        public T handle() {
            CompiledReplicatedMapIterationContext.this.checkOnEachPublicOperation();
            return CompiledReplicatedMapIterationContext.this.entryBytesAccessHandle;
        }

        @Override
        public long size() {
            CompiledReplicatedMapIterationContext.this.checkOnEachPublicOperation();
            return CompiledReplicatedMapIterationContext.this.keySize();
        }

        public void closeEntryKeyBytesDataSizeDependants() {
            EntryKeyBytesData.this.closeEntryKeyBytesDataInnerGetUsingDependants();
        }

        private K innerGetUsing(K usingKey) {
            CompiledReplicatedMapIterationContext.this.entryBytes.position(CompiledReplicatedMapIterationContext.this.keyOffset());
            return CompiledReplicatedMapIterationContext.this.keyReader.read(CompiledReplicatedMapIterationContext.this.entryBytes, size(), usingKey);
        }

        public void closeEntryKeyBytesDataInnerGetUsingDependants() {
            EntryKeyBytesData.this.closeCachedEntryKey();
        }

        @Override
        public K getUsing(K usingKey) {
            CompiledReplicatedMapIterationContext.this.checkOnEachPublicOperation();
            return innerGetUsing(usingKey);
        }

        private K cachedEntryKey;

        private boolean cachedEntryKeyRead = false;

        public boolean cachedEntryKeyInit() {
            return (this.cachedEntryKeyRead) != false;
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
            if (!(this.cachedEntryKeyInit()))
                return ;

            this.cachedEntryKeyRead = false;
        }

        @Override
        public K get() {
            CompiledReplicatedMapIterationContext.this.checkOnEachPublicOperation();
            return cachedEntryKey();
        }

        @Override
        public long offset() {
            CompiledReplicatedMapIterationContext.this.checkOnEachPublicOperation();
            return CompiledReplicatedMapIterationContext.this.entryBytesAccessOffset(CompiledReplicatedMapIterationContext.this.keyOffset());
        }
    }

    public class EntryValueBytesData extends AbstractData<V, T> {
        @Override
        public T handle() {
            CompiledReplicatedMapIterationContext.this.checkOnEachPublicOperation();
            return CompiledReplicatedMapIterationContext.this.entryBytesAccessHandle;
        }

        @Override
        public ReadAccess<T> access() {
            CompiledReplicatedMapIterationContext.this.checkOnEachPublicOperation();
            return CompiledReplicatedMapIterationContext.this.entryBytesAccess;
        }

        @Override
        public long size() {
            CompiledReplicatedMapIterationContext.this.checkOnEachPublicOperation();
            return CompiledReplicatedMapIterationContext.this.valueSize();
        }

        public void closeEntryValueBytesDataSizeDependants() {
            EntryValueBytesData.this.closeEntryValueBytesDataInnerGetUsingDependants();
        }

        private V innerGetUsing(V usingValue) {
            CompiledReplicatedMapIterationContext.this.entryBytes.position(CompiledReplicatedMapIterationContext.this.valueOffset());
            return CompiledReplicatedMapIterationContext.this.valueReader.read(CompiledReplicatedMapIterationContext.this.entryBytes, size(), usingValue);
        }

        public void closeEntryValueBytesDataInnerGetUsingDependants() {
            EntryValueBytesData.this.closeCachedEntryValue();
        }

        @Override
        public V getUsing(V usingValue) {
            CompiledReplicatedMapIterationContext.this.checkOnEachPublicOperation();
            return innerGetUsing(usingValue);
        }

        private V cachedEntryValue;

        private boolean cachedEntryValueRead = false;

        public boolean cachedEntryValueInit() {
            return (this.cachedEntryValueRead) != false;
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

            this.cachedEntryValueRead = false;
        }

        @Override
        public V get() {
            CompiledReplicatedMapIterationContext.this.checkOnEachPublicOperation();
            return cachedEntryValue();
        }

        @Override
        public long offset() {
            CompiledReplicatedMapIterationContext.this.checkOnEachPublicOperation();
            return CompiledReplicatedMapIterationContext.this.entryBytesAccessOffset(CompiledReplicatedMapIterationContext.this.valueOffset());
        }
    }

    public class ReadLock implements InterProcessLock {
        @Override
        public boolean tryLock() {
            if ((CompiledReplicatedMapIterationContext.this.localLockState()) == (LocalLockState.UNLOCKED)) {
                if (CompiledReplicatedMapIterationContext.this.segmentHeader().tryReadLock(CompiledReplicatedMapIterationContext.this.segmentHeaderAddress())) {
                    CompiledReplicatedMapIterationContext.this.setLocalLockStateGuarded(LocalLockState.READ_LOCKED);
                    return true;
                } else {
                    return false;
                }
            } else {
                return true;
            }
        }

        @Override
        public void lock() {
            if ((CompiledReplicatedMapIterationContext.this.localLockState()) == (LocalLockState.UNLOCKED)) {
                CompiledReplicatedMapIterationContext.this.segmentHeader().readLock(CompiledReplicatedMapIterationContext.this.segmentHeaderAddress());
                CompiledReplicatedMapIterationContext.this.setLocalLockStateGuarded(LocalLockState.READ_LOCKED);
            }
        }

        @Override
        public void lockInterruptibly() throws InterruptedException {
            if ((CompiledReplicatedMapIterationContext.this.localLockState()) == (LocalLockState.UNLOCKED)) {
                CompiledReplicatedMapIterationContext.this.segmentHeader().readLockInterruptibly(CompiledReplicatedMapIterationContext.this.segmentHeaderAddress());
                CompiledReplicatedMapIterationContext.this.setLocalLockStateGuarded(LocalLockState.READ_LOCKED);
            }
        }

        @Override
        public boolean tryLock(long time, @NotNull
        TimeUnit unit) throws InterruptedException {
            if ((CompiledReplicatedMapIterationContext.this.localLockState()) == (LocalLockState.UNLOCKED)) {
                if (CompiledReplicatedMapIterationContext.this.segmentHeader().tryReadLock(CompiledReplicatedMapIterationContext.this.segmentHeaderAddress(), time, unit)) {
                    CompiledReplicatedMapIterationContext.this.setLocalLockStateGuarded(LocalLockState.READ_LOCKED);
                    return true;
                } else {
                    return false;
                }
            } else {
                return true;
            }
        }

        @Override
        public void unlock() {
            switch (CompiledReplicatedMapIterationContext.this.localLockState()) {
                case UNLOCKED :
                    return ;
                case READ_LOCKED :
                    CompiledReplicatedMapIterationContext.this.segmentHeader().readUnlock(CompiledReplicatedMapIterationContext.this.segmentHeaderAddress());
                    break;
                case UPDATE_LOCKED :
                    CompiledReplicatedMapIterationContext.this.segmentHeader().updateUnlock(CompiledReplicatedMapIterationContext.this.segmentHeaderAddress());
                    break;
                case WRITE_LOCKED :
                    CompiledReplicatedMapIterationContext.this.segmentHeader().writeUnlock(CompiledReplicatedMapIterationContext.this.segmentHeaderAddress());
            }
            CompiledReplicatedMapIterationContext.this.setLocalLockStateGuarded(LocalLockState.UNLOCKED);
            CompiledReplicatedMapIterationContext.this.closeHashLookupPos();
            CompiledReplicatedMapIterationContext.this.closePos();
        }

        @Override
        public boolean isHeldByCurrentThread() {
            return CompiledReplicatedMapIterationContext.this.localLockState().read;
        }
    }

    public class UpdateLock implements InterProcessLock {
        @NotNull
        private IllegalMonitorStateException forbiddenUpgrade() {
            return new IllegalMonitorStateException("Cannot upgrade from read to update lock");
        }

        @Override
        public boolean tryLock() {
            switch (CompiledReplicatedMapIterationContext.this.localLockState()) {
                case UNLOCKED :
                    if (CompiledReplicatedMapIterationContext.this.segmentHeader().tryUpdateLock(CompiledReplicatedMapIterationContext.this.segmentHeaderAddress())) {
                        CompiledReplicatedMapIterationContext.this.setLocalLockStateGuarded(LocalLockState.UPDATE_LOCKED);
                        return true;
                    } else {
                        return false;
                    }
                case READ_LOCKED :
                    throw forbiddenUpgrade();
                case UPDATE_LOCKED :
                case WRITE_LOCKED :
                    return true;
            }
            throw new AssertionError();
        }

        @Override
        public void lockInterruptibly() throws InterruptedException {
            switch (CompiledReplicatedMapIterationContext.this.localLockState()) {
                case UNLOCKED :
                    CompiledReplicatedMapIterationContext.this.segmentHeader().updateLockInterruptibly(CompiledReplicatedMapIterationContext.this.segmentHeaderAddress());
                    CompiledReplicatedMapIterationContext.this.setLocalLockStateGuarded(LocalLockState.UPDATE_LOCKED);
                    return ;
                case READ_LOCKED :
                    throw forbiddenUpgrade();
                case UPDATE_LOCKED :
                case WRITE_LOCKED :
            }
        }

        @Override
        public boolean isHeldByCurrentThread() {
            return CompiledReplicatedMapIterationContext.this.localLockState().update;
        }

        @Override
        public void unlock() {
            switch (CompiledReplicatedMapIterationContext.this.localLockState()) {
                case UNLOCKED :
                case READ_LOCKED :
                    return ;
                case UPDATE_LOCKED :
                    CompiledReplicatedMapIterationContext.this.segmentHeader().downgradeUpdateToReadLock(CompiledReplicatedMapIterationContext.this.segmentHeaderAddress());
                    break;
                case WRITE_LOCKED :
                    CompiledReplicatedMapIterationContext.this.segmentHeader().downgradeWriteToReadLock(CompiledReplicatedMapIterationContext.this.segmentHeaderAddress());
            }
            CompiledReplicatedMapIterationContext.this.setLocalLockStateGuarded(LocalLockState.READ_LOCKED);
        }

        @Override
        public boolean tryLock(long time, @NotNull
        TimeUnit unit) throws InterruptedException {
            switch (CompiledReplicatedMapIterationContext.this.localLockState()) {
                case UNLOCKED :
                    if (CompiledReplicatedMapIterationContext.this.segmentHeader().tryUpdateLock(CompiledReplicatedMapIterationContext.this.segmentHeaderAddress(), time, unit)) {
                        CompiledReplicatedMapIterationContext.this.setLocalLockStateGuarded(LocalLockState.UPDATE_LOCKED);
                        return true;
                    } else {
                        return false;
                    }
                case READ_LOCKED :
                    throw forbiddenUpgrade();
                case UPDATE_LOCKED :
                case WRITE_LOCKED :
                    return true;
            }
            throw new AssertionError();
        }

        @Override
        public void lock() {
            switch (CompiledReplicatedMapIterationContext.this.localLockState()) {
                case UNLOCKED :
                    CompiledReplicatedMapIterationContext.this.segmentHeader().updateLock(CompiledReplicatedMapIterationContext.this.segmentHeaderAddress());
                    CompiledReplicatedMapIterationContext.this.setLocalLockStateGuarded(LocalLockState.UPDATE_LOCKED);
                    return ;
                case READ_LOCKED :
                    throw forbiddenUpgrade();
                case UPDATE_LOCKED :
                case WRITE_LOCKED :
            }
        }
    }

    public class WrappedValueInstanceData extends CopyingInstanceData<V, T> {
        public WrappedValueInstanceData getUnusedWrappedValueGuarded() {
            assert this.nextInit() : "Next should be init";
            return getUnusedWrappedValue();
        }

        public WrappedValueInstanceData getUnusedWrappedValue() {
            if (!(valueInit()))
                return this;

            if ((next) == null)
                next = new WrappedValueInstanceData();

            return next.getUnusedWrappedValue();
        }

        private WrappedValueInstanceData next;

        boolean nextInit() {
            return true;
        }

        void closeNext() {
            if (!(this.nextInit()))
                return ;

        }

        private V value;

        public boolean valueInit() {
            return (value) != null;
        }

        public void initValue(V value) {
            CompiledReplicatedMapIterationContext.this.m().checkValue(value);
            this.value = value;
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
            if ((next) != null)
                next.closeValue();

        }

        public void closeValueDependants() {
            WrappedValueInstanceData.this.closeBuffer();
        }

        @Override
        public V instance() {
            return value();
        }

        private boolean marshalled = false;

        private DirectBytes buf;

        public boolean bufferInit() {
            return (this.marshalled) != false;
        }

        private void initBuffer() {
            MVI mvi = CompiledReplicatedMapIterationContext.this.valueMetaInterop(value());
            long size = mvi.size(CompiledReplicatedMapIterationContext.this.valueInterop, value());
            buf = CopyingInstanceData.getBuffer(this.buf, size);
            mvi.write(CompiledReplicatedMapIterationContext.this.valueInterop, buf, value());
            buf.flip();
            marshalled = true;
        }

        public DirectBytes buf() {
            if (!(this.bufferInit()))
                this.initBuffer();

            return this.buf;
        }

        public void closeBuffer() {
            if (!(this.bufferInit()))
                return ;

            this.marshalled = false;
        }

        @Override
        public DirectBytes buffer() {
            return buf();
        }

        @Override
        public V getUsing(V usingValue) {
            buf().position(0);
            return CompiledReplicatedMapIterationContext.this.valueReader.read(buf(), buf().limit(), usingValue);
        }
    }

    public class WriteLock implements InterProcessLock {
        @NotNull
        private IllegalMonitorStateException forbiddenUpgrade() {
            return new IllegalMonitorStateException("Cannot upgrade from read to write lock");
        }

        @Override
        public boolean isHeldByCurrentThread() {
            return CompiledReplicatedMapIterationContext.this.localLockState().write;
        }

        @Override
        public boolean tryLock() {
            switch (CompiledReplicatedMapIterationContext.this.localLockState()) {
                case UNLOCKED :
                    if (CompiledReplicatedMapIterationContext.this.segmentHeader().tryWriteLock(CompiledReplicatedMapIterationContext.this.segmentHeaderAddress())) {
                        CompiledReplicatedMapIterationContext.this.setLocalLockStateGuarded(LocalLockState.WRITE_LOCKED);
                        return true;
                    } else {
                        return false;
                    }
                case READ_LOCKED :
                    throw forbiddenUpgrade();
                case UPDATE_LOCKED :
                    if (CompiledReplicatedMapIterationContext.this.segmentHeader().tryUpgradeUpdateToWriteLock(CompiledReplicatedMapIterationContext.this.segmentHeaderAddress())) {
                        CompiledReplicatedMapIterationContext.this.setLocalLockStateGuarded(LocalLockState.WRITE_LOCKED);
                        return true;
                    } else {
                        return false;
                    }
                case WRITE_LOCKED :
                    return true;
            }
            throw new AssertionError();
        }

        @Override
        public void unlock() {
            switch (CompiledReplicatedMapIterationContext.this.localLockState()) {
                case UNLOCKED :
                case READ_LOCKED :
                case UPDATE_LOCKED :
                    return ;
                case WRITE_LOCKED :
                    CompiledReplicatedMapIterationContext.this.segmentHeader().downgradeWriteToUpdateLock(CompiledReplicatedMapIterationContext.this.segmentHeaderAddress());
            }
            CompiledReplicatedMapIterationContext.this.setLocalLockStateGuarded(LocalLockState.UPDATE_LOCKED);
        }

        @Override
        public void lock() {
            switch (CompiledReplicatedMapIterationContext.this.localLockState()) {
                case UNLOCKED :
                    CompiledReplicatedMapIterationContext.this.segmentHeader().writeLock(CompiledReplicatedMapIterationContext.this.segmentHeaderAddress());
                    CompiledReplicatedMapIterationContext.this.setLocalLockStateGuarded(LocalLockState.WRITE_LOCKED);
                    return ;
                case READ_LOCKED :
                    throw forbiddenUpgrade();
                case UPDATE_LOCKED :
                    CompiledReplicatedMapIterationContext.this.segmentHeader().upgradeUpdateToWriteLock(CompiledReplicatedMapIterationContext.this.segmentHeaderAddress());
                    CompiledReplicatedMapIterationContext.this.setLocalLockStateGuarded(LocalLockState.WRITE_LOCKED);
                case WRITE_LOCKED :
            }
        }

        @Override
        public void lockInterruptibly() throws InterruptedException {
            switch (CompiledReplicatedMapIterationContext.this.localLockState()) {
                case UNLOCKED :
                    CompiledReplicatedMapIterationContext.this.segmentHeader().writeLockInterruptibly(CompiledReplicatedMapIterationContext.this.segmentHeaderAddress());
                    CompiledReplicatedMapIterationContext.this.setLocalLockStateGuarded(LocalLockState.WRITE_LOCKED);
                    return ;
                case READ_LOCKED :
                    throw forbiddenUpgrade();
                case UPDATE_LOCKED :
                    CompiledReplicatedMapIterationContext.this.segmentHeader().upgradeUpdateToWriteLockInterruptibly(CompiledReplicatedMapIterationContext.this.segmentHeaderAddress());
                    CompiledReplicatedMapIterationContext.this.setLocalLockStateGuarded(LocalLockState.WRITE_LOCKED);
                case WRITE_LOCKED :
            }
        }

        @Override
        public boolean tryLock(long time, @NotNull
        TimeUnit unit) throws InterruptedException {
            switch (CompiledReplicatedMapIterationContext.this.localLockState()) {
                case UNLOCKED :
                    if (CompiledReplicatedMapIterationContext.this.segmentHeader().tryWriteLock(CompiledReplicatedMapIterationContext.this.segmentHeaderAddress(), time, unit)) {
                        CompiledReplicatedMapIterationContext.this.setLocalLockStateGuarded(LocalLockState.WRITE_LOCKED);
                        return true;
                    } else {
                        return false;
                    }
                case READ_LOCKED :
                    throw forbiddenUpgrade();
                case UPDATE_LOCKED :
                    if (CompiledReplicatedMapIterationContext.this.segmentHeader().tryUpgradeUpdateToWriteLock(CompiledReplicatedMapIterationContext.this.segmentHeaderAddress(), time, unit)) {
                        CompiledReplicatedMapIterationContext.this.setLocalLockStateGuarded(LocalLockState.WRITE_LOCKED);
                        return true;
                    } else {
                        return false;
                    }
                case WRITE_LOCKED :
                    return true;
            }
            throw new AssertionError();
        }
    }

    public void incrementModCount() {
        contextModCount = rootContextOnThisSegment.latestSameThreadSegmentModCount = (rootContextOnThisSegment.latestSameThreadSegmentModCount) + 1;
    }

    public void setHashLookupPos(long hashLookupPos) {
        this.hashLookupPos = hashLookupPos;
    }

    public void setLocalLockState(LocalLockState newState) {
        localLockState = newState;
    }

    final Thread owner;

    public Thread owner() {
        return this.owner;
    }

    private void closeNestedLocks() {
        unlinkFromSegmentContextsChain();
        switch (localLockState) {
            case UNLOCKED :
                break;
            case READ_LOCKED :
                int newTotalReadLockCount = this.rootContextOnThisSegment.totalReadLockCount -= 1;
                if (newTotalReadLockCount == 0) {
                    if (((this.rootContextOnThisSegment.totalUpdateLockCount) == 0) && ((this.rootContextOnThisSegment.totalWriteLockCount) == 0)) {
                        segmentHeader().readUnlock(segmentHeaderAddress());
                    }
                } else if (newTotalReadLockCount < 0) {
                    throw new IllegalStateException("read underflow");
                }
                break;
            case UPDATE_LOCKED :
                int newTotalUpdateLockCount = this.rootContextOnThisSegment.totalUpdateLockCount -= 1;
                if (newTotalUpdateLockCount == 0) {
                    if ((this.rootContextOnThisSegment.totalWriteLockCount) == 0) {
                        if ((this.rootContextOnThisSegment.totalReadLockCount) == 0) {
                            segmentHeader().updateUnlock(segmentHeaderAddress());
                        } else {
                            segmentHeader().downgradeUpdateToReadLock(segmentHeaderAddress());
                        }
                    }
                } else if (newTotalUpdateLockCount < 0) {
                    throw new IllegalStateException("update underflow");
                }
                break;
            case WRITE_LOCKED :
                int newTotalWriteLockCount = this.rootContextOnThisSegment.totalWriteLockCount -= 1;
                if (newTotalWriteLockCount == 0) {
                    if ((this.rootContextOnThisSegment.totalUpdateLockCount) > 0) {
                        segmentHeader().downgradeWriteToUpdateLock(segmentHeaderAddress());
                    } else {
                        if ((this.rootContextOnThisSegment.totalReadLockCount) > 0) {
                            segmentHeader().downgradeWriteToReadLock(segmentHeaderAddress());
                        } else {
                            segmentHeader().writeUnlock(segmentHeaderAddress());
                        }
                    }
                }
                break;
        }
    }

    private void closeRootLocks() {
        assert (nextNode) == null;
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

    private void innerInitSegmentHashLookup(long address, long capacity, int entrySize, int keyBits, int valueBits) {
        this.address = address;
        this.capacityMask = capacity - 1L;
        this.hashLookupEntrySize = entrySize;
        this.capacityMask2 = (capacityMask) * entrySize;
        this.keyBits = keyBits;
        this.keyMask = CompiledReplicatedMapIterationContext.mask(keyBits);
        this.valueMask = CompiledReplicatedMapIterationContext.mask(valueBits);
        this.entryMask = CompiledReplicatedMapIterationContext.mask((keyBits + valueBits));
    }

    private void unlinkFromSegmentContextsChain() {
        CompiledReplicatedMapIterationContext prevContext = this.rootContextOnThisSegment;
        while (true) {
            assert (prevContext.nextNode) != null;
            if ((prevContext.nextNode) == this)
                break;

            prevContext = prevContext.nextNode;
        }
        assert (nextNode) == null;
        prevContext.nextNode = null;
    }

    public final int indexInContextChain;

    public int indexInContextChain() {
        return this.indexInContextChain;
    }

    public static int entrySize(int keyBits, int valueBits) {
        return ((int)(MemoryUnit.BYTES.alignAndConvert(((long)(keyBits + valueBits)), MemoryUnit.BITS)));
    }

    public static int keyBits(long entriesPerSegment, int valueBits) {
        int minKeyBits = 64 - (Long.numberOfLeadingZeros((entriesPerSegment - 1L)));
        minKeyBits += 3;
        int actualEntryBits = ((int)(MemoryUnit.BYTES.align(((long)(minKeyBits + valueBits)), MemoryUnit.BITS)));
        return actualEntryBits - valueBits;
    }

    public static int valueBits(long actualChunksPerSegment) {
        return 64 - (Long.numberOfLeadingZeros((actualChunksPerSegment - 1L)));
    }

    public final Bytes entryBytes;

    public Bytes entryBytes() {
        return this.entryBytes;
    }

    public static long capacityFor(long entriesPerSegment) {
        if (entriesPerSegment < 0L)
            throw new IllegalArgumentException("entriesPerSegment should be positive");

        long capacity = Maths.nextPower2(entriesPerSegment, 64L);
        if ((((double)(entriesPerSegment)) / ((double)(capacity))) > (2.0 / 3.0)) {
            capacity <<= 1L;
        }
        return capacity;
    }

    public static long mask(int bits) {
        return (1L << bits) - 1L;
    }

    public static final int MAX_SEGMENT_CHUNKS = 1 << 30;

    public static final int MAX_SEGMENT_ENTRIES = 1 << 29;

    public int MAX_SEGMENT_ENTRIES() {
        return this.MAX_SEGMENT_ENTRIES;
    }

    public int MAX_SEGMENT_CHUNKS() {
        return this.MAX_SEGMENT_CHUNKS;
    }

    public static final long UNSET_KEY = 0L;

    public static final long UNSET_ENTRY = 0L;

    public long UNSET_ENTRY() {
        return this.UNSET_ENTRY;
    }

    public long UNSET_KEY() {
        return this.UNSET_KEY;
    }

    public final ReadLock innerReadLock;

    public ReadLock innerReadLock() {
        return this.innerReadLock;
    }

    public final WriteLock innerWriteLock;

    public WriteLock innerWriteLock() {
        return this.innerWriteLock;
    }

    public final List<CompiledReplicatedMapIterationContext> contextChain;

    public final UpdateLock innerUpdateLock;

    public UpdateLock innerUpdateLock() {
        return this.innerUpdateLock;
    }

    public List<CompiledReplicatedMapIterationContext> contextChain() {
        return this.contextChain;
    }

    public final ThreadLocalCopies copies;

    public ThreadLocalCopies copies() {
        return this.copies;
    }

    private void countValueOffset() {
        CompiledReplicatedMapIterationContext.this.m().alignment.alignPositionAddr(entryBytes);
        valueOffset = entryBytes.position();
    }

    final DummyValueZeroData dummyValue;

    public DummyValueZeroData dummyValue() {
        return this.dummyValue;
    }

    final EntryKeyBytesData entryKey;

    public EntryKeyBytesData entryKey() {
        return this.entryKey;
    }

    public final EntryValueBytesData entryValue;

    public EntryValueBytesData entryValue() {
        return this.entryValue;
    }

    final WrappedValueInstanceData wrappedValueInstanceValue;

    public WrappedValueInstanceData wrappedValueInstanceValue() {
        return this.wrappedValueInstanceValue;
    }

    public final DeprecatedMapKeyContextOnIteration deprecatedMapKeyContextOnIteration;

    public DeprecatedMapKeyContextOnIteration deprecatedMapKeyContextOnIteration() {
        return this.deprecatedMapKeyContextOnIteration;
    }

    private final ReplicatedChronicleMap<K, KI, MKI, V, VI, MVI, R> m;

    public final VI valueInterop;

    public VI valueInterop() {
        return this.valueInterop;
    }

    final Full<Bytes, ?> entryBytesAccessor;

    public Full<Bytes, ?> entryBytesAccessor() {
        return this.entryBytesAccessor;
    }

    @SuppressWarnings(value = "unchecked")
    public final T entryBytesAccessHandle;

    public T entryBytesAccessHandle() {
        return this.entryBytesAccessHandle;
    }

    public final BytesReader<V> valueReader;

    public BytesReader<V> valueReader() {
        return this.valueReader;
    }

    public final KI keyInterop;

    public KI keyInterop() {
        return this.keyInterop;
    }

    @SuppressWarnings(value = "unchecked")
    public final Access<T> entryBytesAccess;

    public Access<T> entryBytesAccess() {
        return this.entryBytesAccess;
    }

    public final BytesReader<K> keyReader;

    public BytesReader<K> keyReader() {
        return this.keyReader;
    }

    public <T>T contextAtIndexInChain(int index) {
        return ((T)(contextChain.get(index)));
    }

    public void closeReplicatedChronicleMapHolderImplContextAtIndexInChainDependants() {
        CompiledReplicatedMapIterationContext.this.closeSegmentStagesTryFindInitLocksOfThisSegmentDependants();
    }

    public MVI valueMetaInterop(V value) {
        return CompiledReplicatedMapIterationContext.this.m().metaValueInteropProvider.get(CompiledReplicatedMapIterationContext.this.copies, CompiledReplicatedMapIterationContext.this.m().originalMetaValueInterop, valueInterop, value);
    }

    public void closeValueBytesInteropValueMetaInteropDependants() {
        CompiledReplicatedMapIterationContext.this.wrappedValueInstanceValue.closeBuffer();
    }

    @NotNull
    @Override
    public MapContext<K, V, ?> context() {
        return CompiledReplicatedMapIterationContext.this;
    }

    public long entryBytesAccessOffset(long offset) {
        return entryBytesAccessor.offset(entryBytes, offset);
    }

    @Override
    public ReplicatedChronicleMap<K, KI, MKI, V, VI, MVI, R> m() {
        return m;
    }

    @Override
    public ChronicleMap<K, V> map() {
        return m();
    }

    public long innerEntrySize(long sizeOfEverythingBeforeValue, long valueSize) {
        if (CompiledReplicatedMapIterationContext.this.m().constantlySizedEntry) {
            return CompiledReplicatedMapIterationContext.this.m().alignment.alignAddr((sizeOfEverythingBeforeValue + valueSize));
        } else if (CompiledReplicatedMapIterationContext.this.m().couldNotDetermineAlignmentBeforeAllocation) {
            return (sizeOfEverythingBeforeValue + (CompiledReplicatedMapIterationContext.this.m().worstAlignment)) + valueSize;
        } else {
            return (CompiledReplicatedMapIterationContext.this.m().alignment.alignAddr(sizeOfEverythingBeforeValue)) + valueSize;
        }
    }

    public MKI keyMetaInterop(K key) {
        return CompiledReplicatedMapIterationContext.this.h().metaKeyInteropProvider.get(CompiledReplicatedMapIterationContext.this.copies, CompiledReplicatedMapIterationContext.this.h().originalMetaKeyInterop, keyInterop, key);
    }

    private CompiledReplicatedMapIterationContext _Chaining_createChaining() {
        return new CompiledReplicatedMapIterationContext(this);
    }

    public CompiledReplicatedMapIterationContext createChaining() {
        return new CompiledReplicatedMapIterationContext(this);
    }

    public <T>T getContext() {
        for (CompiledReplicatedMapIterationContext context : contextChain) {
            if (!(context.usedInit())) {
                return ((T)(context));
            }
        }
        int maxNestedContexts = 1 << 16;
        if ((contextChain.size()) > maxNestedContexts) {
            throw new IllegalStateException((((((("More than " + maxNestedContexts) + " nested ChronicleHash contexts are not supported. Very probable that ") + "you simply forgot to close context somewhere (recommended to use ") + "try-with-resources statement). ") + "Otherwise this is a bug, please report with this ") + "stack trace on https://github.com/OpenHFT/Chronicle-Map/issues"));
        }
        return ((T)(createChaining()));
    }

    private long _MapEntryStages_sizeOfEverythingBeforeValue(long keySize, long valueSize) {
        return (((CompiledReplicatedMapIterationContext.this.m().metaDataBytes) + (CompiledReplicatedMapIterationContext.this.m().keySizeMarshaller.sizeEncodingSize(keySize))) + keySize) + (CompiledReplicatedMapIterationContext.this.m().valueSizeMarshaller.sizeEncodingSize(valueSize));
    }

    long sizeOfEverythingBeforeValue(long keySize, long valueSize) {
        return (_MapEntryStages_sizeOfEverythingBeforeValue(keySize, valueSize)) + (ReplicatedChronicleMap.ADDITIONAL_ENTRY_BYTES);
    }

    public final long entrySize(long keySize, long valueSize) {
        long sizeOfEverythingBeforeValue = sizeOfEverythingBeforeValue(keySize, valueSize);
        return innerEntrySize(sizeOfEverythingBeforeValue, valueSize);
    }

    public void incrementSegmentEntriesIfNeeded() {
    }

    public void checkAccessingFromOwnerThread() {
        if ((owner) != (Thread.currentThread())) {
            throw new ConcurrentModificationException("Context shouldn\'t be accessed from multiple threads");
        }
    }

    public void closeOwnerThreadHolderCheckAccessingFromOwnerThreadDependants() {
        CompiledReplicatedMapIterationContext.this.closeInterationCheckOnEachPublicOperationCheckOnEachPublicOperationDependants();
    }

    public void checkEntryNotRemovedOnThisIteration() {
        if (entryRemovedOnThisIterationInit())
            throw new IllegalStateException("Entry was already removed on this iteration");

    }

    public void closeReplicatedMapSegmentIterationCheckEntryNotRemovedOnThisIterationDependants() {
        CompiledReplicatedMapIterationContext.this.closeInterationCheckOnEachPublicOperationCheckOnEachPublicOperationDependants();
    }

    private void _CheckOnEachPublicOperation_checkOnEachPublicOperation() {
        CompiledReplicatedMapIterationContext.this.checkAccessingFromOwnerThread();
    }

    public void checkOnEachPublicOperation() {
        _CheckOnEachPublicOperation_checkOnEachPublicOperation();
        CompiledReplicatedMapIterationContext.this.checkEntryNotRemovedOnThisIteration();
    }

    public void closeInterationCheckOnEachPublicOperationCheckOnEachPublicOperationDependants() {
        CompiledReplicatedMapIterationContext.this.entryValue.closeEntryValueBytesDataSizeDependants();
        CompiledReplicatedMapIterationContext.this.entryKey.closeEntryKeyBytesDataSizeDependants();
    }

    @Override
    public R insert(@NotNull
                    MapAbsentEntry<K, V> absentEntry, Data<V, ?> value) {
        CompiledReplicatedMapIterationContext.this.checkOnEachPublicOperation();
        return CompiledReplicatedMapIterationContext.this.m().entryOperations.insert(absentEntry, value);
    }

    @NotNull
    @Override
    public InterProcessLock readLock() {
        CompiledReplicatedMapIterationContext.this.checkOnEachPublicOperation();
        return CompiledReplicatedMapIterationContext.this.innerReadLock;
    }

    @Override
    public Data<V, ?> defaultValue(@NotNull
                                                              MapAbsentEntry<K, V> absentEntry) {
        CompiledReplicatedMapIterationContext.this.checkOnEachPublicOperation();
        return CompiledReplicatedMapIterationContext.this.m().entryOperations.defaultValue(absentEntry);
    }

    @NotNull
    @Override
    public Data<K, ?> key() {
        CompiledReplicatedMapIterationContext.this.checkOnEachPublicOperation();
        return CompiledReplicatedMapIterationContext.this.entryKey;
    }

    @NotNull
    @Override
    public InterProcessLock writeLock() {
        CompiledReplicatedMapIterationContext.this.checkOnEachPublicOperation();
        return CompiledReplicatedMapIterationContext.this.innerWriteLock;
    }

    @Override
    public Data<V, ?> wrapValueAsValue(V value) {
        CompiledReplicatedMapIterationContext.this.checkOnEachPublicOperation();
        WrappedValueInstanceData wrapped = CompiledReplicatedMapIterationContext.this.wrappedValueInstanceValue;
        wrapped = wrapped.getUnusedWrappedValueGuarded();
        wrapped.initValue(value);
        return wrapped;
    }

    @Override
    public R remove(@NotNull
                    MapEntry<K, V> entry) {
        CompiledReplicatedMapIterationContext.this.checkOnEachPublicOperation();
        return CompiledReplicatedMapIterationContext.this.m().entryOperations.remove(entry);
    }

    @Override
    public R replaceValue(@NotNull
                          MapEntry<K, V> entry, Data<V, ?> newValue) {
        CompiledReplicatedMapIterationContext.this.checkOnEachPublicOperation();
        return CompiledReplicatedMapIterationContext.this.m().entryOperations.replaceValue(entry, newValue);
    }

    @NotNull
    @Override
    public InterProcessLock updateLock() {
        CompiledReplicatedMapIterationContext.this.checkOnEachPublicOperation();
        return CompiledReplicatedMapIterationContext.this.innerUpdateLock;
    }

    @NotNull
    @Override
    public Data<V, ?> value() {
        CompiledReplicatedMapIterationContext.this.checkOnEachPublicOperation();
        return CompiledReplicatedMapIterationContext.this.entryValue;
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
        if (!(this.allocatedChunksInit()))
            return ;

        this.allocatedChunks = 0;
    }

    public int segmentIndex = -1;

    public boolean theSegmentIndexInit() {
        return (this.segmentIndex) >= 0;
    }

    public void initTheSegmentIndex(int segmentIndex) {
        this.segmentIndex = segmentIndex;
        this.closeTheSegmentIndexDependants();
    }

    public int segmentIndex() {
        assert this.theSegmentIndexInit() : "TheSegmentIndex should be init";
        return this.segmentIndex;
    }

    public void closeTheSegmentIndex() {
        if (!(this.theSegmentIndexInit()))
            return ;

        this.closeTheSegmentIndexDependants();
        this.segmentIndex = -1;
    }

    public void closeTheSegmentIndexDependants() {
        CompiledReplicatedMapIterationContext.this.closeSegment();
        CompiledReplicatedMapIterationContext.this.closeSegmentHashLookup();
        CompiledReplicatedMapIterationContext.this.closeSegHeader();
    }

    long entrySpaceOffset = 0;

    MultiStoreBytes freeListBytes = new MultiStoreBytes();

    public SingleThreadedDirectBitSet freeList = new SingleThreadedDirectBitSet();

    boolean segmentInit() {
        return (entrySpaceOffset) > 0;
    }

    void initSegment() {
        VanillaChronicleHash<?, ?, ?, ?, ?> h = CompiledReplicatedMapIterationContext.this.h();
        long hashLookupOffset = h.segmentOffset(segmentIndex());
        long freeListOffset = hashLookupOffset + (h.segmentHashLookupOuterSize);
        freeListBytes.storePositionAndSize(h.ms, freeListOffset, h.segmentFreeListInnerSize);
        freeList.reuse(freeListBytes);
        entrySpaceOffset = (freeListOffset + (h.segmentFreeListOuterSize)) + (h.segmentEntrySpaceInnerOffset);
        this.closeSegmentDependants();
    }

    public long entrySpaceOffset() {
        if (!(this.segmentInit()))
            this.initSegment();

        return this.entrySpaceOffset;
    }

    public SingleThreadedDirectBitSet freeList() {
        if (!(this.segmentInit()))
            this.initSegment();

        return this.freeList;
    }

    void closeSegment() {
        if (!(this.segmentInit()))
            return ;

        this.closeSegmentDependants();
        entrySpaceOffset = 0;
    }

    public void closeSegmentDependants() {
        CompiledReplicatedMapIterationContext.this.closeEntryOffset();
    }

    public void moveChange(long oldPos, long newPos) {
        CompiledReplicatedMapIterationContext.this.m().moveChange(CompiledReplicatedMapIterationContext.this.segmentIndex(), oldPos, newPos);
    }

    int hashLookupEntrySize;

    int keyBits;

    long address = -1;

    long capacityMask;

    long capacityMask2;

    long keyMask;

    long valueMask;

    long entryMask;

    public boolean segmentHashLookupInit() {
        return (this.address) >= 0;
    }

    public void initSegmentHashLookup() {
        long hashLookupOffset = CompiledReplicatedMapIterationContext.this.h().segmentOffset(CompiledReplicatedMapIterationContext.this.segmentIndex());
        innerInitSegmentHashLookup(((CompiledReplicatedMapIterationContext.this.h().ms.address()) + hashLookupOffset), CompiledReplicatedMapIterationContext.this.h().segmentHashLookupCapacity, CompiledReplicatedMapIterationContext.this.h().segmentHashLookupEntrySize, CompiledReplicatedMapIterationContext.this.h().segmentHashLookupKeyBits, CompiledReplicatedMapIterationContext.this.h().segmentHashLookupValueBits);
    }

    public void initSegmentHashLookup(long address, long capacity, int entrySize, int keyBits, int valueBits) {
        innerInitSegmentHashLookup(address, capacity, entrySize, keyBits, valueBits);
    }

    public int hashLookupEntrySize() {
        if (!(this.segmentHashLookupInit()))
            this.initSegmentHashLookup();

        return this.hashLookupEntrySize;
    }

    public int keyBits() {
        if (!(this.segmentHashLookupInit()))
            this.initSegmentHashLookup();

        return this.keyBits;
    }

    public long address() {
        if (!(this.segmentHashLookupInit()))
            this.initSegmentHashLookup();

        return this.address;
    }

    public long capacityMask() {
        if (!(this.segmentHashLookupInit()))
            this.initSegmentHashLookup();

        return this.capacityMask;
    }

    public long capacityMask2() {
        if (!(this.segmentHashLookupInit()))
            this.initSegmentHashLookup();

        return this.capacityMask2;
    }

    public long entryMask() {
        if (!(this.segmentHashLookupInit()))
            this.initSegmentHashLookup();

        return this.entryMask;
    }

    public long keyMask() {
        if (!(this.segmentHashLookupInit()))
            this.initSegmentHashLookup();

        return this.keyMask;
    }

    public long valueMask() {
        if (!(this.segmentHashLookupInit()))
            this.initSegmentHashLookup();

        return this.valueMask;
    }

    public void closeSegmentHashLookup() {
        if (!(this.segmentHashLookupInit()))
            return ;

        this.address = -1;
    }

    public long step(long pos) {
        return (pos += hashLookupEntrySize()) <= (capacityMask2()) ? pos : 0L;
    }

    public void clearHashLookup() {
        NativeBytes.UNSAFE.setMemory(address(), ((capacityMask2()) + (hashLookupEntrySize())), ((byte)(0)));
    }

    long entry(long key, long value) {
        return key | (value << (keyBits()));
    }

    public void writeEntryVolatile(long pos, long prevEntry, long key, long value) {
        long entry = (prevEntry & (~(entryMask()))) | (entry(key, value));
        NativeBytes.UNSAFE.putLongVolatile(null, ((address()) + pos), entry);
    }

    public long key(long entry) {
        return entry & (keyMask());
    }

    public boolean empty(long entry) {
        return (entry & (entryMask())) == (UNSET_ENTRY);
    }

    public void checkValueForPut(long value) {
        assert (value & (~(valueMask()))) == 0L : "Value out of range, was " + value;
    }

    public long stepBack(long pos) {
        return (pos -= hashLookupEntrySize()) >= 0 ? pos : capacityMask2();
    }

    public long readEntry(long pos) {
        return NativeBytes.UNSAFE.getLong(((address()) + pos));
    }

    public void putValueVolatile(long pos, long value) {
        checkValueForPut(value);
        long currentEntry = readEntry(pos);
        writeEntryVolatile(pos, currentEntry, key(currentEntry), value);
    }

    public long value(long entry) {
        return (entry >>> (keyBits())) & (valueMask());
    }

    public long maskUnsetKey(long key) {
        return (key &= keyMask()) != (UNSET_KEY) ? key : keyMask();
    }

    void writeEntry(long pos, long prevEntry, long anotherEntry) {
        long entry = (prevEntry & (~(entryMask()))) | (anotherEntry & (entryMask()));
        NativeBytes.UNSAFE.putLong(((address()) + pos), entry);
    }

    void clearEntry(long pos, long prevEntry) {
        long entry = prevEntry & (~(entryMask()));
        NativeBytes.UNSAFE.putLong(((address()) + pos), entry);
    }

    void forEach(EntryConsumer action) {
        for (long pos = 0L ; pos <= (capacityMask2()) ; pos += hashLookupEntrySize()) {
            long entry = readEntry(pos);
            if (!(empty(entry)))
                action.accept(key(entry), value(entry));

        }
    }

    String hashLookupToString() {
        final StringBuilder sb = new StringBuilder("{");
        forEach((long key,long value) -> sb.append(key).append('=').append(value).append(','));
        sb.append('}');
        return sb.toString();
    }

    long indexToPos(long index) {
        return index * (hashLookupEntrySize());
    }

    public long hlPos(long key) {
        return indexToPos((key & (capacityMask())));
    }

    public long remove(long posToRemove) {
        long entryToRemove = readEntry(posToRemove);
        long posToShift = posToRemove;
        while (true) {
            posToShift = step(posToShift);
            long entryToShift = readEntry(posToShift);
            if (empty(entryToShift))
                break;

            long insertPos = hlPos(key(entryToShift));
            boolean cond1 = insertPos <= posToRemove;
            boolean cond2 = posToRemove <= posToShift;
            if ((cond1 && cond2) || ((posToShift < insertPos) && (cond1 || cond2))) {
                writeEntry(posToRemove, entryToRemove, entryToShift);
                posToRemove = posToShift;
                entryToRemove = entryToShift;
            }
        }
        clearEntry(posToRemove, entryToRemove);
        return posToRemove;
    }

    public void writeEntry(long pos, long prevEntry, long key, long value) {
        long entry = (prevEntry & (~(entryMask()))) | (entry(key, value));
        NativeBytes.UNSAFE.putLong(((address()) + pos), entry);
    }

    long segmentHeaderAddress;

    SegmentHeader segmentHeader = null;

    public boolean segHeaderInit() {
        return (this.segmentHeader) != null;
    }

    private void initSegHeader() {
        segmentHeaderAddress = (CompiledReplicatedMapIterationContext.this.h().ms.address()) + (CompiledReplicatedMapIterationContext.this.h().segmentHeaderOffset(segmentIndex()));
        segmentHeader = BigSegmentHeader.INSTANCE;
        this.closeSegHeaderDependants();
    }

    public long segmentHeaderAddress() {
        if (!(this.segHeaderInit()))
            this.initSegHeader();

        return this.segmentHeaderAddress;
    }

    public SegmentHeader segmentHeader() {
        if (!(this.segHeaderInit()))
            this.initSegHeader();

        return this.segmentHeader;
    }

    public void closeSegHeader() {
        if (!(this.segHeaderInit()))
            return ;

        this.closeSegHeaderDependants();
        this.segmentHeader = null;
    }

    public void closeSegHeaderDependants() {
        CompiledReplicatedMapIterationContext.this.closeSegmentStagesTryFindInitLocksOfThisSegmentDependants();
        CompiledReplicatedMapIterationContext.this.closeLocks();
    }

    public long entries() {
        return segmentHeader().size(segmentHeaderAddress());
    }

    public void nextPosToSearchFrom(long nextPosToSearchFrom) {
        segmentHeader().nextPosToSearchFrom(segmentHeaderAddress(), nextPosToSearchFrom);
    }

    public void updateNextPosToSearchFrom(long allocated, int chunks) {
        long nextPosToSearchFrom = allocated + chunks;
        if (nextPosToSearchFrom >= (CompiledReplicatedMapIterationContext.this.h().actualChunksPerSegment))
            nextPosToSearchFrom = 0L;

        nextPosToSearchFrom(nextPosToSearchFrom);
    }

    public long deleted() {
        return segmentHeader().deleted(segmentHeaderAddress());
    }

    public long size() {
        return (entries()) - (deleted());
    }

    boolean tryFindInitLocksOfThisSegment(Object thisContext, int index) {
        CompiledReplicatedMapIterationContext c = CompiledReplicatedMapIterationContext.this.contextAtIndexInChain(index);
        if ((((c.segmentHeader()) != null) && ((c.segmentHeaderAddress()) == (segmentHeaderAddress()))) && ((c.rootContextOnThisSegment()) != null)) {
            throw new IllegalStateException("Nested context not implemented yet");
        } else {
            return false;
        }
    }

    public void closeSegmentStagesTryFindInitLocksOfThisSegmentDependants() {
        CompiledReplicatedMapIterationContext.this.closeLocks();
    }

    public void entries(long size) {
        segmentHeader().size(segmentHeaderAddress(), size);
    }

    long nextPosToSearchFrom() {
        return segmentHeader().nextPosToSearchFrom(segmentHeaderAddress());
    }

    public void free(long fromPos, int chunks) {
        freeList().clear(fromPos, (fromPos + chunks));
        if (fromPos < (nextPosToSearchFrom()))
            nextPosToSearchFrom(fromPos);

    }

    public long alloc(int chunks) {
        VanillaChronicleHash<?, ?, ?, ?, ?> h = CompiledReplicatedMapIterationContext.this.h();
        if (chunks > (h.maxChunksPerEntry))
            throw new IllegalArgumentException((((("Entry is too large: requires " + chunks) + " entry size chucks, ") + (h.maxChunksPerEntry)) + " is maximum."));

        long ret = freeList().setNextNContinuousClearBits(nextPosToSearchFrom(), chunks);
        if ((ret == (DirectBitSet.NOT_FOUND)) || ((ret + chunks) > (h.actualChunksPerSegment))) {
            if (((ret != (DirectBitSet.NOT_FOUND)) && ((ret + chunks) > (h.actualChunksPerSegment))) && (ret < (h.actualChunksPerSegment)))
                freeList().clear(ret, h.actualChunksPerSegment);

            ret = freeList().setNextNContinuousClearBits(0L, chunks);
            if ((ret == (DirectBitSet.NOT_FOUND)) || ((ret + chunks) > (h.actualChunksPerSegment))) {
                if (((ret != (DirectBitSet.NOT_FOUND)) && ((ret + chunks) > (h.actualChunksPerSegment))) && (ret < (h.actualChunksPerSegment)))
                    freeList().clear(ret, h.actualChunksPerSegment);

                if (chunks == 1) {
                    throw new IllegalStateException("Segment is full, no free entries found");
                } else {
                    throw new IllegalStateException((("Segment is full or has no ranges of " + chunks) + " continuous free chunks"));
                }
            }
            updateNextPosToSearchFrom(ret, chunks);
        } else {
            if ((chunks == 1) || (freeList().isSet(nextPosToSearchFrom()))) {
                updateNextPosToSearchFrom(ret, chunks);
            }
        }
        return ret;
    }

    int totalReadLockCount;

    int totalUpdateLockCount;

    int totalWriteLockCount;

    public int latestSameThreadSegmentModCount;

    public int contextModCount;

    CompiledReplicatedMapIterationContext nextNode;

    public boolean concurrentSameThreadContexts;

    LocalLockState localLockState;

    public CompiledReplicatedMapIterationContext rootContextOnThisSegment = null;

    public boolean locksInit() {
        return (this.rootContextOnThisSegment) != null;
    }

    void initLocks() {
        localLockState = LocalLockState.UNLOCKED;
        int indexOfThisContext = CompiledReplicatedMapIterationContext.this.indexInContextChain;
        for (int i = indexOfThisContext - 1 ; i >= 0 ; i--) {
            if (tryFindInitLocksOfThisSegment(this, i))
                return ;

        }
        for (int i = indexOfThisContext + 1, size = CompiledReplicatedMapIterationContext.this.contextChain.size() ; i < size ; i++) {
            if (tryFindInitLocksOfThisSegment(this, i))
                return ;

        }
        rootContextOnThisSegment = this;
        concurrentSameThreadContexts = false;
        latestSameThreadSegmentModCount = 0;
        contextModCount = 0;
        nextNode = null;
        totalReadLockCount = 0;
        totalUpdateLockCount = 0;
        totalWriteLockCount = 0;
    }

    public LocalLockState localLockState() {
        if (!(this.locksInit()))
            this.initLocks();

        return this.localLockState;
    }

    public CompiledReplicatedMapIterationContext rootContextOnThisSegment() {
        if (!(this.locksInit()))
            this.initLocks();

        return this.rootContextOnThisSegment;
    }

    void closeLocks() {
        if (!(this.locksInit()))
            return ;

        if ((rootContextOnThisSegment) == this) {
            closeRootLocks();
        } else {
            closeNestedLocks();
        }
        localLockState = null;
        rootContextOnThisSegment = null;
    }

    public void clearSegment() {
        CompiledReplicatedMapIterationContext.this.innerWriteLock.lock();
        CompiledReplicatedMapIterationContext.this.clearHashLookup();
        freeList().clear();
        nextPosToSearchFrom(0L);
        entries(0L);
    }

    public void clear() {
        clearSegment();
    }

    public void deleted(long deleted) {
        segmentHeader().deleted(segmentHeaderAddress(), deleted);
    }

    public long keySize = -1;

    public boolean keySizeInit() {
        return (this.keySize) >= 0;
    }

    public void initKeySize(long keySize) {
        this.keySize = keySize;
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
        CompiledReplicatedMapIterationContext.this.entryKey.closeEntryKeyBytesDataSizeDependants();
        CompiledReplicatedMapIterationContext.this.closeReplicatedMapEntryStagesKeyEndDependants();
    }

    boolean used;

    public boolean usedInit() {
        return used;
    }

    public void initUsed(boolean used) {
        this.used = used;
    }

    void closeUsed() {
        if (!(this.usedInit()))
            return ;

        used = false;
    }

    public long hashLookupPos = -1;

    public boolean hashLookupPosInit() {
        return (this.hashLookupPos) >= 0;
    }

    public void initHashLookupPos(long hashLookupPos) {
        this.hashLookupPos = hashLookupPos;
    }

    public long hashLookupPos() {
        assert this.hashLookupPosInit() : "HashLookupPos should be init";
        return this.hashLookupPos;
    }

    public void closeHashLookupPos() {
        if (!(this.hashLookupPosInit()))
            return ;

        this.hashLookupPos = -1;
    }

    public boolean entryRemovedOnThisIteration = false;

    boolean entryRemovedOnThisIterationInit() {
        return (this.entryRemovedOnThisIteration) != false;
    }

    protected void initEntryRemovedOnThisIteration(boolean entryRemovedOnThisIteration) {
        this.entryRemovedOnThisIteration = entryRemovedOnThisIteration;
    }

    public void closeEntryRemovedOnThisIteration() {
        if (!(this.entryRemovedOnThisIterationInit()))
            return ;

        this.entryRemovedOnThisIteration = false;
    }

    public long keyOffset = -1;

    public boolean keyOffsetInit() {
        return (this.keyOffset) >= 0;
    }

    public void initKeyOffset(long keyOffset) {
        this.keyOffset = keyOffset;
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
        CompiledReplicatedMapIterationContext.this.entryKey.closeEntryKeyBytesDataInnerGetUsingDependants();
        CompiledReplicatedMapIterationContext.this.closeReplicatedMapEntryStagesKeyEndDependants();
    }

    public long keyEnd() {
        return (keyOffset()) + (keySize());
    }

    public void closeReplicatedMapEntryStagesKeyEndDependants() {
        CompiledReplicatedMapIterationContext.this.closeReplicationState();
        CompiledReplicatedMapIterationContext.this.closeReplicatedMapEntryStagesCountValueSizeOffsetDependants();
        CompiledReplicatedMapIterationContext.this.closeReplicatedMapEntryStagesEntryEndDependants();
    }

    long replicationBytesOffset = -1;

    public boolean replicationStateInit() {
        return (this.replicationBytesOffset) >= 0;
    }

    void initReplicationState() {
        replicationBytesOffset = keyEnd();
    }

    public long replicationBytesOffset() {
        if (!(this.replicationStateInit()))
            this.initReplicationState();

        return this.replicationBytesOffset;
    }

    public void closeReplicationState() {
        if (!(this.replicationStateInit()))
            return ;

        this.replicationBytesOffset = -1;
    }

    private long identifierOffset() {
        return (replicationBytesOffset()) + 8L;
    }

    byte identifier() {
        return entryBytesAccess.readByte(entryBytesAccessHandle, identifierOffset());
    }

    @Override
    public byte originIdentifier() {
        CompiledReplicatedMapIterationContext.this.checkOnEachPublicOperation();
        return identifier();
    }

    private long timestampOffset() {
        return replicationBytesOffset();
    }

    public long timestamp() {
        return entryBytesAccess.readLong(entryBytesAccessHandle, timestampOffset());
    }

    @Override
    public long originTimestamp() {
        CompiledReplicatedMapIterationContext.this.checkOnEachPublicOperation();
        return timestamp();
    }

    void updateReplicationState(long timestamp, byte identifier) {
        entryBytes.position(replicationBytesOffset());
        entryBytes.writeLong(timestamp);
        entryBytes.writeByte(identifier);
    }

    public void updatedReplicationStateOnAbsentEntry() {
        if (!(CompiledReplicatedMapIterationContext.this.replicationUpdateInit())) {
            CompiledReplicatedMapIterationContext.this.innerWriteLock.lock();
            updateReplicationState(CompiledReplicatedMapIterationContext.this.m().timeProvider.currentTime(), CompiledReplicatedMapIterationContext.this.m().identifier());
        }
    }

    public void updatedReplicationStateOnPresentEntry() {
        if (!(CompiledReplicatedMapIterationContext.this.replicationUpdateInit())) {
            CompiledReplicatedMapIterationContext.this.innerWriteLock.lock();
            long timestamp;
            if ((identifier()) != (CompiledReplicatedMapIterationContext.this.m().identifier())) {
                timestamp = Math.max(((timestamp()) + 1), CompiledReplicatedMapIterationContext.this.m().timeProvider.currentTime());
            } else {
                timestamp = CompiledReplicatedMapIterationContext.this.m().timeProvider.currentTime();
            }
            updateReplicationState(timestamp, CompiledReplicatedMapIterationContext.this.m().identifier());
        }
    }

    @Override
    public void updateOrigin(byte newIdentifier, long newTimestamp) {
        CompiledReplicatedMapIterationContext.this.checkOnEachPublicOperation();
        CompiledReplicatedMapIterationContext.this.innerWriteLock.lock();
        updateReplicationState(newTimestamp, newIdentifier);
    }

    private long entryDeletedOffset() {
        return (replicationBytesOffset()) + 9L;
    }

    public boolean entryDeleted() {
        return entryBytesAccess.readBoolean(entryBytesAccessHandle, entryDeletedOffset());
    }

    private boolean _MapSegmentIteration_entryIsPresent() {
        return true;
    }

    public boolean entryIsPresent() {
        return !(CompiledReplicatedMapIterationContext.this.entryDeleted());
    }

    public void writeEntryDeleted() {
        entryBytesAccess.writeBoolean(entryBytesAccessHandle, entryDeletedOffset(), true);
    }

    public void writeEntryPresent() {
        entryBytesAccess.writeBoolean(entryBytesAccessHandle, entryDeletedOffset(), false);
    }

    private long _MapEntryStages_countValueSizeOffset() {
        return keyEnd();
    }

    long countValueSizeOffset() {
        return (_MapEntryStages_countValueSizeOffset()) + (ReplicatedChronicleMap.ADDITIONAL_ENTRY_BYTES);
    }

    public void closeReplicatedMapEntryStagesCountValueSizeOffsetDependants() {
        CompiledReplicatedMapIterationContext.this.closeValueSizeOffset();
    }

    public long valueSizeOffset = -1;

    public boolean valueSizeOffsetInit() {
        return (this.valueSizeOffset) >= 0;
    }

    void initValueSizeOffset() {
        valueSizeOffset = countValueSizeOffset();
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
        CompiledReplicatedMapIterationContext.this.closeValSize();
    }

    public long valueSize = -1;

    public long valueOffset;

    public boolean valSizeInit() {
        return (this.valueSize) >= 0;
    }

    void initValSize() {
        entryBytes.position(valueSizeOffset());
        valueSize = CompiledReplicatedMapIterationContext.this.m().readValueSize(entryBytes);
        countValueOffset();
        this.closeValSizeDependants();
    }

    void initValSize(long valueSize) {
        this.valueSize = valueSize;
        entryBytes.position(valueSizeOffset());
        CompiledReplicatedMapIterationContext.this.m().valueSizeMarshaller.writeSize(entryBytes, valueSize);
        countValueOffset();
        this.closeValSizeDependants();
    }

    void initValSizeEqualToOld(long oldValueSizeOffset, long oldValueSize, long oldValueOffset) {
        valueSize = oldValueSize;
        valueOffset = (valueSizeOffset()) + (oldValueOffset - oldValueSizeOffset);
        this.closeValSizeDependants();
    }

    public long valueOffset() {
        if (!(this.valSizeInit()))
            this.initValSize();

        return this.valueOffset;
    }

    public long valueSize() {
        if (!(this.valSizeInit()))
            this.initValSize();

        return this.valueSize;
    }

    public void closeValSize() {
        if (!(this.valSizeInit()))
            return ;

        this.closeValSizeDependants();
        this.valueSize = -1;
    }

    public void closeValSizeDependants() {
        CompiledReplicatedMapIterationContext.this.closeReplicatedMapEntryStagesEntryEndDependants();
        CompiledReplicatedMapIterationContext.this.entryValue.closeEntryValueBytesDataSizeDependants();
        CompiledReplicatedMapIterationContext.this.entryValue.closeEntryValueBytesDataInnerGetUsingDependants();
    }

    private long _HashEntryStages_entryEnd() {
        return keyEnd();
    }

    protected long entryEnd() {
        return (valueOffset()) + (valueSize());
    }

    public void closeReplicatedMapEntryStagesEntryEndDependants() {
        CompiledReplicatedMapIterationContext.this.closeReplicatedMapEntryStagesEntrySizeDependants();
    }

    public void writeValue(Data<?, ?> value) {
        value.writeTo(entryBytesAccess, entryBytesAccessHandle, entryBytesAccessOffset(valueOffset()));
    }

    public void initValueWithoutSize(Data<?, ?> value, long oldValueSizeOffset, long oldValueSize, long oldValueOffset) {
        assert oldValueSize == (value.size());
        initValSizeEqualToOld(oldValueSizeOffset, oldValueSize, oldValueOffset);
        writeValue(value);
    }

    public void initValue(Data<?, ?> value) {
        entryBytes.position(valueSizeOffset());
        initValSize(value.size());
        writeValue(value);
    }

    public long innerRemoteTimestamp;

    public byte innerRemoteIdentifier = ((byte)(0));

    public boolean replicationUpdateInit() {
        return (this.innerRemoteIdentifier) != ((byte)(0));
    }

    public void initReplicationUpdate(long timestamp, byte identifier) {
        innerRemoteTimestamp = timestamp;
        if (identifier == 0)
            throw new IllegalStateException("identifier can\'t be 0");

        innerRemoteIdentifier = identifier;
    }

    public byte innerRemoteIdentifier() {
        assert this.replicationUpdateInit() : "ReplicationUpdate should be init";
        return this.innerRemoteIdentifier;
    }

    public long innerRemoteTimestamp() {
        assert this.replicationUpdateInit() : "ReplicationUpdate should be init";
        return this.innerRemoteTimestamp;
    }

    public void closeReplicationUpdate() {
        if (!(this.replicationUpdateInit()))
            return ;

        this.innerRemoteIdentifier = ((byte)(0));
    }

    @Override
    public byte remoteIdentifier() {
        CompiledReplicatedMapIterationContext.this.checkOnEachPublicOperation();
        return innerRemoteIdentifier();
    }

    private boolean testTimeStampInSensibleRange() {
        if ((CompiledReplicatedMapIterationContext.this.m().timeProvider) == (TimeProvider.SYSTEM)) {
            long currentTime = TimeProvider.SYSTEM.currentTime();
            assert (Math.abs((currentTime - (timestamp())))) <= 100000000 : "unrealistic timestamp: " + (timestamp());
            assert (Math.abs((currentTime - (CompiledReplicatedMapIterationContext.this.innerRemoteTimestamp())))) <= 100000000 : "unrealistic innerRemoteTimestamp: " + (CompiledReplicatedMapIterationContext.this.innerRemoteTimestamp());
        }
        return true;
    }

    @Override
    public long remoteTimestamp() {
        CompiledReplicatedMapIterationContext.this.checkOnEachPublicOperation();
        return innerRemoteTimestamp();
    }

    public long pos = -1;

    public boolean posInit() {
        return (this.pos) >= 0;
    }

    public void initPos(long pos) {
        this.pos = pos;
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
        CompiledReplicatedMapIterationContext.this.closeEntryOffset();
    }

    public void dropChange() {
        CompiledReplicatedMapIterationContext.this.m().dropChange(CompiledReplicatedMapIterationContext.this.segmentIndex(), CompiledReplicatedMapIterationContext.this.pos());
    }

    @Override
    public void dropChanged() {
        CompiledReplicatedMapIterationContext.this.checkOnEachPublicOperation();
        CompiledReplicatedMapIterationContext.this.innerUpdateLock.lock();
        CompiledReplicatedMapIterationContext.this.dropChange();
    }

    public long keySizeOffset = -1;

    public boolean entryOffsetInit() {
        return (this.keySizeOffset) >= 0;
    }

    public void initEntryOffset() {
        keySizeOffset = (CompiledReplicatedMapIterationContext.this.entrySpaceOffset()) + ((pos()) * (CompiledReplicatedMapIterationContext.this.h().chunkSize));
        entryBytes.limit(entryBytes.capacity());
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
        CompiledReplicatedMapIterationContext.this.closeReplicatedMapEntryStagesEntrySizeDependants();
    }

    long entrySize() {
        return (entryEnd()) - (keySizeOffset());
    }

    public void closeReplicatedMapEntryStagesEntrySizeDependants() {
        CompiledReplicatedMapIterationContext.this.closeTheEntrySizeInChunks();
    }

    public int entrySizeInChunks = 0;

    public boolean theEntrySizeInChunksInit() {
        return (this.entrySizeInChunks) != 0;
    }

    void initTheEntrySizeInChunks() {
        entrySizeInChunks = CompiledReplicatedMapIterationContext.this.h().inChunks(entrySize());
    }

    public void initTheEntrySizeInChunks(int actuallyUsedChunks) {
        entrySizeInChunks = actuallyUsedChunks;
    }

    public int entrySizeInChunks() {
        if (!(this.theEntrySizeInChunksInit()))
            this.initTheEntrySizeInChunks();

        return this.entrySizeInChunks;
    }

    public void closeTheEntrySizeInChunks() {
        if (!(this.theEntrySizeInChunksInit()))
            return ;

        this.entrySizeInChunks = 0;
    }

    public void innerRemoveEntryExceptHashLookupUpdate() {
        CompiledReplicatedMapIterationContext.this.free(pos(), entrySizeInChunks());
        CompiledReplicatedMapIterationContext.this.entries(((CompiledReplicatedMapIterationContext.this.entries()) - 1L));
        CompiledReplicatedMapIterationContext.this.incrementModCountGuarded();
    }

    public void copyExistingEntry(long newPos, long bytesToCopy) {
        long oldKeySizeOffset = keySizeOffset();
        long oldKeyOffset = keyOffset();
        initPos(newPos);
        initKeyOffset(((keySizeOffset()) + (oldKeyOffset - oldKeySizeOffset)));
        Access.copy(entryBytesAccess, entryBytesAccessHandle, entryBytesAccessOffset(oldKeySizeOffset), entryBytesAccess, entryBytesAccessHandle, entryBytesAccessOffset(keySizeOffset()), bytesToCopy);
    }

    public void initEntryAndKeyCopying(long entrySize, long bytesToCopy) {
        initAllocatedChunks(CompiledReplicatedMapIterationContext.this.h().inChunks(entrySize));
        CompiledReplicatedMapIterationContext.this.copyExistingEntry(CompiledReplicatedMapIterationContext.this.alloc(allocatedChunks()), bytesToCopy);
        incrementSegmentEntriesIfNeeded();
    }

    public long newSizeOfEverythingBeforeValue(Data<V, ?> newValue) {
        return ((valueSizeOffset()) + (CompiledReplicatedMapIterationContext.this.m().valueSizeMarshaller.sizeEncodingSize(newValue.size()))) - (keySizeOffset());
    }

    public void readExistingEntry(long pos) {
        initPos(pos);
        entryBytes.position(keySizeOffset());
        initKeySize(CompiledReplicatedMapIterationContext.this.h().keySizeMarshaller.readSize(entryBytes));
        initKeyOffset(entryBytes.position());
    }

    public boolean forEachRemoving(Predicate<? super MapEntry<K, V>> action) {
        CompiledReplicatedMapIterationContext.this.innerUpdateLock.lock();
        try {
            long size = CompiledReplicatedMapIterationContext.this.size();
            if (size == 0)
                return true;

            boolean interrupted = false;
            long startPos = 0L;
            while (!(CompiledReplicatedMapIterationContext.this.empty(CompiledReplicatedMapIterationContext.this.readEntry(startPos)))) {
                startPos = CompiledReplicatedMapIterationContext.this.step(startPos);
            }
            CompiledReplicatedMapIterationContext.this.initHashLookupPos(startPos);
            do {
                CompiledReplicatedMapIterationContext.this.setHashLookupPosGuarded(CompiledReplicatedMapIterationContext.this.step(CompiledReplicatedMapIterationContext.this.hashLookupPos()));
                long entry = CompiledReplicatedMapIterationContext.this.readEntry(CompiledReplicatedMapIterationContext.this.hashLookupPos());
                if (!(CompiledReplicatedMapIterationContext.this.empty(entry))) {
                    CompiledReplicatedMapIterationContext.this.readExistingEntry(CompiledReplicatedMapIterationContext.this.value(entry));
                    if (entryIsPresent()) {
                        initEntryRemovedOnThisIteration(false);
                        if (!(action.test(((MapEntry<K, V>)(this))))) {
                            interrupted = true;
                            break;
                        } else {
                            if ((--size) == 0)
                                break;

                        }
                    }
                }
            } while ((CompiledReplicatedMapIterationContext.this.hashLookupPos()) != startPos );
            return !interrupted;
        } finally {
            CompiledReplicatedMapIterationContext.this.innerReadLock.unlock();
            initEntryRemovedOnThisIteration(false);
        }
    }

    @Override
    public void forEachReplicableEntry(Consumer<? super ReplicableEntry> action) {
        CompiledReplicatedMapIterationContext.this.innerUpdateLock.lock();
        try {
            long entries = CompiledReplicatedMapIterationContext.this.entries();
            if (entries == 0)
                return ;

            long startPos = 0L;
            while (!(CompiledReplicatedMapIterationContext.this.empty(CompiledReplicatedMapIterationContext.this.readEntry(startPos)))) {
                startPos = CompiledReplicatedMapIterationContext.this.step(startPos);
            }
            CompiledReplicatedMapIterationContext.this.initHashLookupPos(startPos);
            do {
                CompiledReplicatedMapIterationContext.this.setHashLookupPosGuarded(CompiledReplicatedMapIterationContext.this.step(CompiledReplicatedMapIterationContext.this.hashLookupPos()));
                long entry = CompiledReplicatedMapIterationContext.this.readEntry(CompiledReplicatedMapIterationContext.this.hashLookupPos());
                if (!(CompiledReplicatedMapIterationContext.this.empty(entry))) {
                    CompiledReplicatedMapIterationContext.this.readExistingEntry(CompiledReplicatedMapIterationContext.this.value(entry));
                    action.accept(this);
                    if ((--entries) == 0)
                        break;

                }
            } while ((CompiledReplicatedMapIterationContext.this.hashLookupPos()) != startPos );
        } finally {
            CompiledReplicatedMapIterationContext.this.innerReadLock.unlock();
        }
    }

    public void writeNewEntry(long pos, Data<?, ?> key) {
        initPos(pos);
        initKeySize(key.size());
        entryBytes.position(keySizeOffset());
        CompiledReplicatedMapIterationContext.this.h().keySizeMarshaller.writeSize(entryBytes, keySize());
        initKeyOffset(entryBytes.position());
        key.writeTo(entryBytesAccessor, entryBytes, keyOffset());
    }

    public final void freeExtraAllocatedChunks() {
        if (((!(CompiledReplicatedMapIterationContext.this.m().constantlySizedEntry)) && (CompiledReplicatedMapIterationContext.this.m().couldNotDetermineAlignmentBeforeAllocation)) && ((entrySizeInChunks()) < (CompiledReplicatedMapIterationContext.this.allocatedChunks()))) {
            CompiledReplicatedMapIterationContext.this.free(((pos()) + (entrySizeInChunks())), ((CompiledReplicatedMapIterationContext.this.allocatedChunks()) - (entrySizeInChunks())));
        } else {
            initTheEntrySizeInChunks(CompiledReplicatedMapIterationContext.this.allocatedChunks());
        }
    }

    private void _MapEntryStages_putValueDeletedEntry(Data<V, ?> newValue) {
        assert CompiledReplicatedMapIterationContext.this.innerUpdateLock.isHeldByCurrentThread();
        int newSizeInChunks;
        long entryStartOffset = keySizeOffset();
        long newSizeOfEverythingBeforeValue = -1;
        boolean newValueSizeIsDifferent = (newValue.size()) != (valueSize());
        if (newValueSizeIsDifferent) {
            newSizeOfEverythingBeforeValue = newSizeOfEverythingBeforeValue(newValue);
            long newValueOffset = CompiledReplicatedMapIterationContext.this.m().alignment.alignAddr((entryStartOffset + newSizeOfEverythingBeforeValue));
            long newEntrySize = (newValueOffset + (newValue.size())) - entryStartOffset;
            newSizeInChunks = CompiledReplicatedMapIterationContext.this.m().inChunks(newEntrySize);
        } else {
            newSizeInChunks = entrySizeInChunks();
        }
        if ((((pos()) + newSizeInChunks) < (CompiledReplicatedMapIterationContext.this.freeList().size())) && (CompiledReplicatedMapIterationContext.this.freeList().allClear(pos(), ((pos()) + newSizeInChunks)))) {
            CompiledReplicatedMapIterationContext.this.freeList().set(pos(), ((pos()) + newSizeInChunks));
            CompiledReplicatedMapIterationContext.this.innerWriteLock.lock();
            CompiledReplicatedMapIterationContext.this.incrementSegmentEntriesIfNeeded();
            if (newValueSizeIsDifferent) {
                initValue(newValue);
            } else {
                writeValue(newValue);
            }
        } else {
            if (newValueSizeIsDifferent) {
                assert newSizeOfEverythingBeforeValue >= 0;
            } else {
                newSizeOfEverythingBeforeValue = newSizeOfEverythingBeforeValue(newValue);
            }
            long entrySize = innerEntrySize(newSizeOfEverythingBeforeValue, newValue.size());
            if (newValueSizeIsDifferent) {
                CompiledReplicatedMapIterationContext.this.initEntryAndKeyCopying(entrySize, ((valueSizeOffset()) - entryStartOffset));
                initValue(newValue);
            } else {
                long oldValueSizeOffset = valueSizeOffset();
                long oldValueSize = valueSize();
                long oldValueOffset = valueOffset();
                CompiledReplicatedMapIterationContext.this.initEntryAndKeyCopying(entrySize, ((valueOffset()) - entryStartOffset));
                initValueWithoutSize(newValue, oldValueSizeOffset, oldValueSize, oldValueOffset);
            }
            freeExtraAllocatedChunks();
        }
        CompiledReplicatedMapIterationContext.this.putValueVolatile(CompiledReplicatedMapIterationContext.this.hashLookupPos(), pos());
    }

    public void putValueDeletedEntry(Data<V, ?> newValue) {
        throw new AssertionError("Replicated Map doesn\'t remove entries truly, yet");
    }

    public void writeValueAndPutPos(Data<V, ?> value) {
        initValue(value);
        freeExtraAllocatedChunks();
        CompiledReplicatedMapIterationContext.this.putValueVolatile(CompiledReplicatedMapIterationContext.this.hashLookupPos(), pos());
    }

    private void _MapEntryStages_relocation(Data<V, ?> newValue, long newSizeOfEverythingBeforeValue) {
        CompiledReplicatedMapIterationContext.this.free(pos(), entrySizeInChunks());
        long entrySize = innerEntrySize(newSizeOfEverythingBeforeValue, newValue.size());
        CompiledReplicatedMapIterationContext.this.initEntryAndKeyCopying(entrySize, ((valueSizeOffset()) - (keySizeOffset())));
        writeValueAndPutPos(newValue);
    }

    protected void relocation(Data<V, ?> newValue, long newSizeOfEverythingBeforeValue) {
        long oldPos = pos();
        _MapEntryStages_relocation(newValue, newSizeOfEverythingBeforeValue);
        CompiledReplicatedMapIterationContext.this.moveChange(oldPos, pos());
    }

    public void innerDefaultReplaceValue(Data<V, ?> newValue) {
        assert CompiledReplicatedMapIterationContext.this.innerUpdateLock.isHeldByCurrentThread();
        boolean newValueSizeIsDifferent = (newValue.size()) != (this.valueSize());
        if (newValueSizeIsDifferent) {
            long newSizeOfEverythingBeforeValue = newSizeOfEverythingBeforeValue(newValue);
            long entryStartOffset = keySizeOffset();
            VanillaChronicleMap<?, ?, ?, ?, ?, ?, ?> m = CompiledReplicatedMapIterationContext.this.m();
            long newValueOffset = m.alignment.alignAddr((entryStartOffset + newSizeOfEverythingBeforeValue));
            long newEntrySize = (newValueOffset + (newValue.size())) - entryStartOffset;
            int newSizeInChunks = m.inChunks(newEntrySize);
            newValueDoesNotFit : if (newSizeInChunks > (entrySizeInChunks())) {
                if (newSizeInChunks > (m.maxChunksPerEntry)) {
                    throw new IllegalArgumentException(((((("Value too large: " + "entry takes ") + newSizeInChunks) + " chunks, ") + (m.maxChunksPerEntry)) + " is maximum."));
                }
                if (CompiledReplicatedMapIterationContext.this.freeList().allClear(((pos()) + (entrySizeInChunks())), ((pos()) + newSizeInChunks))) {
                    CompiledReplicatedMapIterationContext.this.freeList().set(((pos()) + (entrySizeInChunks())), ((pos()) + newSizeInChunks));
                    break newValueDoesNotFit;
                }
                relocation(newValue, newSizeOfEverythingBeforeValue);
                return ;
            } else if (newSizeInChunks < (entrySizeInChunks())) {
                CompiledReplicatedMapIterationContext.this.freeList().clear(((pos()) + newSizeInChunks), ((pos()) + (entrySizeInChunks())));
            }
        } else {
        }
        CompiledReplicatedMapIterationContext.this.innerWriteLock.lock();
        if (newValueSizeIsDifferent) {
            initValue(newValue);
        } else {
            writeValue(newValue);
        }
        CompiledReplicatedMapIterationContext.this.putValueVolatile(CompiledReplicatedMapIterationContext.this.hashLookupPos(), pos());
    }

    public void raiseChange() {
        CompiledReplicatedMapIterationContext.this.m().raiseChange(CompiledReplicatedMapIterationContext.this.segmentIndex(), CompiledReplicatedMapIterationContext.this.pos());
    }

    public void updateChange() {
        if (!(replicationUpdateInit())) {
            raiseChange();
        }
    }

    private void _MapSegmentIteration_doRemove() {
        CompiledReplicatedMapIterationContext.this.checkOnEachPublicOperation();
        initEntryRemovedOnThisIteration(true);
        CompiledReplicatedMapIterationContext.this.innerWriteLock.lock();
        try {
            if ((CompiledReplicatedMapIterationContext.this.remove(CompiledReplicatedMapIterationContext.this.hashLookupPos())) != (CompiledReplicatedMapIterationContext.this.hashLookupPos())) {
                CompiledReplicatedMapIterationContext.this.setHashLookupPosGuarded(CompiledReplicatedMapIterationContext.this.stepBack(CompiledReplicatedMapIterationContext.this.hashLookupPos()));
            }
            CompiledReplicatedMapIterationContext.this.innerRemoveEntryExceptHashLookupUpdate();
        } finally {
            CompiledReplicatedMapIterationContext.this.innerWriteLock.unlock();
        }
    }

    @Override
    public void doRemove() {
        CompiledReplicatedMapIterationContext.this.checkOnEachPublicOperation();
        initEntryRemovedOnThisIteration(true);
        try {
            if ((CompiledReplicatedMapIterationContext.this.valueSize()) > (CompiledReplicatedMapIterationContext.this.dummyValue.size()))
                CompiledReplicatedMapIterationContext.this.innerDefaultReplaceValue(CompiledReplicatedMapIterationContext.this.dummyValue);

            CompiledReplicatedMapIterationContext.this.updatedReplicationStateOnPresentEntry();
            CompiledReplicatedMapIterationContext.this.writeEntryDeleted();
            CompiledReplicatedMapIterationContext.this.updateChange();
            CompiledReplicatedMapIterationContext.this.deleted(((CompiledReplicatedMapIterationContext.this.deleted()) + 1));
        } finally {
            CompiledReplicatedMapIterationContext.this.innerWriteLock.unlock();
        }
    }

    private void _MapSegmentIteration_doReplaceValue(Data<V, ?> newValue) {
        CompiledReplicatedMapIterationContext.this.checkOnEachPublicOperation();
        try {
            CompiledReplicatedMapIterationContext.this.innerDefaultReplaceValue(newValue);
        } finally {
            CompiledReplicatedMapIterationContext.this.innerWriteLock.unlock();
        }
    }

    @Override
    public void doReplaceValue(Data<V, ?> newValue) {
        CompiledReplicatedMapIterationContext.this.checkOnEachPublicOperation();
        try {
            CompiledReplicatedMapIterationContext.this.innerDefaultReplaceValue(newValue);
            CompiledReplicatedMapIterationContext.this.updateChange();
            CompiledReplicatedMapIterationContext.this.updatedReplicationStateOnPresentEntry();
        } finally {
            CompiledReplicatedMapIterationContext.this.innerWriteLock.unlock();
        }
    }

    @Override
    public void raiseChanged() {
        CompiledReplicatedMapIterationContext.this.checkOnEachPublicOperation();
        CompiledReplicatedMapIterationContext.this.innerUpdateLock.lock();
        CompiledReplicatedMapIterationContext.this.raiseChange();
    }
}
