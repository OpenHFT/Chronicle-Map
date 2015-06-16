package net.openhft.chronicle.map.impl;

import net.openhft.chronicle.bytes.Access;
import net.openhft.chronicle.bytes.Accessor;
import net.openhft.chronicle.bytes.Accessor.Full;
import net.openhft.chronicle.bytes.ReadAccess;
import net.openhft.chronicle.hash.AbstractData;
import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.HashEntry;
import net.openhft.chronicle.hash.impl.*;
import net.openhft.chronicle.hash.impl.hashlookup.EntryConsumer;
import net.openhft.chronicle.hash.locks.InterProcessLock;
import net.openhft.chronicle.hash.locks.InterProcessReadWriteUpdateLock;
import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.chronicle.hash.serialization.internal.MetaBytesInterop;
import net.openhft.chronicle.map.*;
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
import java.util.function.Predicate;

public class CompiledMapIterationContext<K, KI, MKI extends MetaBytesInterop<K, ? super KI>, V, VI, MVI extends MetaBytesInterop<V, ? super VI>, R, T> implements AutoCloseable , HashEntry<K> , InterProcessReadWriteUpdateLock , MapContext<K, V, R> , MapEntry<K, V> , IterationContextInterface<K, V> , VanillaChronicleMapHolder<K, KI, MKI, V, VI, MVI, R> {
    public void close() {
        CompiledMapIterationContext.this.closeHashLookupPos();
        CompiledMapIterationContext.this.closeKeyOffset();
        CompiledMapIterationContext.this.wrappedValueInstanceValue.closeNext();
        CompiledMapIterationContext.this.closeEntryRemovedOnThisIteration();
        CompiledMapIterationContext.this.closePos();
        CompiledMapIterationContext.this.wrappedValueInstanceValue.closeValue();
        CompiledMapIterationContext.this.closeTheSegmentIndex();
        CompiledMapIterationContext.this.closeKeySize();
        CompiledMapIterationContext.this.closeUsed();
        CompiledMapIterationContext.this.closeAllocatedChunks();
        CompiledMapIterationContext.this.closeValueBytesInteropValueMetaInteropDependants();
        CompiledMapIterationContext.this.closeMapSegmentIterationCheckEntryNotRemovedOnThisIterationDependants();
        CompiledMapIterationContext.this.closeVanillaChronicleMapHolderImplContextAtIndexInChainDependants();
        CompiledMapIterationContext.this.closeOwnerThreadHolderCheckAccessingFromOwnerThreadDependants();
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

    public CompiledMapIterationContext(VanillaChronicleMap<K, KI, MKI, V, VI, MVI, R> m) {
        contextChain = new ArrayList<CompiledMapIterationContext>();
        contextChain.add(this);
        indexInContextChain = 0;
        this.m = m;
        this.entryKey = new EntryKeyBytesData();
        this.entryValue = new EntryValueBytesData();
        this.innerReadLock = new ReadLock();
        this.deprecatedMapKeyContextOnIteration = new DeprecatedMapKeyContextOnIteration();
        this.entryBytes = CompiledMapIterationContext.this.h().ms.bytes();
        this.entryBytesAccessor = JavaLangBytesAccessors.uncheckedBytesAccessor(entryBytes);
        this.entryBytesAccessHandle = ((T)(entryBytesAccessor.handle(entryBytes)));
        this.entryBytesAccess = ((Access<T>)(entryBytesAccessor.access(entryBytes)));
        this.innerWriteLock = new WriteLock();
        this.owner = Thread.currentThread();
        this.innerUpdateLock = new UpdateLock();
        this.copies = ThreadLocalCopies.get();
        this.valueInterop = CompiledMapIterationContext.this.m().valueInteropProvider.get(CompiledMapIterationContext.this.copies, CompiledMapIterationContext.this.m().originalValueInterop);
        this.valueReader = CompiledMapIterationContext.this.m().valueReaderProvider.get(CompiledMapIterationContext.this.copies, CompiledMapIterationContext.this.m().originalValueReader);
        this.keyInterop = CompiledMapIterationContext.this.h().keyInteropProvider.get(CompiledMapIterationContext.this.copies, CompiledMapIterationContext.this.h().originalKeyInterop);
        this.keyReader = CompiledMapIterationContext.this.h().keyReaderProvider.get(CompiledMapIterationContext.this.copies, CompiledMapIterationContext.this.h().originalKeyReader);
        this.wrappedValueInstanceValue = new WrappedValueInstanceData();
    }

    public CompiledMapIterationContext(CompiledMapIterationContext c) {
        contextChain = c.contextChain;
        indexInContextChain = contextChain.size();
        contextChain.add(this);
        this.m = ((VanillaChronicleMap<K, KI, MKI, V, VI, MVI, R>)(c.m));
        this.entryKey = new EntryKeyBytesData();
        this.entryValue = new EntryValueBytesData();
        this.innerReadLock = new ReadLock();
        this.deprecatedMapKeyContextOnIteration = new DeprecatedMapKeyContextOnIteration();
        this.entryBytes = CompiledMapIterationContext.this.h().ms.bytes();
        this.entryBytesAccessor = JavaLangBytesAccessors.uncheckedBytesAccessor(entryBytes);
        this.entryBytesAccessHandle = ((T)(entryBytesAccessor.handle(entryBytes)));
        this.entryBytesAccess = ((Access<T>)(entryBytesAccessor.access(entryBytes)));
        this.innerWriteLock = new WriteLock();
        this.owner = Thread.currentThread();
        this.innerUpdateLock = new UpdateLock();
        this.copies = ThreadLocalCopies.get();
        this.valueInterop = CompiledMapIterationContext.this.m().valueInteropProvider.get(CompiledMapIterationContext.this.copies, CompiledMapIterationContext.this.m().originalValueInterop);
        this.valueReader = CompiledMapIterationContext.this.m().valueReaderProvider.get(CompiledMapIterationContext.this.copies, CompiledMapIterationContext.this.m().originalValueReader);
        this.keyInterop = CompiledMapIterationContext.this.h().keyInteropProvider.get(CompiledMapIterationContext.this.copies, CompiledMapIterationContext.this.h().originalKeyInterop);
        this.keyReader = CompiledMapIterationContext.this.h().keyReaderProvider.get(CompiledMapIterationContext.this.copies, CompiledMapIterationContext.this.h().originalKeyReader);
        this.wrappedValueInstanceValue = new WrappedValueInstanceData();
    }

    public class DeprecatedMapKeyContextOnIteration implements MapKeyContext<K, V> {
        @NotNull
        private UnsupportedOperationException unsupportedLocks() {
            return new UnsupportedOperationException("Lock operations are not supported (and not needed!) during iteration");
        }

        @NotNull
        @Override
        public InterProcessLock readLock() {
            throw unsupportedLocks();
        }

        @NotNull
        @Override
        public InterProcessLock updateLock() {
            throw unsupportedLocks();
        }

        @NotNull
        @Override
        public InterProcessLock writeLock() {
            throw unsupportedLocks();
        }

        @Override
        public void close() {
            throw new UnsupportedOperationException("close() is not supported during iteration");
        }

        @NotNull
        @Override
        public Bytes entry() {
            CompiledMapIterationContext.this.checkOnEachPublicOperation();
            return CompiledMapIterationContext.this.entryBytes;
        }

        @NotNull
        @Override
        public K key() {
            CompiledMapIterationContext.this.checkOnEachPublicOperation();
            return CompiledMapIterationContext.this.key().get();
        }

        @Override
        public V get() {
            CompiledMapIterationContext.this.checkOnEachPublicOperation();
            return CompiledMapIterationContext.this.value().get();
        }

        @Override
        public V getUsing(V usingValue) {
            CompiledMapIterationContext.this.checkOnEachPublicOperation();
            return CompiledMapIterationContext.this.value().getUsing(usingValue);
        }

        @Override
        public boolean remove() {
            CompiledMapIterationContext.this.checkOnEachPublicOperation();
            CompiledMapIterationContext.this.remove(CompiledMapIterationContext.this);
            return true;
        }

        @Override
        public boolean containsKey() {
            CompiledMapIterationContext.this.checkOnEachPublicOperation();
            return true;
        }

        @Override
        public boolean valueEqualTo(V value) {
            CompiledMapIterationContext.this.checkOnEachPublicOperation();
            return Data.bytesEquivalent(CompiledMapIterationContext.this.entryValue, CompiledMapIterationContext.this.context().wrapValueAsValue(value));
        }

        @Override
        public boolean put(V newValue) {
            CompiledMapIterationContext.this.checkOnEachPublicOperation();
            CompiledMapIterationContext.this.replaceValue(CompiledMapIterationContext.this, CompiledMapIterationContext.this.context().wrapValueAsValue(newValue));
            return true;
        }

        @Override
        public long keySize() {
            CompiledMapIterationContext.this.checkOnEachPublicOperation();
            return CompiledMapIterationContext.this.keySize();
        }

        @Override
        public long keyOffset() {
            CompiledMapIterationContext.this.checkOnEachPublicOperation();
            return CompiledMapIterationContext.this.keyOffset();
        }

        @Override
        public long valueOffset() {
            CompiledMapIterationContext.this.checkOnEachPublicOperation();
            return CompiledMapIterationContext.this.valueOffset();
        }

        @Override
        public long valueSize() {
            CompiledMapIterationContext.this.checkOnEachPublicOperation();
            return CompiledMapIterationContext.this.valueSize();
        }
    }

    public class EntryKeyBytesData extends AbstractData<K, T> {
        @Override
        public ReadAccess<T> access() {
            CompiledMapIterationContext.this.checkOnEachPublicOperation();
            return CompiledMapIterationContext.this.entryBytesAccess;
        }

        @Override
        public T handle() {
            CompiledMapIterationContext.this.checkOnEachPublicOperation();
            return CompiledMapIterationContext.this.entryBytesAccessHandle;
        }

        @Override
        public long size() {
            CompiledMapIterationContext.this.checkOnEachPublicOperation();
            return CompiledMapIterationContext.this.keySize();
        }

        public void closeEntryKeyBytesDataSizeDependants() {
            EntryKeyBytesData.this.closeEntryKeyBytesDataInnerGetUsingDependants();
        }

        @Override
        public long offset() {
            CompiledMapIterationContext.this.checkOnEachPublicOperation();
            return CompiledMapIterationContext.this.entryBytesAccessOffset(CompiledMapIterationContext.this.keyOffset());
        }

        private K innerGetUsing(K usingKey) {
            CompiledMapIterationContext.this.entryBytes.position(CompiledMapIterationContext.this.keyOffset());
            return CompiledMapIterationContext.this.keyReader.read(CompiledMapIterationContext.this.entryBytes, size(), usingKey);
        }

        public void closeEntryKeyBytesDataInnerGetUsingDependants() {
            EntryKeyBytesData.this.closeCachedEntryKey();
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
            CompiledMapIterationContext.this.checkOnEachPublicOperation();
            return cachedEntryKey();
        }

        @Override
        public K getUsing(K usingKey) {
            CompiledMapIterationContext.this.checkOnEachPublicOperation();
            return innerGetUsing(usingKey);
        }
    }

    public class EntryValueBytesData extends AbstractData<V, T> {
        @Override
        public T handle() {
            CompiledMapIterationContext.this.checkOnEachPublicOperation();
            return CompiledMapIterationContext.this.entryBytesAccessHandle;
        }

        @Override
        public ReadAccess<T> access() {
            CompiledMapIterationContext.this.checkOnEachPublicOperation();
            return CompiledMapIterationContext.this.entryBytesAccess;
        }

        @Override
        public long size() {
            CompiledMapIterationContext.this.checkOnEachPublicOperation();
            return CompiledMapIterationContext.this.valueSize();
        }

        public void closeEntryValueBytesDataSizeDependants() {
            EntryValueBytesData.this.closeEntryValueBytesDataInnerGetUsingDependants();
        }

        @Override
        public long offset() {
            CompiledMapIterationContext.this.checkOnEachPublicOperation();
            return CompiledMapIterationContext.this.entryBytesAccessOffset(CompiledMapIterationContext.this.valueOffset());
        }

        private V innerGetUsing(V usingValue) {
            CompiledMapIterationContext.this.entryBytes.position(CompiledMapIterationContext.this.valueOffset());
            return CompiledMapIterationContext.this.valueReader.read(CompiledMapIterationContext.this.entryBytes, size(), usingValue);
        }

        public void closeEntryValueBytesDataInnerGetUsingDependants() {
            EntryValueBytesData.this.closeCachedEntryValue();
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
            CompiledMapIterationContext.this.checkOnEachPublicOperation();
            return cachedEntryValue();
        }

        @Override
        public V getUsing(V usingValue) {
            CompiledMapIterationContext.this.checkOnEachPublicOperation();
            return innerGetUsing(usingValue);
        }
    }

    public class ReadLock implements InterProcessLock {
        @Override
        public void lockInterruptibly() throws InterruptedException {
            if ((CompiledMapIterationContext.this.localLockState()) == (LocalLockState.UNLOCKED)) {
                CompiledMapIterationContext.this.segmentHeader().readLockInterruptibly(CompiledMapIterationContext.this.segmentHeaderAddress());
                CompiledMapIterationContext.this.setLocalLockStateGuarded(LocalLockState.READ_LOCKED);
            }
        }

        @Override
        public void lock() {
            if ((CompiledMapIterationContext.this.localLockState()) == (LocalLockState.UNLOCKED)) {
                CompiledMapIterationContext.this.segmentHeader().readLock(CompiledMapIterationContext.this.segmentHeaderAddress());
                CompiledMapIterationContext.this.setLocalLockStateGuarded(LocalLockState.READ_LOCKED);
            }
        }

        @Override
        public boolean isHeldByCurrentThread() {
            return CompiledMapIterationContext.this.localLockState().read;
        }

        @Override
        public boolean tryLock() {
            if ((CompiledMapIterationContext.this.localLockState()) == (LocalLockState.UNLOCKED)) {
                if (CompiledMapIterationContext.this.segmentHeader().tryReadLock(CompiledMapIterationContext.this.segmentHeaderAddress())) {
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
        public void unlock() {
            switch (CompiledMapIterationContext.this.localLockState()) {
                case UNLOCKED :
                    return ;
                case READ_LOCKED :
                    CompiledMapIterationContext.this.segmentHeader().readUnlock(CompiledMapIterationContext.this.segmentHeaderAddress());
                    break;
                case UPDATE_LOCKED :
                    CompiledMapIterationContext.this.segmentHeader().updateUnlock(CompiledMapIterationContext.this.segmentHeaderAddress());
                    break;
                case WRITE_LOCKED :
                    CompiledMapIterationContext.this.segmentHeader().writeUnlock(CompiledMapIterationContext.this.segmentHeaderAddress());
            }
            CompiledMapIterationContext.this.setLocalLockStateGuarded(LocalLockState.UNLOCKED);
            CompiledMapIterationContext.this.closeHashLookupPos();
            CompiledMapIterationContext.this.closePos();
        }

        @Override
        public boolean tryLock(long time, @NotNull
        TimeUnit unit) throws InterruptedException {
            if ((CompiledMapIterationContext.this.localLockState()) == (LocalLockState.UNLOCKED)) {
                if (CompiledMapIterationContext.this.segmentHeader().tryReadLock(CompiledMapIterationContext.this.segmentHeaderAddress(), time, unit)) {
                    CompiledMapIterationContext.this.setLocalLockStateGuarded(LocalLockState.READ_LOCKED);
                    return true;
                } else {
                    return false;
                }
            } else {
                return true;
            }
        }
    }

    public class UpdateLock implements InterProcessLock {
        @NotNull
        private IllegalMonitorStateException forbiddenUpgrade() {
            return new IllegalMonitorStateException("Cannot upgrade from read to update lock");
        }

        @Override
        public boolean tryLock(long time, @NotNull
        TimeUnit unit) throws InterruptedException {
            switch (CompiledMapIterationContext.this.localLockState()) {
                case UNLOCKED :
                    if (CompiledMapIterationContext.this.segmentHeader().tryUpdateLock(CompiledMapIterationContext.this.segmentHeaderAddress(), time, unit)) {
                        CompiledMapIterationContext.this.setLocalLockStateGuarded(LocalLockState.UPDATE_LOCKED);
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
            switch (CompiledMapIterationContext.this.localLockState()) {
                case UNLOCKED :
                    CompiledMapIterationContext.this.segmentHeader().updateLockInterruptibly(CompiledMapIterationContext.this.segmentHeaderAddress());
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
            return CompiledMapIterationContext.this.localLockState().update;
        }

        @Override
        public boolean tryLock() {
            switch (CompiledMapIterationContext.this.localLockState()) {
                case UNLOCKED :
                    if (CompiledMapIterationContext.this.segmentHeader().tryUpdateLock(CompiledMapIterationContext.this.segmentHeaderAddress())) {
                        CompiledMapIterationContext.this.setLocalLockStateGuarded(LocalLockState.UPDATE_LOCKED);
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
        public void unlock() {
            switch (CompiledMapIterationContext.this.localLockState()) {
                case UNLOCKED :
                case READ_LOCKED :
                    return ;
                case UPDATE_LOCKED :
                    CompiledMapIterationContext.this.segmentHeader().downgradeUpdateToReadLock(CompiledMapIterationContext.this.segmentHeaderAddress());
                    break;
                case WRITE_LOCKED :
                    CompiledMapIterationContext.this.segmentHeader().downgradeWriteToReadLock(CompiledMapIterationContext.this.segmentHeaderAddress());
            }
            CompiledMapIterationContext.this.setLocalLockStateGuarded(LocalLockState.READ_LOCKED);
        }

        @Override
        public void lock() {
            switch (CompiledMapIterationContext.this.localLockState()) {
                case UNLOCKED :
                    CompiledMapIterationContext.this.segmentHeader().updateLock(CompiledMapIterationContext.this.segmentHeaderAddress());
                    CompiledMapIterationContext.this.setLocalLockStateGuarded(LocalLockState.UPDATE_LOCKED);
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

        private V value;

        public boolean valueInit() {
            return (value) != null;
        }

        public void initValue(V value) {
            CompiledMapIterationContext.this.m().checkValue(value);
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

        private boolean marshalled = false;

        private DirectBytes buf;

        public boolean bufferInit() {
            return (this.marshalled) != false;
        }

        private void initBuffer() {
            MVI mvi = CompiledMapIterationContext.this.valueMetaInterop(value());
            long size = mvi.size(CompiledMapIterationContext.this.valueInterop, value());
            buf = CopyingInstanceData.getBuffer(this.buf, size);
            mvi.write(CompiledMapIterationContext.this.valueInterop, buf, value());
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
            return CompiledMapIterationContext.this.valueReader.read(buf(), buf().limit(), usingValue);
        }

        @Override
        public V instance() {
            return value();
        }

        private WrappedValueInstanceData next;

        boolean nextInit() {
            return true;
        }

        void closeNext() {
            if (!(this.nextInit()))
                return ;

        }
    }

    public class WriteLock implements InterProcessLock {
        @NotNull
        private IllegalMonitorStateException forbiddenUpgrade() {
            return new IllegalMonitorStateException("Cannot upgrade from read to write lock");
        }

        @Override
        public boolean tryLock(long time, @NotNull
        TimeUnit unit) throws InterruptedException {
            switch (CompiledMapIterationContext.this.localLockState()) {
                case UNLOCKED :
                    if (CompiledMapIterationContext.this.segmentHeader().tryWriteLock(CompiledMapIterationContext.this.segmentHeaderAddress(), time, unit)) {
                        CompiledMapIterationContext.this.setLocalLockStateGuarded(LocalLockState.WRITE_LOCKED);
                        return true;
                    } else {
                        return false;
                    }
                case READ_LOCKED :
                    throw forbiddenUpgrade();
                case UPDATE_LOCKED :
                    if (CompiledMapIterationContext.this.segmentHeader().tryUpgradeUpdateToWriteLock(CompiledMapIterationContext.this.segmentHeaderAddress(), time, unit)) {
                        CompiledMapIterationContext.this.setLocalLockStateGuarded(LocalLockState.WRITE_LOCKED);
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
        public boolean tryLock() {
            switch (CompiledMapIterationContext.this.localLockState()) {
                case UNLOCKED :
                    if (CompiledMapIterationContext.this.segmentHeader().tryWriteLock(CompiledMapIterationContext.this.segmentHeaderAddress())) {
                        CompiledMapIterationContext.this.setLocalLockStateGuarded(LocalLockState.WRITE_LOCKED);
                        return true;
                    } else {
                        return false;
                    }
                case READ_LOCKED :
                    throw forbiddenUpgrade();
                case UPDATE_LOCKED :
                    if (CompiledMapIterationContext.this.segmentHeader().tryUpgradeUpdateToWriteLock(CompiledMapIterationContext.this.segmentHeaderAddress())) {
                        CompiledMapIterationContext.this.setLocalLockStateGuarded(LocalLockState.WRITE_LOCKED);
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
        public void lockInterruptibly() throws InterruptedException {
            switch (CompiledMapIterationContext.this.localLockState()) {
                case UNLOCKED :
                    CompiledMapIterationContext.this.segmentHeader().writeLockInterruptibly(CompiledMapIterationContext.this.segmentHeaderAddress());
                    CompiledMapIterationContext.this.setLocalLockStateGuarded(LocalLockState.WRITE_LOCKED);
                    return ;
                case READ_LOCKED :
                    throw forbiddenUpgrade();
                case UPDATE_LOCKED :
                    CompiledMapIterationContext.this.segmentHeader().upgradeUpdateToWriteLockInterruptibly(CompiledMapIterationContext.this.segmentHeaderAddress());
                    CompiledMapIterationContext.this.setLocalLockStateGuarded(LocalLockState.WRITE_LOCKED);
                case WRITE_LOCKED :
            }
        }

        @Override
        public void unlock() {
            switch (CompiledMapIterationContext.this.localLockState()) {
                case UNLOCKED :
                case READ_LOCKED :
                case UPDATE_LOCKED :
                    return ;
                case WRITE_LOCKED :
                    CompiledMapIterationContext.this.segmentHeader().downgradeWriteToUpdateLock(CompiledMapIterationContext.this.segmentHeaderAddress());
            }
            CompiledMapIterationContext.this.setLocalLockStateGuarded(LocalLockState.UPDATE_LOCKED);
        }

        @Override
        public boolean isHeldByCurrentThread() {
            return CompiledMapIterationContext.this.localLockState().write;
        }

        @Override
        public void lock() {
            switch (CompiledMapIterationContext.this.localLockState()) {
                case UNLOCKED :
                    CompiledMapIterationContext.this.segmentHeader().writeLock(CompiledMapIterationContext.this.segmentHeaderAddress());
                    CompiledMapIterationContext.this.setLocalLockStateGuarded(LocalLockState.WRITE_LOCKED);
                    return ;
                case READ_LOCKED :
                    throw forbiddenUpgrade();
                case UPDATE_LOCKED :
                    CompiledMapIterationContext.this.segmentHeader().upgradeUpdateToWriteLock(CompiledMapIterationContext.this.segmentHeaderAddress());
                    CompiledMapIterationContext.this.setLocalLockStateGuarded(LocalLockState.WRITE_LOCKED);
                case WRITE_LOCKED :
            }
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
        this.keyMask = CompiledMapIterationContext.mask(keyBits);
        this.valueMask = CompiledMapIterationContext.mask(valueBits);
        this.entryMask = CompiledMapIterationContext.mask((keyBits + valueBits));
    }

    private void unlinkFromSegmentContextsChain() {
        CompiledMapIterationContext prevContext = this.rootContextOnThisSegment;
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

    public final List<CompiledMapIterationContext> contextChain;

    public final UpdateLock innerUpdateLock;

    public UpdateLock innerUpdateLock() {
        return this.innerUpdateLock;
    }

    public List<CompiledMapIterationContext> contextChain() {
        return this.contextChain;
    }

    public final ThreadLocalCopies copies;

    public ThreadLocalCopies copies() {
        return this.copies;
    }

    private void countValueOffset() {
        CompiledMapIterationContext.this.m().alignment.alignPositionAddr(entryBytes);
        valueOffset = entryBytes.position();
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

    private final VanillaChronicleMap<K, KI, MKI, V, VI, MVI, R> m;

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

    long sizeOfEverythingBeforeValue(long keySize, long valueSize) {
        return (((CompiledMapIterationContext.this.m().metaDataBytes) + (CompiledMapIterationContext.this.m().keySizeMarshaller.sizeEncodingSize(keySize))) + keySize) + (CompiledMapIterationContext.this.m().valueSizeMarshaller.sizeEncodingSize(valueSize));
    }

    public void incrementSegmentEntriesIfNeeded() {
    }

    public long entryBytesAccessOffset(long offset) {
        return entryBytesAccessor.offset(entryBytes, offset);
    }

    public boolean entryIsPresent() {
        return true;
    }

    @Override
    public VanillaChronicleMap<K, KI, MKI, V, VI, MVI, R> m() {
        return m;
    }

    @Override
    public ChronicleMap<K, V> map() {
        return m();
    }

    public void checkAccessingFromOwnerThread() {
        if ((owner) != (Thread.currentThread())) {
            throw new ConcurrentModificationException("Context shouldn\'t be accessed from multiple threads");
        }
    }

    public void closeOwnerThreadHolderCheckAccessingFromOwnerThreadDependants() {
        CompiledMapIterationContext.this.closeInterationCheckOnEachPublicOperationCheckOnEachPublicOperationDependants();
    }

    public <T>T contextAtIndexInChain(int index) {
        return ((T)(contextChain.get(index)));
    }

    public void closeVanillaChronicleMapHolderImplContextAtIndexInChainDependants() {
        CompiledMapIterationContext.this.closeSegmentStagesTryFindInitLocksOfThisSegmentDependants();
    }

    public void checkEntryNotRemovedOnThisIteration() {
        if (entryRemovedOnThisIterationInit())
            throw new IllegalStateException("Entry was already removed on this iteration");

    }

    public void closeMapSegmentIterationCheckEntryNotRemovedOnThisIterationDependants() {
        CompiledMapIterationContext.this.closeInterationCheckOnEachPublicOperationCheckOnEachPublicOperationDependants();
    }

    private void _CheckOnEachPublicOperation_checkOnEachPublicOperation() {
        CompiledMapIterationContext.this.checkAccessingFromOwnerThread();
    }

    public void checkOnEachPublicOperation() {
        _CheckOnEachPublicOperation_checkOnEachPublicOperation();
        CompiledMapIterationContext.this.checkEntryNotRemovedOnThisIteration();
    }

    public void closeInterationCheckOnEachPublicOperationCheckOnEachPublicOperationDependants() {
        CompiledMapIterationContext.this.entryValue.closeEntryValueBytesDataSizeDependants();
        CompiledMapIterationContext.this.entryKey.closeEntryKeyBytesDataSizeDependants();
    }

    @NotNull
    @Override
    public Data<V, ?> value() {
        CompiledMapIterationContext.this.checkOnEachPublicOperation();
        return CompiledMapIterationContext.this.entryValue;
    }

    @NotNull
    @Override
    public Data<K, ?> key() {
        CompiledMapIterationContext.this.checkOnEachPublicOperation();
        return CompiledMapIterationContext.this.entryKey;
    }

    @NotNull
    @Override
    public InterProcessLock updateLock() {
        CompiledMapIterationContext.this.checkOnEachPublicOperation();
        return CompiledMapIterationContext.this.innerUpdateLock;
    }

    @Override
    public Data<V, ?> wrapValueAsValue(V value) {
        CompiledMapIterationContext.this.checkOnEachPublicOperation();
        WrappedValueInstanceData wrapped = CompiledMapIterationContext.this.wrappedValueInstanceValue;
        wrapped = wrapped.getUnusedWrappedValueGuarded();
        wrapped.initValue(value);
        return wrapped;
    }

    @NotNull
    @Override
    public InterProcessLock readLock() {
        CompiledMapIterationContext.this.checkOnEachPublicOperation();
        return CompiledMapIterationContext.this.innerReadLock;
    }

    @NotNull
    @Override
    public InterProcessLock writeLock() {
        CompiledMapIterationContext.this.checkOnEachPublicOperation();
        return CompiledMapIterationContext.this.innerWriteLock;
    }

    @Override
    public R insert(@NotNull
                    MapAbsentEntry<K, V> absentEntry, Data<V, ?> value) {
        CompiledMapIterationContext.this.checkOnEachPublicOperation();
        return CompiledMapIterationContext.this.m().entryOperations.insert(absentEntry, value);
    }

    @Override
    public Data<V, ?> defaultValue(@NotNull
                                                              MapAbsentEntry<K, V> absentEntry) {
        CompiledMapIterationContext.this.checkOnEachPublicOperation();
        return CompiledMapIterationContext.this.m().entryOperations.defaultValue(absentEntry);
    }

    @Override
    public R replaceValue(@NotNull
                          MapEntry<K, V> entry, Data<V, ?> newValue) {
        CompiledMapIterationContext.this.checkOnEachPublicOperation();
        return CompiledMapIterationContext.this.m().entryOperations.replaceValue(entry, newValue);
    }

    @Override
    public R remove(@NotNull
                    MapEntry<K, V> entry) {
        CompiledMapIterationContext.this.checkOnEachPublicOperation();
        return CompiledMapIterationContext.this.m().entryOperations.remove(entry);
    }

    public long innerEntrySize(long sizeOfEverythingBeforeValue, long valueSize) {
        if (CompiledMapIterationContext.this.m().constantlySizedEntry) {
            return CompiledMapIterationContext.this.m().alignment.alignAddr((sizeOfEverythingBeforeValue + valueSize));
        } else if (CompiledMapIterationContext.this.m().couldNotDetermineAlignmentBeforeAllocation) {
            return (sizeOfEverythingBeforeValue + (CompiledMapIterationContext.this.m().worstAlignment)) + valueSize;
        } else {
            return (CompiledMapIterationContext.this.m().alignment.alignAddr(sizeOfEverythingBeforeValue)) + valueSize;
        }
    }

    public final long entrySize(long keySize, long valueSize) {
        long sizeOfEverythingBeforeValue = sizeOfEverythingBeforeValue(keySize, valueSize);
        return innerEntrySize(sizeOfEverythingBeforeValue, valueSize);
    }

    public MKI keyMetaInterop(K key) {
        return CompiledMapIterationContext.this.h().metaKeyInteropProvider.get(CompiledMapIterationContext.this.copies, CompiledMapIterationContext.this.h().originalMetaKeyInterop, keyInterop, key);
    }

    public MVI valueMetaInterop(V value) {
        return CompiledMapIterationContext.this.m().metaValueInteropProvider.get(CompiledMapIterationContext.this.copies, CompiledMapIterationContext.this.m().originalMetaValueInterop, valueInterop, value);
    }

    public void closeValueBytesInteropValueMetaInteropDependants() {
        CompiledMapIterationContext.this.wrappedValueInstanceValue.closeBuffer();
    }

    private CompiledMapIterationContext _Chaining_createChaining() {
        return new CompiledMapIterationContext(this);
    }

    public CompiledMapIterationContext createChaining() {
        return new CompiledMapIterationContext(this);
    }

    public <T>T getContext() {
        for (CompiledMapIterationContext context : contextChain) {
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

    @NotNull
    @Override
    public MapContext<K, V, ?> context() {
        return CompiledMapIterationContext.this;
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
        CompiledMapIterationContext.this.entryKey.closeEntryKeyBytesDataSizeDependants();
        CompiledMapIterationContext.this.closeMapEntryStagesKeyEndDependants();
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
        CompiledMapIterationContext.this.closeSegmentHashLookup();
        CompiledMapIterationContext.this.closeSegment();
        CompiledMapIterationContext.this.closeSegHeader();
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
        long hashLookupOffset = CompiledMapIterationContext.this.h().segmentOffset(CompiledMapIterationContext.this.segmentIndex());
        innerInitSegmentHashLookup(((CompiledMapIterationContext.this.h().ms.address()) + hashLookupOffset), CompiledMapIterationContext.this.h().segmentHashLookupCapacity, CompiledMapIterationContext.this.h().segmentHashLookupEntrySize, CompiledMapIterationContext.this.h().segmentHashLookupKeyBits, CompiledMapIterationContext.this.h().segmentHashLookupValueBits);
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

    public long key(long entry) {
        return entry & (keyMask());
    }

    public void clearHashLookup() {
        NativeBytes.UNSAFE.setMemory(address(), ((capacityMask2()) + (hashLookupEntrySize())), ((byte)(0)));
    }

    public long maskUnsetKey(long key) {
        return (key &= keyMask()) != (UNSET_KEY) ? key : keyMask();
    }

    public boolean empty(long entry) {
        return (entry & (entryMask())) == (UNSET_ENTRY);
    }

    void writeEntry(long pos, long prevEntry, long anotherEntry) {
        long entry = (prevEntry & (~(entryMask()))) | (anotherEntry & (entryMask()));
        NativeBytes.UNSAFE.putLong(((address()) + pos), entry);
    }

    public long stepBack(long pos) {
        return (pos -= hashLookupEntrySize()) >= 0 ? pos : capacityMask2();
    }

    public void checkValueForPut(long value) {
        assert (value & (~(valueMask()))) == 0L : "Value out of range, was " + value;
    }

    long indexToPos(long index) {
        return index * (hashLookupEntrySize());
    }

    public long readEntry(long pos) {
        return NativeBytes.UNSAFE.getLong(((address()) + pos));
    }

    long entry(long key, long value) {
        return key | (value << (keyBits()));
    }

    public void writeEntry(long pos, long prevEntry, long key, long value) {
        long entry = (prevEntry & (~(entryMask()))) | (entry(key, value));
        NativeBytes.UNSAFE.putLong(((address()) + pos), entry);
    }

    public void writeEntryVolatile(long pos, long prevEntry, long key, long value) {
        long entry = (prevEntry & (~(entryMask()))) | (entry(key, value));
        NativeBytes.UNSAFE.putLongVolatile(null, ((address()) + pos), entry);
    }

    public void putValueVolatile(long pos, long value) {
        checkValueForPut(value);
        long currentEntry = readEntry(pos);
        writeEntryVolatile(pos, currentEntry, key(currentEntry), value);
    }

    public long hlPos(long key) {
        return indexToPos((key & (capacityMask())));
    }

    public long step(long pos) {
        return (pos += hashLookupEntrySize()) <= (capacityMask2()) ? pos : 0L;
    }

    void clearEntry(long pos, long prevEntry) {
        long entry = prevEntry & (~(entryMask()));
        NativeBytes.UNSAFE.putLong(((address()) + pos), entry);
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

    public long value(long entry) {
        return (entry >>> (keyBits())) & (valueMask());
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

    long entrySpaceOffset = 0;

    MultiStoreBytes freeListBytes = new MultiStoreBytes();

    public SingleThreadedDirectBitSet freeList = new SingleThreadedDirectBitSet();

    boolean segmentInit() {
        return (entrySpaceOffset) > 0;
    }

    void initSegment() {
        VanillaChronicleHash<?, ?, ?, ?, ?> h = CompiledMapIterationContext.this.h();
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
        CompiledMapIterationContext.this.closeEntryOffset();
    }

    long segmentHeaderAddress;

    SegmentHeader segmentHeader = null;

    public boolean segHeaderInit() {
        return (this.segmentHeader) != null;
    }

    private void initSegHeader() {
        segmentHeaderAddress = (CompiledMapIterationContext.this.h().ms.address()) + (CompiledMapIterationContext.this.h().segmentHeaderOffset(segmentIndex()));
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
        CompiledMapIterationContext.this.closeSegmentStagesTryFindInitLocksOfThisSegmentDependants();
        CompiledMapIterationContext.this.closeLocks();
    }

    boolean tryFindInitLocksOfThisSegment(Object thisContext, int index) {
        CompiledMapIterationContext c = CompiledMapIterationContext.this.contextAtIndexInChain(index);
        if ((((c.segmentHeader()) != null) && ((c.segmentHeaderAddress()) == (segmentHeaderAddress()))) && ((c.rootContextOnThisSegment()) != null)) {
            throw new IllegalStateException("Nested context not implemented yet");
        } else {
            return false;
        }
    }

    public void closeSegmentStagesTryFindInitLocksOfThisSegmentDependants() {
        CompiledMapIterationContext.this.closeLocks();
    }

    int totalReadLockCount;

    int totalUpdateLockCount;

    int totalWriteLockCount;

    public int latestSameThreadSegmentModCount;

    public int contextModCount;

    CompiledMapIterationContext nextNode;

    public boolean concurrentSameThreadContexts;

    LocalLockState localLockState;

    public CompiledMapIterationContext rootContextOnThisSegment = null;

    public boolean locksInit() {
        return (this.rootContextOnThisSegment) != null;
    }

    void initLocks() {
        localLockState = LocalLockState.UNLOCKED;
        int indexOfThisContext = CompiledMapIterationContext.this.indexInContextChain;
        for (int i = indexOfThisContext - 1 ; i >= 0 ; i--) {
            if (tryFindInitLocksOfThisSegment(this, i))
                return ;

        }
        for (int i = indexOfThisContext + 1, size = CompiledMapIterationContext.this.contextChain.size() ; i < size ; i++) {
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

    public CompiledMapIterationContext rootContextOnThisSegment() {
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

    public long entries() {
        return segmentHeader().size(segmentHeaderAddress());
    }

    long nextPosToSearchFrom() {
        return segmentHeader().nextPosToSearchFrom(segmentHeaderAddress());
    }

    public void entries(long size) {
        segmentHeader().size(segmentHeaderAddress(), size);
    }

    public void nextPosToSearchFrom(long nextPosToSearchFrom) {
        segmentHeader().nextPosToSearchFrom(segmentHeaderAddress(), nextPosToSearchFrom);
    }

    public void updateNextPosToSearchFrom(long allocated, int chunks) {
        long nextPosToSearchFrom = allocated + chunks;
        if (nextPosToSearchFrom >= (CompiledMapIterationContext.this.h().actualChunksPerSegment))
            nextPosToSearchFrom = 0L;

        nextPosToSearchFrom(nextPosToSearchFrom);
    }

    public long alloc(int chunks) {
        VanillaChronicleHash<?, ?, ?, ?, ?> h = CompiledMapIterationContext.this.h();
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

    public void free(long fromPos, int chunks) {
        freeList().clear(fromPos, (fromPos + chunks));
        if (fromPos < (nextPosToSearchFrom()))
            nextPosToSearchFrom(fromPos);

    }

    public void deleted(long deleted) {
        segmentHeader().deleted(segmentHeaderAddress(), deleted);
    }

    public void clearSegment() {
        CompiledMapIterationContext.this.innerWriteLock.lock();
        CompiledMapIterationContext.this.clearHashLookup();
        freeList().clear();
        nextPosToSearchFrom(0L);
        entries(0L);
    }

    public void clear() {
        clearSegment();
    }

    public long deleted() {
        return segmentHeader().deleted(segmentHeaderAddress());
    }

    public long size() {
        return (entries()) - (deleted());
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
        CompiledMapIterationContext.this.closeEntryOffset();
    }

    public long keySizeOffset = -1;

    public boolean entryOffsetInit() {
        return (this.keySizeOffset) >= 0;
    }

    public void initEntryOffset() {
        keySizeOffset = (CompiledMapIterationContext.this.entrySpaceOffset()) + ((pos()) * (CompiledMapIterationContext.this.h().chunkSize));
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
        CompiledMapIterationContext.this.closeMapEntryStagesEntrySizeDependants();
    }

    public void readExistingEntry(long pos) {
        initPos(pos);
        entryBytes.position(keySizeOffset());
        initKeySize(CompiledMapIterationContext.this.h().keySizeMarshaller.readSize(entryBytes));
        initKeyOffset(entryBytes.position());
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
        CompiledMapIterationContext.this.entryKey.closeEntryKeyBytesDataInnerGetUsingDependants();
        CompiledMapIterationContext.this.closeMapEntryStagesKeyEndDependants();
    }

    public void writeNewEntry(long pos, Data<?, ?> key) {
        initPos(pos);
        initKeySize(key.size());
        entryBytes.position(keySizeOffset());
        CompiledMapIterationContext.this.h().keySizeMarshaller.writeSize(entryBytes, keySize());
        initKeyOffset(entryBytes.position());
        key.writeTo(entryBytesAccessor, entryBytes, keyOffset());
    }

    public long keyEnd() {
        return (keyOffset()) + (keySize());
    }

    public void closeMapEntryStagesKeyEndDependants() {
        CompiledMapIterationContext.this.closeMapEntryStagesCountValueSizeOffsetDependants();
        CompiledMapIterationContext.this.closeMapEntryStagesEntryEndDependants();
    }

    long countValueSizeOffset() {
        return keyEnd();
    }

    public void closeMapEntryStagesCountValueSizeOffsetDependants() {
        CompiledMapIterationContext.this.closeValueSizeOffset();
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
        CompiledMapIterationContext.this.closeValSize();
    }

    public long newSizeOfEverythingBeforeValue(Data<V, ?> newValue) {
        return ((valueSizeOffset()) + (CompiledMapIterationContext.this.m().valueSizeMarshaller.sizeEncodingSize(newValue.size()))) - (keySizeOffset());
    }

    public long valueSize = -1;

    public long valueOffset;

    public boolean valSizeInit() {
        return (this.valueSize) >= 0;
    }

    void initValSize() {
        entryBytes.position(valueSizeOffset());
        valueSize = CompiledMapIterationContext.this.m().readValueSize(entryBytes);
        countValueOffset();
        this.closeValSizeDependants();
    }

    void initValSize(long valueSize) {
        this.valueSize = valueSize;
        entryBytes.position(valueSizeOffset());
        CompiledMapIterationContext.this.m().valueSizeMarshaller.writeSize(entryBytes, valueSize);
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
        CompiledMapIterationContext.this.entryValue.closeEntryValueBytesDataSizeDependants();
        CompiledMapIterationContext.this.entryValue.closeEntryValueBytesDataInnerGetUsingDependants();
        CompiledMapIterationContext.this.closeMapEntryStagesEntryEndDependants();
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

    private long _HashEntryStages_entryEnd() {
        return keyEnd();
    }

    protected long entryEnd() {
        return (valueOffset()) + (valueSize());
    }

    public void closeMapEntryStagesEntryEndDependants() {
        CompiledMapIterationContext.this.closeMapEntryStagesEntrySizeDependants();
    }

    long entrySize() {
        return (entryEnd()) - (keySizeOffset());
    }

    public void closeMapEntryStagesEntrySizeDependants() {
        CompiledMapIterationContext.this.closeTheEntrySizeInChunks();
    }

    public int entrySizeInChunks = 0;

    public boolean theEntrySizeInChunksInit() {
        return (this.entrySizeInChunks) != 0;
    }

    void initTheEntrySizeInChunks() {
        entrySizeInChunks = CompiledMapIterationContext.this.h().inChunks(entrySize());
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
        CompiledMapIterationContext.this.free(pos(), entrySizeInChunks());
        CompiledMapIterationContext.this.entries(((CompiledMapIterationContext.this.entries()) - 1L));
        CompiledMapIterationContext.this.incrementModCountGuarded();
    }

    public final void freeExtraAllocatedChunks() {
        if (((!(CompiledMapIterationContext.this.m().constantlySizedEntry)) && (CompiledMapIterationContext.this.m().couldNotDetermineAlignmentBeforeAllocation)) && ((entrySizeInChunks()) < (CompiledMapIterationContext.this.allocatedChunks()))) {
            CompiledMapIterationContext.this.free(((pos()) + (entrySizeInChunks())), ((CompiledMapIterationContext.this.allocatedChunks()) - (entrySizeInChunks())));
        } else {
            initTheEntrySizeInChunks(CompiledMapIterationContext.this.allocatedChunks());
        }
    }

    public void copyExistingEntry(long newPos, long bytesToCopy) {
        long oldKeySizeOffset = keySizeOffset();
        long oldKeyOffset = keyOffset();
        initPos(newPos);
        initKeyOffset(((keySizeOffset()) + (oldKeyOffset - oldKeySizeOffset)));
        Access.copy(entryBytesAccess, entryBytesAccessHandle, entryBytesAccessOffset(oldKeySizeOffset), entryBytesAccess, entryBytesAccessHandle, entryBytesAccessOffset(keySizeOffset()), bytesToCopy);
    }

    public void initEntryAndKeyCopying(long entrySize, long bytesToCopy) {
        initAllocatedChunks(CompiledMapIterationContext.this.h().inChunks(entrySize));
        CompiledMapIterationContext.this.copyExistingEntry(CompiledMapIterationContext.this.alloc(allocatedChunks()), bytesToCopy);
        incrementSegmentEntriesIfNeeded();
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

    public void putValueDeletedEntry(Data<V, ?> newValue) {
        assert CompiledMapIterationContext.this.innerUpdateLock.isHeldByCurrentThread();
        int newSizeInChunks;
        long entryStartOffset = keySizeOffset();
        long newSizeOfEverythingBeforeValue = -1;
        boolean newValueSizeIsDifferent = (newValue.size()) != (valueSize());
        if (newValueSizeIsDifferent) {
            newSizeOfEverythingBeforeValue = newSizeOfEverythingBeforeValue(newValue);
            long newValueOffset = CompiledMapIterationContext.this.m().alignment.alignAddr((entryStartOffset + newSizeOfEverythingBeforeValue));
            long newEntrySize = (newValueOffset + (newValue.size())) - entryStartOffset;
            newSizeInChunks = CompiledMapIterationContext.this.m().inChunks(newEntrySize);
        } else {
            newSizeInChunks = entrySizeInChunks();
        }
        if ((((pos()) + newSizeInChunks) < (CompiledMapIterationContext.this.freeList().size())) && (CompiledMapIterationContext.this.freeList().allClear(pos(), ((pos()) + newSizeInChunks)))) {
            CompiledMapIterationContext.this.freeList().set(pos(), ((pos()) + newSizeInChunks));
            CompiledMapIterationContext.this.innerWriteLock.lock();
            CompiledMapIterationContext.this.incrementSegmentEntriesIfNeeded();
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
                CompiledMapIterationContext.this.initEntryAndKeyCopying(entrySize, ((valueSizeOffset()) - entryStartOffset));
                initValue(newValue);
            } else {
                long oldValueSizeOffset = valueSizeOffset();
                long oldValueSize = valueSize();
                long oldValueOffset = valueOffset();
                CompiledMapIterationContext.this.initEntryAndKeyCopying(entrySize, ((valueOffset()) - entryStartOffset));
                initValueWithoutSize(newValue, oldValueSizeOffset, oldValueSize, oldValueOffset);
            }
            freeExtraAllocatedChunks();
        }
        CompiledMapIterationContext.this.putValueVolatile(CompiledMapIterationContext.this.hashLookupPos(), pos());
    }

    public void writeValueAndPutPos(Data<V, ?> value) {
        initValue(value);
        freeExtraAllocatedChunks();
        CompiledMapIterationContext.this.putValueVolatile(CompiledMapIterationContext.this.hashLookupPos(), pos());
    }

    protected void relocation(Data<V, ?> newValue, long newSizeOfEverythingBeforeValue) {
        CompiledMapIterationContext.this.free(pos(), entrySizeInChunks());
        long entrySize = innerEntrySize(newSizeOfEverythingBeforeValue, newValue.size());
        CompiledMapIterationContext.this.initEntryAndKeyCopying(entrySize, ((valueSizeOffset()) - (keySizeOffset())));
        writeValueAndPutPos(newValue);
    }

    public void innerDefaultReplaceValue(Data<V, ?> newValue) {
        assert CompiledMapIterationContext.this.innerUpdateLock.isHeldByCurrentThread();
        boolean newValueSizeIsDifferent = (newValue.size()) != (this.valueSize());
        if (newValueSizeIsDifferent) {
            long newSizeOfEverythingBeforeValue = newSizeOfEverythingBeforeValue(newValue);
            long entryStartOffset = keySizeOffset();
            VanillaChronicleMap<?, ?, ?, ?, ?, ?, ?> m = CompiledMapIterationContext.this.m();
            long newValueOffset = m.alignment.alignAddr((entryStartOffset + newSizeOfEverythingBeforeValue));
            long newEntrySize = (newValueOffset + (newValue.size())) - entryStartOffset;
            int newSizeInChunks = m.inChunks(newEntrySize);
            newValueDoesNotFit : if (newSizeInChunks > (entrySizeInChunks())) {
                if (newSizeInChunks > (m.maxChunksPerEntry)) {
                    throw new IllegalArgumentException(((((("Value too large: " + "entry takes ") + newSizeInChunks) + " chunks, ") + (m.maxChunksPerEntry)) + " is maximum."));
                }
                if (CompiledMapIterationContext.this.freeList().allClear(((pos()) + (entrySizeInChunks())), ((pos()) + newSizeInChunks))) {
                    CompiledMapIterationContext.this.freeList().set(((pos()) + (entrySizeInChunks())), ((pos()) + newSizeInChunks));
                    break newValueDoesNotFit;
                }
                relocation(newValue, newSizeOfEverythingBeforeValue);
                return ;
            } else if (newSizeInChunks < (entrySizeInChunks())) {
                CompiledMapIterationContext.this.freeList().clear(((pos()) + newSizeInChunks), ((pos()) + (entrySizeInChunks())));
            }
        } else {
        }
        CompiledMapIterationContext.this.innerWriteLock.lock();
        if (newValueSizeIsDifferent) {
            initValue(newValue);
        } else {
            writeValue(newValue);
        }
        CompiledMapIterationContext.this.putValueVolatile(CompiledMapIterationContext.this.hashLookupPos(), pos());
    }

    @Override
    public void doReplaceValue(Data<V, ?> newValue) {
        CompiledMapIterationContext.this.checkOnEachPublicOperation();
        try {
            CompiledMapIterationContext.this.innerDefaultReplaceValue(newValue);
        } finally {
            CompiledMapIterationContext.this.innerWriteLock.unlock();
        }
    }

    @Override
    public void doRemove() {
        CompiledMapIterationContext.this.checkOnEachPublicOperation();
        initEntryRemovedOnThisIteration(true);
        CompiledMapIterationContext.this.innerWriteLock.lock();
        try {
            if ((CompiledMapIterationContext.this.remove(CompiledMapIterationContext.this.hashLookupPos())) != (CompiledMapIterationContext.this.hashLookupPos())) {
                CompiledMapIterationContext.this.setHashLookupPosGuarded(CompiledMapIterationContext.this.stepBack(CompiledMapIterationContext.this.hashLookupPos()));
            }
            CompiledMapIterationContext.this.innerRemoveEntryExceptHashLookupUpdate();
        } finally {
            CompiledMapIterationContext.this.innerWriteLock.unlock();
        }
    }

    public boolean forEachRemoving(Predicate<? super MapEntry<K, V>> action) {
        CompiledMapIterationContext.this.innerUpdateLock.lock();
        try {
            long size = CompiledMapIterationContext.this.size();
            if (size == 0)
                return true;

            boolean interrupted = false;
            long startPos = 0L;
            while (!(CompiledMapIterationContext.this.empty(CompiledMapIterationContext.this.readEntry(startPos)))) {
                startPos = CompiledMapIterationContext.this.step(startPos);
            }
            CompiledMapIterationContext.this.initHashLookupPos(startPos);
            do {
                CompiledMapIterationContext.this.setHashLookupPosGuarded(CompiledMapIterationContext.this.step(CompiledMapIterationContext.this.hashLookupPos()));
                long entry = CompiledMapIterationContext.this.readEntry(CompiledMapIterationContext.this.hashLookupPos());
                if (!(CompiledMapIterationContext.this.empty(entry))) {
                    CompiledMapIterationContext.this.readExistingEntry(CompiledMapIterationContext.this.value(entry));
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
            } while ((CompiledMapIterationContext.this.hashLookupPos()) != startPos );
            return !interrupted;
        } finally {
            CompiledMapIterationContext.this.innerReadLock.unlock();
            initEntryRemovedOnThisIteration(false);
        }
    }
}
