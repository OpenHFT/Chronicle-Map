package net.openhft.chronicle.map.impl;

import net.openhft.chronicle.algo.hashing.LongHashFunction;
import net.openhft.chronicle.bytes.Access;
import net.openhft.chronicle.bytes.Accessor;
import net.openhft.chronicle.bytes.Accessor.Full;
import net.openhft.chronicle.bytes.ReadAccess;
import net.openhft.chronicle.hash.*;
import net.openhft.chronicle.hash.AbstractData;
import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.impl.*;
import net.openhft.chronicle.hash.impl.hashlookup.EntryConsumer;
import net.openhft.chronicle.hash.impl.value.instance.KeyInitableData;
import net.openhft.chronicle.hash.locks.InterProcessLock;
import net.openhft.chronicle.hash.locks.InterProcessReadWriteUpdateLock;
import net.openhft.chronicle.hash.replication.RemoteOperationContext;
import net.openhft.chronicle.hash.replication.TimeProvider;
import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.chronicle.hash.serialization.internal.MetaBytesInterop;
import net.openhft.chronicle.map.*;
import net.openhft.chronicle.map.TcpReplicator.TcpSocketChannelEntryWriter;
import net.openhft.chronicle.map.impl.ret.InstanceReturnValue;
import net.openhft.chronicle.map.impl.ret.UsableReturnValue;
import net.openhft.chronicle.map.impl.value.instance.ValueInitableData;
import net.openhft.chronicle.map.replication.MapRemoteQueryContext;
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
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class CompiledReplicatedMapQueryContext<K, KI, MKI extends MetaBytesInterop<K, ? super KI>, V, VI, MVI extends MetaBytesInterop<V, ? super VI>, R, T> implements AutoCloseable , HashEntry<K> , InterProcessReadWriteUpdateLock , RemoteOperationContext<K> , ExternalMapQueryContext<K, V, R> , MapAbsentEntry<K, V> , MapContext<K, V, R> , MapEntry<K, V> , MapAbsentEntryHolder<K, V> , QueryContextInterface<K, V, R> , ReplicatedChronicleMapHolder<K, KI, MKI, V, VI, MVI, R> , MapRemoteQueryContext<K, V, R> , MapReplicableEntry<K, V> {
    public void close() {
        CompiledReplicatedMapQueryContext.this.inputKeyInstanceValue.closeKey();
        CompiledReplicatedMapQueryContext.this.wrappedValueInstanceValue.closeNext();
        CompiledReplicatedMapQueryContext.this.closeReplicationInput();
        CompiledReplicatedMapQueryContext.this.wrappedValueInstanceValue.closeValue();
        CompiledReplicatedMapQueryContext.this.closePos();
        CompiledReplicatedMapQueryContext.this.bytesReturnValue.closeOutput();
        CompiledReplicatedMapQueryContext.this.closeKeySize();
        CompiledReplicatedMapQueryContext.this.closeInputKey();
        CompiledReplicatedMapQueryContext.this.closeReplicationUpdate();
        CompiledReplicatedMapQueryContext.this.closeInputBytes();
        CompiledReplicatedMapQueryContext.this.closeKeyOffset();
        CompiledReplicatedMapQueryContext.this.defaultReturnValue.closeDefaultReturnedValue();
        CompiledReplicatedMapQueryContext.this.closeUsed();
        CompiledReplicatedMapQueryContext.this.inputValueInstanceValue.closeValue();
        CompiledReplicatedMapQueryContext.this.closeAllocatedChunks();
        CompiledReplicatedMapQueryContext.this.usingReturnValue.closeUsingReturnValue();
        CompiledReplicatedMapQueryContext.this.closeReplicatedInputBytes();
        CompiledReplicatedMapQueryContext.this.closeReplicatedChronicleMapHolderImplContextAtIndexInChainDependants();
        CompiledReplicatedMapQueryContext.this.closeOwnerThreadHolderCheckAccessingFromOwnerThreadDependants();
        CompiledReplicatedMapQueryContext.this.innerWriteLock.closeWriteLockForbiddenUpgradeDependants();
        CompiledReplicatedMapQueryContext.this.closeValueBytesInteropValueMetaInteropDependants();
        CompiledReplicatedMapQueryContext.this.closeKeyBytesInteropKeyMetaInteropDependants();
        CompiledReplicatedMapQueryContext.this.closeReplicatedMapEntryStagesEntryBytesAccessOffsetDependants();
    }

    public void incrementModCountGuarded() {
        if (!(this.locksInit()))
            this.initLocks();

        incrementModCount();
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

    public void setSearchStateGuarded(SearchState newSearchState) {
        if (!(this.keySearchInit()))
            this.initKeySearch();

        setSearchState(newSearchState);
    }

    void keyFound() {
        searchState = SearchState.PRESENT;
    }

    public CompiledReplicatedMapQueryContext(ReplicatedChronicleMap<K, KI, MKI, V, VI, MVI, R> m) {
        contextChain = new ArrayList<CompiledReplicatedMapQueryContext>();
        contextChain.add(this);
        indexInContextChain = 0;
        this.m = m;
        this.usingReturnValue = new UsingReturnValue();
        this.inputValueInstanceValue = new InputValueInstanceData();
        this.defaultReturnValue = new DefaultReturnValue();
        this.innerWriteLock = new WriteLock();
        this.wrappedValueInstanceValue = new WrappedValueInstanceData();
        this.bytesReturnValue = new BytesReturnValue();
        this.entryBytes = CompiledReplicatedMapQueryContext.this.h().ms.bytes();
        this.entryBytesAccessor = JavaLangBytesAccessors.uncheckedBytesAccessor(entryBytes);
        this.entryBytesAccessHandle = ((T)(entryBytesAccessor.handle(entryBytes)));
        this.entryBytesAccess = ((Access<T>)(entryBytesAccessor.access(entryBytes)));
        this.deprecatedMapAcquireContext = new DeprecatedMapAcquireContextOnQuery();
        this.absentDelegating = new ReplicatedMapAbsentDelegating();
        this.inputKeyBytesValue = new InputKeyBytesData();
        this.entryKey = new EntryKeyBytesData();
        this.inputFirstValueBytesValue = new InputFirstValueBytesData();
        this.inputKeyInstanceValue = new InputKeyInstanceData();
        this.deprecatedMapKeyContext = new DeprecatedMapKeyContextOnQuery();
        this.copies = ThreadLocalCopies.get();
        this.valueInterop = CompiledReplicatedMapQueryContext.this.m().valueInteropProvider.get(CompiledReplicatedMapQueryContext.this.copies, CompiledReplicatedMapQueryContext.this.m().originalValueInterop);
        this.valueReader = CompiledReplicatedMapQueryContext.this.m().valueReaderProvider.get(CompiledReplicatedMapQueryContext.this.copies, CompiledReplicatedMapQueryContext.this.m().originalValueReader);
        this.keyInterop = CompiledReplicatedMapQueryContext.this.h().keyInteropProvider.get(CompiledReplicatedMapQueryContext.this.copies, CompiledReplicatedMapQueryContext.this.h().originalKeyInterop);
        this.keyReader = CompiledReplicatedMapQueryContext.this.h().keyReaderProvider.get(CompiledReplicatedMapQueryContext.this.copies, CompiledReplicatedMapQueryContext.this.h().originalKeyReader);
        this.dummyValue = new DummyValueZeroData();
        this.inputSecondValueBytesValue = new InputSecondValueBytesData();
        this.entryValue = new EntryValueBytesData();
        this.innerReadLock = new ReadLock();
        this.innerUpdateLock = new UpdateLock();
        this.owner = Thread.currentThread();
        this.replicatedInputValueBytesValue = new ReplicatedInputValueBytesData();
        this.replicatedInputKeyBytesValue = new ReplicatedInputKeyBytesData();
    }

    public CompiledReplicatedMapQueryContext(CompiledReplicatedMapQueryContext c) {
        contextChain = c.contextChain;
        indexInContextChain = contextChain.size();
        contextChain.add(this);
        this.m = ((ReplicatedChronicleMap<K, KI, MKI, V, VI, MVI, R>)(c.m));
        this.usingReturnValue = new UsingReturnValue();
        this.inputValueInstanceValue = new InputValueInstanceData();
        this.defaultReturnValue = new DefaultReturnValue();
        this.innerWriteLock = new WriteLock();
        this.wrappedValueInstanceValue = new WrappedValueInstanceData();
        this.bytesReturnValue = new BytesReturnValue();
        this.entryBytes = CompiledReplicatedMapQueryContext.this.h().ms.bytes();
        this.entryBytesAccessor = JavaLangBytesAccessors.uncheckedBytesAccessor(entryBytes);
        this.entryBytesAccessHandle = ((T)(entryBytesAccessor.handle(entryBytes)));
        this.entryBytesAccess = ((Access<T>)(entryBytesAccessor.access(entryBytes)));
        this.deprecatedMapAcquireContext = new DeprecatedMapAcquireContextOnQuery();
        this.absentDelegating = new ReplicatedMapAbsentDelegating();
        this.inputKeyBytesValue = new InputKeyBytesData();
        this.entryKey = new EntryKeyBytesData();
        this.inputFirstValueBytesValue = new InputFirstValueBytesData();
        this.inputKeyInstanceValue = new InputKeyInstanceData();
        this.deprecatedMapKeyContext = new DeprecatedMapKeyContextOnQuery();
        this.copies = ThreadLocalCopies.get();
        this.valueInterop = CompiledReplicatedMapQueryContext.this.m().valueInteropProvider.get(CompiledReplicatedMapQueryContext.this.copies, CompiledReplicatedMapQueryContext.this.m().originalValueInterop);
        this.valueReader = CompiledReplicatedMapQueryContext.this.m().valueReaderProvider.get(CompiledReplicatedMapQueryContext.this.copies, CompiledReplicatedMapQueryContext.this.m().originalValueReader);
        this.keyInterop = CompiledReplicatedMapQueryContext.this.h().keyInteropProvider.get(CompiledReplicatedMapQueryContext.this.copies, CompiledReplicatedMapQueryContext.this.h().originalKeyInterop);
        this.keyReader = CompiledReplicatedMapQueryContext.this.h().keyReaderProvider.get(CompiledReplicatedMapQueryContext.this.copies, CompiledReplicatedMapQueryContext.this.h().originalKeyReader);
        this.dummyValue = new DummyValueZeroData();
        this.inputSecondValueBytesValue = new InputSecondValueBytesData();
        this.entryValue = new EntryValueBytesData();
        this.innerReadLock = new ReadLock();
        this.innerUpdateLock = new UpdateLock();
        this.owner = Thread.currentThread();
        this.replicatedInputValueBytesValue = new ReplicatedInputValueBytesData();
        this.replicatedInputKeyBytesValue = new ReplicatedInputKeyBytesData();
    }

    public class DeprecatedMapAcquireContextOnQuery implements MapKeyContext<K, V> {
        @NotNull
        @Override
        public InterProcessLock updateLock() {
            return CompiledReplicatedMapQueryContext.this.updateLock();
        }

        @NotNull
        @Override
        public InterProcessLock writeLock() {
            return CompiledReplicatedMapQueryContext.this.writeLock();
        }

        @NotNull
        @Override
        public InterProcessLock readLock() {
            return CompiledReplicatedMapQueryContext.this.readLock();
        }

        @Override
        public long keySize() {
            CompiledReplicatedMapQueryContext.this.checkOnEachPublicOperation();
            return CompiledReplicatedMapQueryContext.this.keySize();
        }

        @Override
        public boolean valueEqualTo(V value) {
            CompiledReplicatedMapQueryContext.this.checkOnEachPublicOperation();
            return Data.bytesEquivalent(CompiledReplicatedMapQueryContext.this.entryValue, CompiledReplicatedMapQueryContext.this.wrapValueAsValue(value));
        }

        @Override
        public long valueSize() {
            CompiledReplicatedMapQueryContext.this.checkOnEachPublicOperation();
            return CompiledReplicatedMapQueryContext.this.valueSize();
        }

        @NotNull
        @Override
        public Bytes entry() {
            CompiledReplicatedMapQueryContext.this.checkOnEachPublicOperation();
            return CompiledReplicatedMapQueryContext.this.entryBytes;
        }

        @Override
        public long keyOffset() {
            CompiledReplicatedMapQueryContext.this.checkOnEachPublicOperation();
            return CompiledReplicatedMapQueryContext.this.keyOffset();
        }

        @NotNull
        @Override
        public K key() {
            CompiledReplicatedMapQueryContext.this.checkOnEachPublicOperation();
            return CompiledReplicatedMapQueryContext.this.queriedKey().get();
        }

        @Override
        public boolean put(V newValue) {
            CompiledReplicatedMapQueryContext.this.checkOnEachPublicOperation();
            updateLock().lock();
            MapEntry<K, V> entry = CompiledReplicatedMapQueryContext.this.entry();
            if (entry != null) {
                CompiledReplicatedMapQueryContext.this.replaceValue(entry, CompiledReplicatedMapQueryContext.this.wrapValueAsValue(newValue));
            } else {
                CompiledReplicatedMapQueryContext.this.insert(CompiledReplicatedMapQueryContext.this.absentEntry(), CompiledReplicatedMapQueryContext.this.wrapValueAsValue(newValue));
            }
            return true;
        }

        @Override
        public void close() {
            CompiledReplicatedMapQueryContext.this.checkOnEachPublicOperation();
            put(CompiledReplicatedMapQueryContext.this.usingReturnValue.returnValue());
            CompiledReplicatedMapQueryContext.this.close();
        }

        @Override
        public boolean remove() {
            CompiledReplicatedMapQueryContext.this.checkOnEachPublicOperation();
            updateLock().lock();
            MapEntry<K, V> entry = CompiledReplicatedMapQueryContext.this.entry();
            if (entry != null) {
                CompiledReplicatedMapQueryContext.this.remove(entry);
                return true;
            } else {
                return false;
            }
        }

        @Override
        public long valueOffset() {
            CompiledReplicatedMapQueryContext.this.checkOnEachPublicOperation();
            return CompiledReplicatedMapQueryContext.this.valueOffset();
        }

        @Override
        public boolean containsKey() {
            CompiledReplicatedMapQueryContext.this.checkOnEachPublicOperation();
            return (CompiledReplicatedMapQueryContext.this.entry()) != null;
        }

        @Override
        public V getUsing(V usingValue) {
            return containsKey() ? CompiledReplicatedMapQueryContext.this.value().getUsing(usingValue) : null;
        }

        @Override
        public V get() {
            assert containsKey();
            return CompiledReplicatedMapQueryContext.this.usingReturnValue.returnValue();
        }
    }

    public class DeprecatedMapKeyContextOnQuery implements MapKeyContext<K, V> {
        @NotNull
        @Override
        public InterProcessLock updateLock() {
            return CompiledReplicatedMapQueryContext.this.updateLock();
        }

        @NotNull
        @Override
        public InterProcessLock readLock() {
            return CompiledReplicatedMapQueryContext.this.readLock();
        }

        @NotNull
        @Override
        public InterProcessLock writeLock() {
            return CompiledReplicatedMapQueryContext.this.writeLock();
        }

        @NotNull
        @Override
        public Bytes entry() {
            CompiledReplicatedMapQueryContext.this.checkOnEachPublicOperation();
            return CompiledReplicatedMapQueryContext.this.entryBytes;
        }

        @Override
        public void close() {
            CompiledReplicatedMapQueryContext.this.checkOnEachPublicOperation();
            CompiledReplicatedMapQueryContext.this.close();
        }

        @Override
        public boolean containsKey() {
            CompiledReplicatedMapQueryContext.this.checkOnEachPublicOperation();
            return (CompiledReplicatedMapQueryContext.this.entry()) != null;
        }

        @Override
        public V getUsing(V usingValue) {
            return containsKey() ? CompiledReplicatedMapQueryContext.this.value().getUsing(usingValue) : null;
        }

        @Override
        public V get() {
            return containsKey() ? CompiledReplicatedMapQueryContext.this.value().get() : null;
        }

        @Override
        public boolean remove() {
            CompiledReplicatedMapQueryContext.this.checkOnEachPublicOperation();
            updateLock().lock();
            MapEntry<K, V> entry = CompiledReplicatedMapQueryContext.this.entry();
            if (entry != null) {
                CompiledReplicatedMapQueryContext.this.remove(entry);
                return true;
            } else {
                return false;
            }
        }

        @Override
        public long keyOffset() {
            CompiledReplicatedMapQueryContext.this.checkOnEachPublicOperation();
            return CompiledReplicatedMapQueryContext.this.keyOffset();
        }

        @Override
        public long keySize() {
            CompiledReplicatedMapQueryContext.this.checkOnEachPublicOperation();
            return CompiledReplicatedMapQueryContext.this.keySize();
        }

        @Override
        public boolean valueEqualTo(V value) {
            CompiledReplicatedMapQueryContext.this.checkOnEachPublicOperation();
            return Data.bytesEquivalent(CompiledReplicatedMapQueryContext.this.entryValue, CompiledReplicatedMapQueryContext.this.wrapValueAsValue(value));
        }

        @Override
        public boolean put(V newValue) {
            CompiledReplicatedMapQueryContext.this.checkOnEachPublicOperation();
            updateLock().lock();
            MapEntry<K, V> entry = CompiledReplicatedMapQueryContext.this.entry();
            if (entry != null) {
                CompiledReplicatedMapQueryContext.this.replaceValue(entry, CompiledReplicatedMapQueryContext.this.wrapValueAsValue(newValue));
            } else {
                CompiledReplicatedMapQueryContext.this.insert(CompiledReplicatedMapQueryContext.this.absentEntry(), CompiledReplicatedMapQueryContext.this.wrapValueAsValue(newValue));
            }
            return true;
        }

        @NotNull
        @Override
        public K key() {
            CompiledReplicatedMapQueryContext.this.checkOnEachPublicOperation();
            return CompiledReplicatedMapQueryContext.this.queriedKey().get();
        }

        @Override
        public long valueOffset() {
            CompiledReplicatedMapQueryContext.this.checkOnEachPublicOperation();
            return CompiledReplicatedMapQueryContext.this.valueOffset();
        }

        @Override
        public long valueSize() {
            CompiledReplicatedMapQueryContext.this.checkOnEachPublicOperation();
            return CompiledReplicatedMapQueryContext.this.valueSize();
        }
    }

    public class DummyValueZeroData extends AbstractData<V, Void> {
        @Override
        public long size() {
            return CompiledReplicatedMapQueryContext.this.m().valueSizeMarshaller.minEncodableSize();
        }

        @Override
        public long offset() {
            return 0;
        }

        @Override
        public Void handle() {
            return null;
        }

        @Override
        public V get() {
            throw new UnsupportedOperationException();
        }

        @Override
        public V getUsing(V usingInstance) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ReadAccess<Void> access() {
            return ReadAccess.zeros();
        }
    }

    public class EntryKeyBytesData extends AbstractData<K, T> {
        @Override
        public long offset() {
            CompiledReplicatedMapQueryContext.this.checkOnEachPublicOperation();
            return CompiledReplicatedMapQueryContext.this.entryBytesAccessOffset(CompiledReplicatedMapQueryContext.this.keyOffset());
        }

        @Override
        public T handle() {
            CompiledReplicatedMapQueryContext.this.checkOnEachPublicOperation();
            return CompiledReplicatedMapQueryContext.this.entryBytesAccessHandle;
        }

        @Override
        public ReadAccess<T> access() {
            CompiledReplicatedMapQueryContext.this.checkOnEachPublicOperation();
            return CompiledReplicatedMapQueryContext.this.entryBytesAccess;
        }

        @Override
        public long size() {
            CompiledReplicatedMapQueryContext.this.checkOnEachPublicOperation();
            return CompiledReplicatedMapQueryContext.this.keySize();
        }

        public void closeEntryKeyBytesDataSizeDependants() {
            EntryKeyBytesData.this.closeEntryKeyBytesDataInnerGetUsingDependants();
        }

        private K innerGetUsing(K usingKey) {
            CompiledReplicatedMapQueryContext.this.entryBytes.position(CompiledReplicatedMapQueryContext.this.keyOffset());
            return CompiledReplicatedMapQueryContext.this.keyReader.read(CompiledReplicatedMapQueryContext.this.entryBytes, size(), usingKey);
        }

        public void closeEntryKeyBytesDataInnerGetUsingDependants() {
            EntryKeyBytesData.this.closeCachedEntryKey();
        }

        @Override
        public K getUsing(K usingKey) {
            CompiledReplicatedMapQueryContext.this.checkOnEachPublicOperation();
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
            CompiledReplicatedMapQueryContext.this.checkOnEachPublicOperation();
            return cachedEntryKey();
        }
    }

    public class EntryValueBytesData extends AbstractData<V, T> {
        @Override
        public long size() {
            CompiledReplicatedMapQueryContext.this.checkOnEachPublicOperation();
            return CompiledReplicatedMapQueryContext.this.valueSize();
        }

        public void closeEntryValueBytesDataSizeDependants() {
            EntryValueBytesData.this.closeEntryValueBytesDataInnerGetUsingDependants();
        }

        private V innerGetUsing(V usingValue) {
            CompiledReplicatedMapQueryContext.this.entryBytes.position(CompiledReplicatedMapQueryContext.this.valueOffset());
            return CompiledReplicatedMapQueryContext.this.valueReader.read(CompiledReplicatedMapQueryContext.this.entryBytes, size(), usingValue);
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
        public ReadAccess<T> access() {
            CompiledReplicatedMapQueryContext.this.checkOnEachPublicOperation();
            return CompiledReplicatedMapQueryContext.this.entryBytesAccess;
        }

        @Override
        public V getUsing(V usingValue) {
            CompiledReplicatedMapQueryContext.this.checkOnEachPublicOperation();
            return innerGetUsing(usingValue);
        }

        @Override
        public V get() {
            CompiledReplicatedMapQueryContext.this.checkOnEachPublicOperation();
            return cachedEntryValue();
        }

        @Override
        public long offset() {
            CompiledReplicatedMapQueryContext.this.checkOnEachPublicOperation();
            return CompiledReplicatedMapQueryContext.this.entryBytesAccessOffset(CompiledReplicatedMapQueryContext.this.valueOffset());
        }

        @Override
        public T handle() {
            CompiledReplicatedMapQueryContext.this.checkOnEachPublicOperation();
            return CompiledReplicatedMapQueryContext.this.entryBytesAccessHandle;
        }
    }

    public class InputFirstValueBytesData extends AbstractData<V, T> {
        @Override
        public ReadAccess<T> access() {
            return ((ReadAccess<T>)(JavaLangBytesAccessors.uncheckedBytesAccessor(CompiledReplicatedMapQueryContext.this.inputBytes()).access(CompiledReplicatedMapQueryContext.this.inputBytes())));
        }

        @Override
        public long size() {
            return CompiledReplicatedMapQueryContext.this.firstInputValueSize();
        }

        public void closeInputFirstValueBytesDataSizeDependants() {
            InputFirstValueBytesData.this.closeInputFirstValueBytesDataGetUsingDependants();
        }

        @Override
        public V getUsing(V usingValue) {
            CompiledReplicatedMapQueryContext.this.inputBytes().position(CompiledReplicatedMapQueryContext.this.firstInputValueOffset());
            return CompiledReplicatedMapQueryContext.this.valueReader.read(CompiledReplicatedMapQueryContext.this.inputBytes(), size(), usingValue);
        }

        public void closeInputFirstValueBytesDataGetUsingDependants() {
            InputFirstValueBytesData.this.closeCachedBytesInputFirstValue();
        }

        private V cachedBytesInputFirstValue;

        private boolean cachedBytesInputFirstValueRead = false;

        public boolean cachedBytesInputFirstValueInit() {
            return (this.cachedBytesInputFirstValueRead) != false;
        }

        private void initCachedBytesInputFirstValue() {
            cachedBytesInputFirstValue = getUsing(cachedBytesInputFirstValue);
            cachedBytesInputFirstValueRead = true;
        }

        public V cachedBytesInputFirstValue() {
            if (!(this.cachedBytesInputFirstValueInit()))
                this.initCachedBytesInputFirstValue();

            return this.cachedBytesInputFirstValue;
        }

        public void closeCachedBytesInputFirstValue() {
            if (!(this.cachedBytesInputFirstValueInit()))
                return ;

            this.cachedBytesInputFirstValueRead = false;
        }

        @Override
        public V get() {
            return cachedBytesInputFirstValue();
        }

        @Override
        public long offset() {
            return JavaLangBytesAccessors.uncheckedBytesAccessor(CompiledReplicatedMapQueryContext.this.inputBytes()).offset(CompiledReplicatedMapQueryContext.this.inputBytes(), CompiledReplicatedMapQueryContext.this.firstInputValueOffset());
        }

        @Override
        public T handle() {
            return ((T)(JavaLangBytesAccessors.uncheckedBytesAccessor(CompiledReplicatedMapQueryContext.this.inputBytes()).handle(CompiledReplicatedMapQueryContext.this.inputBytes())));
        }
    }

    public class InputKeyBytesData extends AbstractData<K, T> {
        @Override
        public T handle() {
            return ((T)(JavaLangBytesAccessors.uncheckedBytesAccessor(CompiledReplicatedMapQueryContext.this.inputBytes()).handle(CompiledReplicatedMapQueryContext.this.inputBytes())));
        }

        @Override
        public long offset() {
            return JavaLangBytesAccessors.uncheckedBytesAccessor(CompiledReplicatedMapQueryContext.this.inputBytes()).offset(CompiledReplicatedMapQueryContext.this.inputBytes(), CompiledReplicatedMapQueryContext.this.inputKeyOffset());
        }

        @Override
        public long size() {
            return CompiledReplicatedMapQueryContext.this.inputKeySize();
        }

        public void closeInputKeyBytesDataSizeDependants() {
            InputKeyBytesData.this.closeInputKeyBytesDataGetUsingDependants();
        }

        @Override
        public K getUsing(K usingKey) {
            Bytes inputBytes = CompiledReplicatedMapQueryContext.this.inputBytes();
            inputBytes.position(CompiledReplicatedMapQueryContext.this.inputKeyOffset());
            return CompiledReplicatedMapQueryContext.this.keyReader.read(inputBytes, size(), usingKey);
        }

        public void closeInputKeyBytesDataGetUsingDependants() {
            InputKeyBytesData.this.closeCachedBytesInputKey();
        }

        private K cachedBytesInputKey;

        private boolean cachedBytesInputKeyRead = false;

        public boolean cachedBytesInputKeyInit() {
            return (this.cachedBytesInputKeyRead) != false;
        }

        private void initCachedBytesInputKey() {
            cachedBytesInputKey = getUsing(cachedBytesInputKey);
            cachedBytesInputKeyRead = true;
        }

        public K cachedBytesInputKey() {
            if (!(this.cachedBytesInputKeyInit()))
                this.initCachedBytesInputKey();

            return this.cachedBytesInputKey;
        }

        public void closeCachedBytesInputKey() {
            if (!(this.cachedBytesInputKeyInit()))
                return ;

            this.cachedBytesInputKeyRead = false;
        }

        @Override
        public K get() {
            return cachedBytesInputKey();
        }

        @Override
        public ReadAccess<T> access() {
            return ((ReadAccess<T>)(JavaLangBytesAccessors.uncheckedBytesAccessor(CompiledReplicatedMapQueryContext.this.inputBytes()).access(CompiledReplicatedMapQueryContext.this.inputBytes())));
        }
    }

    public class InputSecondValueBytesData extends AbstractData<V, T> {
        @Override
        public ReadAccess<T> access() {
            return ((ReadAccess<T>)(JavaLangBytesAccessors.uncheckedBytesAccessor(CompiledReplicatedMapQueryContext.this.inputBytes()).access(CompiledReplicatedMapQueryContext.this.inputBytes())));
        }

        @Override
        public T handle() {
            return ((T)(JavaLangBytesAccessors.uncheckedBytesAccessor(CompiledReplicatedMapQueryContext.this.inputBytes()).handle(CompiledReplicatedMapQueryContext.this.inputBytes())));
        }

        @Override
        public long offset() {
            return JavaLangBytesAccessors.uncheckedBytesAccessor(CompiledReplicatedMapQueryContext.this.inputBytes()).offset(CompiledReplicatedMapQueryContext.this.inputBytes(), CompiledReplicatedMapQueryContext.this.secondInputValueOffset());
        }

        @Override
        public long size() {
            return CompiledReplicatedMapQueryContext.this.secondInputValueSize();
        }

        public void closeInputSecondValueBytesDataSizeDependants() {
            InputSecondValueBytesData.this.closeInputSecondValueBytesDataGetUsingDependants();
        }

        @Override
        public V getUsing(V usingValue) {
            CompiledReplicatedMapQueryContext.this.inputBytes().position(CompiledReplicatedMapQueryContext.this.secondInputValueOffset());
            return CompiledReplicatedMapQueryContext.this.valueReader.read(CompiledReplicatedMapQueryContext.this.inputBytes(), size(), usingValue);
        }

        public void closeInputSecondValueBytesDataGetUsingDependants() {
            InputSecondValueBytesData.this.closeCachedBytesInputSecondValue();
        }

        private V cachedBytesInputSecondValue;

        private boolean cachedBytesInputSecondValueRead = false;

        public boolean cachedBytesInputSecondValueInit() {
            return (this.cachedBytesInputSecondValueRead) != false;
        }

        private void initCachedBytesInputSecondValue() {
            cachedBytesInputSecondValue = getUsing(cachedBytesInputSecondValue);
            cachedBytesInputSecondValueRead = true;
        }

        public V cachedBytesInputSecondValue() {
            if (!(this.cachedBytesInputSecondValueInit()))
                this.initCachedBytesInputSecondValue();

            return this.cachedBytesInputSecondValue;
        }

        public void closeCachedBytesInputSecondValue() {
            if (!(this.cachedBytesInputSecondValueInit()))
                return ;

            this.cachedBytesInputSecondValueRead = false;
        }

        @Override
        public V get() {
            return cachedBytesInputSecondValue();
        }
    }

    public class ReadLock implements InterProcessLock {
        @Override
        public boolean tryLock(long time, @NotNull
        TimeUnit unit) throws InterruptedException {
            if ((CompiledReplicatedMapQueryContext.this.localLockState()) == (LocalLockState.UNLOCKED)) {
                if (CompiledReplicatedMapQueryContext.this.segmentHeader().tryReadLock(CompiledReplicatedMapQueryContext.this.segmentHeaderAddress(), time, unit)) {
                    CompiledReplicatedMapQueryContext.this.setLocalLockStateGuarded(LocalLockState.READ_LOCKED);
                    return true;
                } else {
                    return false;
                }
            } else {
                return true;
            }
        }

        @Override
        public boolean tryLock() {
            if ((CompiledReplicatedMapQueryContext.this.localLockState()) == (LocalLockState.UNLOCKED)) {
                if (CompiledReplicatedMapQueryContext.this.segmentHeader().tryReadLock(CompiledReplicatedMapQueryContext.this.segmentHeaderAddress())) {
                    CompiledReplicatedMapQueryContext.this.setLocalLockStateGuarded(LocalLockState.READ_LOCKED);
                    return true;
                } else {
                    return false;
                }
            } else {
                return true;
            }
        }

        @Override
        public boolean isHeldByCurrentThread() {
            return CompiledReplicatedMapQueryContext.this.localLockState().read;
        }

        @Override
        public void unlock() {
            switch (CompiledReplicatedMapQueryContext.this.localLockState()) {
                case UNLOCKED :
                    return ;
                case READ_LOCKED :
                    CompiledReplicatedMapQueryContext.this.segmentHeader().readUnlock(CompiledReplicatedMapQueryContext.this.segmentHeaderAddress());
                    break;
                case UPDATE_LOCKED :
                    CompiledReplicatedMapQueryContext.this.segmentHeader().updateUnlock(CompiledReplicatedMapQueryContext.this.segmentHeaderAddress());
                    break;
                case WRITE_LOCKED :
                    CompiledReplicatedMapQueryContext.this.segmentHeader().writeUnlock(CompiledReplicatedMapQueryContext.this.segmentHeaderAddress());
            }
            CompiledReplicatedMapQueryContext.this.setLocalLockStateGuarded(LocalLockState.UNLOCKED);
            CompiledReplicatedMapQueryContext.this.closeHashLookupPos();
            CompiledReplicatedMapQueryContext.this.closePos();
        }

        @Override
        public void lock() {
            if ((CompiledReplicatedMapQueryContext.this.localLockState()) == (LocalLockState.UNLOCKED)) {
                CompiledReplicatedMapQueryContext.this.segmentHeader().readLock(CompiledReplicatedMapQueryContext.this.segmentHeaderAddress());
                CompiledReplicatedMapQueryContext.this.setLocalLockStateGuarded(LocalLockState.READ_LOCKED);
            }
        }

        public void closeReadLockLockDependants() {
            CompiledReplicatedMapQueryContext.this.closeHashLookupPos();
        }

        @Override
        public void lockInterruptibly() throws InterruptedException {
            if ((CompiledReplicatedMapQueryContext.this.localLockState()) == (LocalLockState.UNLOCKED)) {
                CompiledReplicatedMapQueryContext.this.segmentHeader().readLockInterruptibly(CompiledReplicatedMapQueryContext.this.segmentHeaderAddress());
                CompiledReplicatedMapQueryContext.this.setLocalLockStateGuarded(LocalLockState.READ_LOCKED);
            }
        }
    }

    public class ReplicatedInputKeyBytesData extends AbstractData<K, T> {
        @Override
        public ReadAccess<T> access() {
            return ((ReadAccess<T>)(JavaLangBytesAccessors.uncheckedBytesAccessor(CompiledReplicatedMapQueryContext.this.replicatedInputBytes()).access(CompiledReplicatedMapQueryContext.this.replicatedInputBytes())));
        }

        @Override
        public T handle() {
            return ((T)(JavaLangBytesAccessors.uncheckedBytesAccessor(CompiledReplicatedMapQueryContext.this.replicatedInputBytes()).handle(CompiledReplicatedMapQueryContext.this.replicatedInputBytes())));
        }

        @Override
        public long size() {
            return CompiledReplicatedMapQueryContext.this.riKeySize();
        }

        public void closeReplicatedInputKeyBytesDataSizeDependants() {
            ReplicatedInputKeyBytesData.this.closeReplicatedInputKeyBytesDataGetUsingDependants();
        }

        @Override
        public K getUsing(K usingKey) {
            Bytes inputBytes = CompiledReplicatedMapQueryContext.this.replicatedInputBytes();
            inputBytes.position(CompiledReplicatedMapQueryContext.this.riKeyOffset());
            return CompiledReplicatedMapQueryContext.this.keyReader.read(inputBytes, size(), usingKey);
        }

        public void closeReplicatedInputKeyBytesDataGetUsingDependants() {
            ReplicatedInputKeyBytesData.this.closeCachedBytesReplicatedInputKey();
        }

        private K cachedBytesReplicatedInputKey;

        private boolean cachedBytesReplicatedInputKeyRead = false;

        public boolean cachedBytesReplicatedInputKeyInit() {
            return (this.cachedBytesReplicatedInputKeyRead) != false;
        }

        private void initCachedBytesReplicatedInputKey() {
            cachedBytesReplicatedInputKey = getUsing(cachedBytesReplicatedInputKey);
            cachedBytesReplicatedInputKeyRead = true;
        }

        public K cachedBytesReplicatedInputKey() {
            if (!(this.cachedBytesReplicatedInputKeyInit()))
                this.initCachedBytesReplicatedInputKey();

            return this.cachedBytesReplicatedInputKey;
        }

        public void closeCachedBytesReplicatedInputKey() {
            if (!(this.cachedBytesReplicatedInputKeyInit()))
                return ;

            this.cachedBytesReplicatedInputKeyRead = false;
        }

        @Override
        public K get() {
            return cachedBytesReplicatedInputKey();
        }

        @Override
        public long offset() {
            return JavaLangBytesAccessors.uncheckedBytesAccessor(CompiledReplicatedMapQueryContext.this.replicatedInputBytes()).offset(CompiledReplicatedMapQueryContext.this.replicatedInputBytes(), CompiledReplicatedMapQueryContext.this.riKeyOffset());
        }
    }

    public class ReplicatedInputValueBytesData extends AbstractData<V, T> {
        @Override
        public ReadAccess<T> access() {
            return ((ReadAccess<T>)(JavaLangBytesAccessors.uncheckedBytesAccessor(CompiledReplicatedMapQueryContext.this.replicatedInputBytes()).access(CompiledReplicatedMapQueryContext.this.replicatedInputBytes())));
        }

        @Override
        public T handle() {
            return ((T)(JavaLangBytesAccessors.uncheckedBytesAccessor(CompiledReplicatedMapQueryContext.this.replicatedInputBytes()).handle(CompiledReplicatedMapQueryContext.this.replicatedInputBytes())));
        }

        @Override
        public long offset() {
            return JavaLangBytesAccessors.uncheckedBytesAccessor(CompiledReplicatedMapQueryContext.this.replicatedInputBytes()).offset(CompiledReplicatedMapQueryContext.this.replicatedInputBytes(), CompiledReplicatedMapQueryContext.this.riValueOffset());
        }

        @Override
        public long size() {
            return CompiledReplicatedMapQueryContext.this.riValueSize();
        }

        public void closeReplicatedInputValueBytesDataSizeDependants() {
            ReplicatedInputValueBytesData.this.closeReplicatedInputValueBytesDataGetUsingDependants();
        }

        @Override
        public V getUsing(V usingValue) {
            Bytes inputBytes = CompiledReplicatedMapQueryContext.this.replicatedInputBytes();
            inputBytes.position(CompiledReplicatedMapQueryContext.this.riValueOffset());
            return CompiledReplicatedMapQueryContext.this.valueReader.read(inputBytes, size(), usingValue);
        }

        public void closeReplicatedInputValueBytesDataGetUsingDependants() {
            ReplicatedInputValueBytesData.this.closeCachedBytesReplicatedInputValue();
        }

        private V cachedBytesReplicatedInputValue;

        private boolean cachedBytesReplicatedInputValueRead = false;

        public boolean cachedBytesReplicatedInputValueInit() {
            return (this.cachedBytesReplicatedInputValueRead) != false;
        }

        private void initCachedBytesReplicatedInputValue() {
            cachedBytesReplicatedInputValue = getUsing(cachedBytesReplicatedInputValue);
            cachedBytesReplicatedInputValueRead = true;
        }

        public V cachedBytesReplicatedInputValue() {
            if (!(this.cachedBytesReplicatedInputValueInit()))
                this.initCachedBytesReplicatedInputValue();

            return this.cachedBytesReplicatedInputValue;
        }

        public void closeCachedBytesReplicatedInputValue() {
            if (!(this.cachedBytesReplicatedInputValueInit()))
                return ;

            this.cachedBytesReplicatedInputValueRead = false;
        }

        @Override
        public V get() {
            return cachedBytesReplicatedInputValue();
        }
    }

    public class ReplicatedMapAbsentDelegating implements MapAbsentEntry<K, V> {
        @NotNull
        @Override
        public Data<K, ?> absentKey() {
            return CompiledReplicatedMapQueryContext.this.absentKey();
        }

        @NotNull
        @Override
        public Data<V, ?> defaultValue() {
            return CompiledReplicatedMapQueryContext.this.defaultValue();
        }

        @NotNull
        @Override
        public MapContext<K, V, ?> context() {
            return CompiledReplicatedMapQueryContext.this.context();
        }

        @Override
        public void doInsert(Data<V, ?> value) {
            CompiledReplicatedMapQueryContext.this.doInsert(value);
        }
    }

    public class UpdateLock implements InterProcessLock {
        @NotNull
        private IllegalMonitorStateException forbiddenUpgrade() {
            return new IllegalMonitorStateException("Cannot upgrade from read to update lock");
        }

        @Override
        public void unlock() {
            switch (CompiledReplicatedMapQueryContext.this.localLockState()) {
                case UNLOCKED :
                case READ_LOCKED :
                    return ;
                case UPDATE_LOCKED :
                    CompiledReplicatedMapQueryContext.this.segmentHeader().downgradeUpdateToReadLock(CompiledReplicatedMapQueryContext.this.segmentHeaderAddress());
                    break;
                case WRITE_LOCKED :
                    CompiledReplicatedMapQueryContext.this.segmentHeader().downgradeWriteToReadLock(CompiledReplicatedMapQueryContext.this.segmentHeaderAddress());
            }
            CompiledReplicatedMapQueryContext.this.setLocalLockStateGuarded(LocalLockState.READ_LOCKED);
        }

        @Override
        public void lockInterruptibly() throws InterruptedException {
            switch (CompiledReplicatedMapQueryContext.this.localLockState()) {
                case UNLOCKED :
                    CompiledReplicatedMapQueryContext.this.segmentHeader().updateLockInterruptibly(CompiledReplicatedMapQueryContext.this.segmentHeaderAddress());
                    CompiledReplicatedMapQueryContext.this.setLocalLockStateGuarded(LocalLockState.UPDATE_LOCKED);
                    return ;
                case READ_LOCKED :
                    throw forbiddenUpgrade();
                case UPDATE_LOCKED :
                case WRITE_LOCKED :
            }
        }

        @Override
        public boolean tryLock() {
            switch (CompiledReplicatedMapQueryContext.this.localLockState()) {
                case UNLOCKED :
                    if (CompiledReplicatedMapQueryContext.this.segmentHeader().tryUpdateLock(CompiledReplicatedMapQueryContext.this.segmentHeaderAddress())) {
                        CompiledReplicatedMapQueryContext.this.setLocalLockStateGuarded(LocalLockState.UPDATE_LOCKED);
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
        public boolean tryLock(long time, @NotNull
        TimeUnit unit) throws InterruptedException {
            switch (CompiledReplicatedMapQueryContext.this.localLockState()) {
                case UNLOCKED :
                    if (CompiledReplicatedMapQueryContext.this.segmentHeader().tryUpdateLock(CompiledReplicatedMapQueryContext.this.segmentHeaderAddress(), time, unit)) {
                        CompiledReplicatedMapQueryContext.this.setLocalLockStateGuarded(LocalLockState.UPDATE_LOCKED);
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
            switch (CompiledReplicatedMapQueryContext.this.localLockState()) {
                case UNLOCKED :
                    CompiledReplicatedMapQueryContext.this.segmentHeader().updateLock(CompiledReplicatedMapQueryContext.this.segmentHeaderAddress());
                    CompiledReplicatedMapQueryContext.this.setLocalLockStateGuarded(LocalLockState.UPDATE_LOCKED);
                    return ;
                case READ_LOCKED :
                    throw forbiddenUpgrade();
                case UPDATE_LOCKED :
                case WRITE_LOCKED :
            }
        }

        @Override
        public boolean isHeldByCurrentThread() {
            return CompiledReplicatedMapQueryContext.this.localLockState().update;
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
            CompiledReplicatedMapQueryContext.this.m().checkValue(value);
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
            MVI mvi = CompiledReplicatedMapQueryContext.this.valueMetaInterop(value());
            long size = mvi.size(CompiledReplicatedMapQueryContext.this.valueInterop, value());
            buf = CopyingInstanceData.getBuffer(this.buf, size);
            mvi.write(CompiledReplicatedMapQueryContext.this.valueInterop, buf, value());
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
        public V getUsing(V usingValue) {
            buf().position(0);
            return CompiledReplicatedMapQueryContext.this.valueReader.read(buf(), buf().limit(), usingValue);
        }

        @Override
        public DirectBytes buffer() {
            return buf();
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

        public void closeWriteLockForbiddenUpgradeDependants() {
            WriteLock.this.closeWriteLockLockDependants();
        }

        @Override
        public boolean tryLock(long time, @NotNull
        TimeUnit unit) throws InterruptedException {
            switch (CompiledReplicatedMapQueryContext.this.localLockState()) {
                case UNLOCKED :
                    if (CompiledReplicatedMapQueryContext.this.segmentHeader().tryWriteLock(CompiledReplicatedMapQueryContext.this.segmentHeaderAddress(), time, unit)) {
                        CompiledReplicatedMapQueryContext.this.setLocalLockStateGuarded(LocalLockState.WRITE_LOCKED);
                        return true;
                    } else {
                        return false;
                    }
                case READ_LOCKED :
                    throw forbiddenUpgrade();
                case UPDATE_LOCKED :
                    if (CompiledReplicatedMapQueryContext.this.segmentHeader().tryUpgradeUpdateToWriteLock(CompiledReplicatedMapQueryContext.this.segmentHeaderAddress(), time, unit)) {
                        CompiledReplicatedMapQueryContext.this.setLocalLockStateGuarded(LocalLockState.WRITE_LOCKED);
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
            switch (CompiledReplicatedMapQueryContext.this.localLockState()) {
                case UNLOCKED :
                case READ_LOCKED :
                case UPDATE_LOCKED :
                    return ;
                case WRITE_LOCKED :
                    CompiledReplicatedMapQueryContext.this.segmentHeader().downgradeWriteToUpdateLock(CompiledReplicatedMapQueryContext.this.segmentHeaderAddress());
            }
            CompiledReplicatedMapQueryContext.this.setLocalLockStateGuarded(LocalLockState.UPDATE_LOCKED);
        }

        @Override
        public boolean isHeldByCurrentThread() {
            return CompiledReplicatedMapQueryContext.this.localLockState().write;
        }

        @Override
        public void lockInterruptibly() throws InterruptedException {
            switch (CompiledReplicatedMapQueryContext.this.localLockState()) {
                case UNLOCKED :
                    CompiledReplicatedMapQueryContext.this.segmentHeader().writeLockInterruptibly(CompiledReplicatedMapQueryContext.this.segmentHeaderAddress());
                    CompiledReplicatedMapQueryContext.this.setLocalLockStateGuarded(LocalLockState.WRITE_LOCKED);
                    return ;
                case READ_LOCKED :
                    throw forbiddenUpgrade();
                case UPDATE_LOCKED :
                    CompiledReplicatedMapQueryContext.this.segmentHeader().upgradeUpdateToWriteLockInterruptibly(CompiledReplicatedMapQueryContext.this.segmentHeaderAddress());
                    CompiledReplicatedMapQueryContext.this.setLocalLockStateGuarded(LocalLockState.WRITE_LOCKED);
                case WRITE_LOCKED :
            }
        }

        @Override
        public boolean tryLock() {
            switch (CompiledReplicatedMapQueryContext.this.localLockState()) {
                case UNLOCKED :
                    if (CompiledReplicatedMapQueryContext.this.segmentHeader().tryWriteLock(CompiledReplicatedMapQueryContext.this.segmentHeaderAddress())) {
                        CompiledReplicatedMapQueryContext.this.setLocalLockStateGuarded(LocalLockState.WRITE_LOCKED);
                        return true;
                    } else {
                        return false;
                    }
                case READ_LOCKED :
                    throw forbiddenUpgrade();
                case UPDATE_LOCKED :
                    if (CompiledReplicatedMapQueryContext.this.segmentHeader().tryUpgradeUpdateToWriteLock(CompiledReplicatedMapQueryContext.this.segmentHeaderAddress())) {
                        CompiledReplicatedMapQueryContext.this.setLocalLockStateGuarded(LocalLockState.WRITE_LOCKED);
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
        public void lock() {
            switch (CompiledReplicatedMapQueryContext.this.localLockState()) {
                case UNLOCKED :
                    CompiledReplicatedMapQueryContext.this.segmentHeader().writeLock(CompiledReplicatedMapQueryContext.this.segmentHeaderAddress());
                    CompiledReplicatedMapQueryContext.this.setLocalLockStateGuarded(LocalLockState.WRITE_LOCKED);
                    return ;
                case READ_LOCKED :
                    throw forbiddenUpgrade();
                case UPDATE_LOCKED :
                    CompiledReplicatedMapQueryContext.this.segmentHeader().upgradeUpdateToWriteLock(CompiledReplicatedMapQueryContext.this.segmentHeaderAddress());
                    CompiledReplicatedMapQueryContext.this.setLocalLockStateGuarded(LocalLockState.WRITE_LOCKED);
                case WRITE_LOCKED :
            }
        }

        public void closeWriteLockLockDependants() {
            CompiledReplicatedMapQueryContext.this.closeUpdatedReplicationState();
        }
    }

    public enum SearchState {
        PRESENT, DELETED, ABSENT;    }

    public void incrementModCount() {
        contextModCount = rootContextOnThisSegment.latestSameThreadSegmentModCount = (rootContextOnThisSegment.latestSameThreadSegmentModCount) + 1;
    }

    public void setHashLookupPos(long hashLookupPos) {
        this.hashLookupPos = hashLookupPos;
    }

    public void setLocalLockState(LocalLockState newState) {
        localLockState = newState;
    }

    public void setSearchState(SearchState newSearchState) {
        this.searchState = newSearchState;
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
        this.keyMask = CompiledReplicatedMapQueryContext.mask(keyBits);
        this.valueMask = CompiledReplicatedMapQueryContext.mask(valueBits);
        this.entryMask = CompiledReplicatedMapQueryContext.mask((keyBits + valueBits));
    }

    private void unlinkFromSegmentContextsChain() {
        CompiledReplicatedMapQueryContext prevContext = this.rootContextOnThisSegment;
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

    public class BytesReturnValue implements AutoCloseable , ReturnValue<V> {
        long startOutputPos;

        TcpSocketChannelEntryWriter output = null;

        public boolean outputInit() {
            return (this.output) != null;
        }

        public void initOutput(TcpSocketChannelEntryWriter output) {
            this.output = output;
            startOutputPos = output.in().position();
        }

        public long startOutputPos() {
            assert this.outputInit() : "Output should be init";
            return this.startOutputPos;
        }

        public TcpSocketChannelEntryWriter output() {
            assert this.outputInit() : "Output should be init";
            return this.output;
        }

        public void closeOutput() {
            if (!(this.outputInit()))
                return ;

            this.output = null;
        }

        @Override
        public void returnValue(@NotNull
                                Data<V, ?> value) {
            long valueSize = value.size();
            long totalSize = (1L + (CompiledReplicatedMapQueryContext.this.m().valueSizeMarshaller.sizeEncodingSize(valueSize))) + valueSize;
            output().ensureBufferSize(totalSize);
            Bytes out = output().in();
            out.writeBoolean(false);
            CompiledReplicatedMapQueryContext.this.m().valueSizeMarshaller.writeSize(out, valueSize);
            Full<Bytes, ?> outAccessor = JavaLangBytesAccessors.uncheckedBytesAccessor(out);
            long outPosition = out.position();
            out.skip(valueSize);
            value.writeTo(outAccessor, out, outPosition);
        }

        @Override
        public void close() {
            if ((output().in().position()) == (startOutputPos())) {
                output().ensureBufferSize(1L);
                output().in().writeBoolean(true);
            }
        }
    }

    public class DefaultReturnValue implements InstanceReturnValue<V> {
        @Override
        public void returnValue(@NotNull
                                Data<V, ?> value) {
            initDefaultReturnedValue(value);
        }

        private V defaultReturnedValue = null;

        boolean defaultReturnedValueInit() {
            return (this.defaultReturnedValue) != null;
        }

        private void initDefaultReturnedValue(@NotNull
                                              Data<V, ?> value) {
            defaultReturnedValue = value.getUsing(null);
        }

        public V defaultReturnedValue() {
            assert this.defaultReturnedValueInit() : "DefaultReturnedValue should be init";
            return this.defaultReturnedValue;
        }

        public void closeDefaultReturnedValue() {
            if (!(this.defaultReturnedValueInit()))
                return ;

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

    public class InputKeyInstanceData extends CopyingInstanceData<K, T> implements KeyInitableData<K, T> {
        private K key = null;

        public boolean keyInit() {
            return (this.key) != null;
        }

        @Override
        public void initKey(K key) {
            this.key = key;
            this.closeKeyDependants();
        }

        public K key() {
            assert this.keyInit() : "Key should be init";
            return this.key;
        }

        public void closeKey() {
            if (!(this.keyInit()))
                return ;

            this.closeKeyDependants();
            this.key = null;
        }

        public void closeKeyDependants() {
            InputKeyInstanceData.this.closeBuffer();
        }

        private boolean marshalled = false;

        private DirectBytes buffer;

        public boolean bufferInit() {
            return (this.marshalled) != false;
        }

        private void initBuffer() {
            MKI mki = CompiledReplicatedMapQueryContext.this.keyMetaInterop(key());
            long size = mki.size(CompiledReplicatedMapQueryContext.this.keyInterop, key());
            buffer = CopyingInstanceData.getBuffer(this.buffer, size);
            mki.write(CompiledReplicatedMapQueryContext.this.keyInterop, buffer, key());
            buffer.flip();
            marshalled = true;
        }

        public DirectBytes buffer() {
            if (!(this.bufferInit()))
                this.initBuffer();

            return this.buffer;
        }

        public void closeBuffer() {
            if (!(this.bufferInit()))
                return ;

            this.marshalled = false;
        }

        @Override
        public K getUsing(K usingKey) {
            buffer().position(0);
            return CompiledReplicatedMapQueryContext.this.keyReader.read(buffer(), buffer().limit(), usingKey);
        }

        @Override
        public K instance() {
            return key();
        }
    }

    public class InputValueInstanceData extends CopyingInstanceData<V, T> implements ValueInitableData<V, T> {
        private V value = null;

        public boolean valueInit() {
            return (this.value) != null;
        }

        @Override
        public void initValue(V value) {
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
            this.value = null;
        }

        public void closeValueDependants() {
            InputValueInstanceData.this.closeBuffer();
        }

        private boolean marshalled = false;

        private DirectBytes buffer;

        public boolean bufferInit() {
            return (this.marshalled) != false;
        }

        private void initBuffer() {
            MVI mvi = CompiledReplicatedMapQueryContext.this.valueMetaInterop(value());
            long size = mvi.size(CompiledReplicatedMapQueryContext.this.valueInterop, value());
            buffer = CopyingInstanceData.getBuffer(this.buffer, size);
            mvi.write(CompiledReplicatedMapQueryContext.this.valueInterop, buffer, value());
            buffer.flip();
            marshalled = true;
        }

        public DirectBytes buffer() {
            if (!(this.bufferInit()))
                this.initBuffer();

            return this.buffer;
        }

        public void closeBuffer() {
            if (!(this.bufferInit()))
                return ;

            this.marshalled = false;
        }

        @Override
        public V getUsing(V usingValue) {
            buffer().position(0);
            return CompiledReplicatedMapQueryContext.this.valueReader.read(buffer(), buffer().limit(), usingValue);
        }

        @Override
        public V instance() {
            return value();
        }
    }

    public class UsingReturnValue implements UsableReturnValue<V> {
        @Override
        public void returnValue(@NotNull
                                Data<V, ?> value) {
            initReturnedValue(value);
        }

        private V usingReturnValue = ((V)(UsableReturnValue.USING_RETURN_VALUE_UNINT));

        public boolean usingReturnValueInit() {
            return (this.usingReturnValue) != ((V)(UsableReturnValue.USING_RETURN_VALUE_UNINT));
        }

        @Override
        public void initUsingReturnValue(V usingReturnValue) {
            this.usingReturnValue = usingReturnValue;
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
            this.usingReturnValue = ((V)(UsableReturnValue.USING_RETURN_VALUE_UNINT));
        }

        public void closeUsingReturnValueDependants() {
            UsingReturnValue.this.closeReturnedValue();
        }

        private V returnedValue = null;

        boolean returnedValueInit() {
            return (this.returnedValue) != null;
        }

        private void initReturnedValue(@NotNull
                                       Data<V, ?> value) {
            returnedValue = value.getUsing(usingReturnValue());
        }

        public V returnedValue() {
            assert this.returnedValueInit() : "ReturnedValue should be init";
            return this.returnedValue;
        }

        public void closeReturnedValue() {
            if (!(this.returnedValueInit()))
                return ;

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

    public static final Logger LOG = LoggerFactory.getLogger(CompiledReplicatedMapQueryContext.class);

    public final WriteLock innerWriteLock;

    public WriteLock innerWriteLock() {
        return this.innerWriteLock;
    }

    public Logger LOG() {
        return this.LOG;
    }

    public final List<CompiledReplicatedMapQueryContext> contextChain;

    public final UpdateLock innerUpdateLock;

    public UpdateLock innerUpdateLock() {
        return this.innerUpdateLock;
    }

    public List<CompiledReplicatedMapQueryContext> contextChain() {
        return this.contextChain;
    }

    final BytesReturnValue bytesReturnValue;

    public BytesReturnValue bytesReturnValue() {
        return this.bytesReturnValue;
    }

    public final ThreadLocalCopies copies;

    public ThreadLocalCopies copies() {
        return this.copies;
    }

    private void countValueOffset() {
        CompiledReplicatedMapQueryContext.this.m().alignment.alignPositionAddr(entryBytes);
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

    public final UsingReturnValue usingReturnValue;

    public UsingReturnValue usingReturnValue() {
        return this.usingReturnValue;
    }

    public final DefaultReturnValue defaultReturnValue;

    public DefaultReturnValue defaultReturnValue() {
        return this.defaultReturnValue;
    }

    public final InputKeyBytesData inputKeyBytesValue;

    public InputKeyBytesData inputKeyBytesValue() {
        return this.inputKeyBytesValue;
    }

    public final EntryValueBytesData entryValue;

    public EntryValueBytesData entryValue() {
        return this.entryValue;
    }

    final ReplicatedInputKeyBytesData replicatedInputKeyBytesValue;

    public ReplicatedInputKeyBytesData replicatedInputKeyBytesValue() {
        return this.replicatedInputKeyBytesValue;
    }

    final ReplicatedMapAbsentDelegating absentDelegating;

    final ReplicatedInputValueBytesData replicatedInputValueBytesValue;

    public ReplicatedInputValueBytesData replicatedInputValueBytesValue() {
        return this.replicatedInputValueBytesValue;
    }

    public ReplicatedMapAbsentDelegating absentDelegating() {
        return this.absentDelegating;
    }

    final WrappedValueInstanceData wrappedValueInstanceValue;

    public WrappedValueInstanceData wrappedValueInstanceValue() {
        return this.wrappedValueInstanceValue;
    }

    public final InputFirstValueBytesData inputFirstValueBytesValue;

    public InputFirstValueBytesData inputFirstValueBytesValue() {
        return this.inputFirstValueBytesValue;
    }

    public final InputSecondValueBytesData inputSecondValueBytesValue;

    public InputSecondValueBytesData inputSecondValueBytesValue() {
        return this.inputSecondValueBytesValue;
    }

    public final InputKeyInstanceData inputKeyInstanceValue;

    public InputKeyInstanceData inputKeyInstanceValue() {
        return this.inputKeyInstanceValue;
    }

    public final InputValueInstanceData inputValueInstanceValue;

    public InputValueInstanceData inputValueInstanceValue() {
        return this.inputValueInstanceValue;
    }

    public final DeprecatedMapKeyContextOnQuery deprecatedMapKeyContext;

    public DeprecatedMapKeyContextOnQuery deprecatedMapKeyContext() {
        return this.deprecatedMapKeyContext;
    }

    public final DeprecatedMapAcquireContextOnQuery deprecatedMapAcquireContext;

    public DeprecatedMapAcquireContextOnQuery deprecatedMapAcquireContext() {
        return this.deprecatedMapAcquireContext;
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

    public long entryBytesAccessOffset(long offset) {
        return entryBytesAccessor.offset(entryBytes, offset);
    }

    public void closeReplicatedMapEntryStagesEntryBytesAccessOffsetDependants() {
        CompiledReplicatedMapQueryContext.this.closeReplicatedMapQueryKeyEqualsDependants();
    }

    public MKI keyMetaInterop(K key) {
        return CompiledReplicatedMapQueryContext.this.h().metaKeyInteropProvider.get(CompiledReplicatedMapQueryContext.this.copies, CompiledReplicatedMapQueryContext.this.h().originalMetaKeyInterop, keyInterop, key);
    }

    public void closeKeyBytesInteropKeyMetaInteropDependants() {
        CompiledReplicatedMapQueryContext.this.inputKeyInstanceValue.closeBuffer();
    }

    public long innerEntrySize(long sizeOfEverythingBeforeValue, long valueSize) {
        if (CompiledReplicatedMapQueryContext.this.m().constantlySizedEntry) {
            return CompiledReplicatedMapQueryContext.this.m().alignment.alignAddr((sizeOfEverythingBeforeValue + valueSize));
        } else if (CompiledReplicatedMapQueryContext.this.m().couldNotDetermineAlignmentBeforeAllocation) {
            return (sizeOfEverythingBeforeValue + (CompiledReplicatedMapQueryContext.this.m().worstAlignment)) + valueSize;
        } else {
            return (CompiledReplicatedMapQueryContext.this.m().alignment.alignAddr(sizeOfEverythingBeforeValue)) + valueSize;
        }
    }

    private long _MapEntryStages_sizeOfEverythingBeforeValue(long keySize, long valueSize) {
        return (((CompiledReplicatedMapQueryContext.this.m().metaDataBytes) + (CompiledReplicatedMapQueryContext.this.m().keySizeMarshaller.sizeEncodingSize(keySize))) + keySize) + (CompiledReplicatedMapQueryContext.this.m().valueSizeMarshaller.sizeEncodingSize(valueSize));
    }

    long sizeOfEverythingBeforeValue(long keySize, long valueSize) {
        return (_MapEntryStages_sizeOfEverythingBeforeValue(keySize, valueSize)) + (ReplicatedChronicleMap.ADDITIONAL_ENTRY_BYTES);
    }

    public final long entrySize(long keySize, long valueSize) {
        long sizeOfEverythingBeforeValue = sizeOfEverythingBeforeValue(keySize, valueSize);
        return innerEntrySize(sizeOfEverythingBeforeValue, valueSize);
    }

    public MVI valueMetaInterop(V value) {
        return CompiledReplicatedMapQueryContext.this.m().metaValueInteropProvider.get(CompiledReplicatedMapQueryContext.this.copies, CompiledReplicatedMapQueryContext.this.m().originalMetaValueInterop, valueInterop, value);
    }

    public void closeValueBytesInteropValueMetaInteropDependants() {
        CompiledReplicatedMapQueryContext.this.inputValueInstanceValue.closeBuffer();
        CompiledReplicatedMapQueryContext.this.wrappedValueInstanceValue.closeBuffer();
    }

    @Override
    public ReplicatedChronicleMap<K, KI, MKI, V, VI, MVI, R> m() {
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
        CompiledReplicatedMapQueryContext.this.closeQueryCheckOnEachPublicOperationCheckOnEachPublicOperationDependants();
    }

    @Override
    public MapAbsentEntry<K, V> absent() {
        return CompiledReplicatedMapQueryContext.this;
    }

    public <T>T contextAtIndexInChain(int index) {
        return ((T)(contextChain.get(index)));
    }

    public void closeReplicatedChronicleMapHolderImplContextAtIndexInChainDependants() {
        CompiledReplicatedMapQueryContext.this.closeQuerySegmentStagesTryFindInitLocksOfThisSegmentDependants();
    }

    private CompiledReplicatedMapQueryContext _Chaining_createChaining() {
        return new CompiledReplicatedMapQueryContext(this);
    }

    public CompiledReplicatedMapQueryContext createChaining() {
        return new CompiledReplicatedMapQueryContext(this);
    }

    public <T>T getContext() {
        for (CompiledReplicatedMapQueryContext context : contextChain) {
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

    public Bytes replicatedInputBytes = null;

    public boolean replicatedInputBytesInit() {
        return (this.replicatedInputBytes) != null;
    }

    public void initReplicatedInputBytes(Bytes replicatedInputBytes) {
        this.replicatedInputBytes = replicatedInputBytes;
        this.closeReplicatedInputBytesDependants();
    }

    public Bytes replicatedInputBytes() {
        assert this.replicatedInputBytesInit() : "ReplicatedInputBytes should be init";
        return this.replicatedInputBytes;
    }

    public void closeReplicatedInputBytes() {
        if (!(this.replicatedInputBytesInit()))
            return ;

        this.closeReplicatedInputBytesDependants();
        this.replicatedInputBytes = null;
    }

    public void closeReplicatedInputBytesDependants() {
        CompiledReplicatedMapQueryContext.this.replicatedInputKeyBytesValue.closeReplicatedInputKeyBytesDataGetUsingDependants();
        CompiledReplicatedMapQueryContext.this.replicatedInputValueBytesValue.closeReplicatedInputValueBytesDataGetUsingDependants();
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
        CompiledReplicatedMapQueryContext.this.closeReplicatedMapEntryStagesKeyEndDependants();
        CompiledReplicatedMapQueryContext.this.closeReplicatedMapQueryKeyEqualsDependants();
        CompiledReplicatedMapQueryContext.this.entryKey.closeEntryKeyBytesDataInnerGetUsingDependants();
    }

    public Bytes inputBytes = null;

    public boolean inputBytesInit() {
        return (this.inputBytes) != null;
    }

    public void initInputBytes(Bytes inputBytes) {
        this.inputBytes = inputBytes;
        this.closeInputBytesDependants();
    }

    public Bytes inputBytes() {
        assert this.inputBytesInit() : "InputBytes should be init";
        return this.inputBytes;
    }

    public void closeInputBytes() {
        if (!(this.inputBytesInit()))
            return ;

        this.closeInputBytesDependants();
        this.inputBytes = null;
    }

    public void closeInputBytesDependants() {
        CompiledReplicatedMapQueryContext.this.closeInputKeyOffsets();
        CompiledReplicatedMapQueryContext.this.closeFirstInputValueOffsets();
        CompiledReplicatedMapQueryContext.this.inputFirstValueBytesValue.closeInputFirstValueBytesDataGetUsingDependants();
        CompiledReplicatedMapQueryContext.this.inputKeyBytesValue.closeInputKeyBytesDataGetUsingDependants();
        CompiledReplicatedMapQueryContext.this.closeSecondInputValueOffsets();
        CompiledReplicatedMapQueryContext.this.inputSecondValueBytesValue.closeInputSecondValueBytesDataGetUsingDependants();
    }

    public long inputKeySize = -1;

    public long inputKeyOffset;

    public boolean inputKeyOffsetsInit() {
        return (this.inputKeySize) >= 0;
    }

    private void initInputKeyOffsets() {
        inputKeySize = CompiledReplicatedMapQueryContext.this.h().keySizeMarshaller.readSize(inputBytes());
        inputKeyOffset = inputBytes().position();
        this.closeInputKeyOffsetsDependants();
    }

    public long inputKeyOffset() {
        if (!(this.inputKeyOffsetsInit()))
            this.initInputKeyOffsets();

        return this.inputKeyOffset;
    }

    public long inputKeySize() {
        if (!(this.inputKeyOffsetsInit()))
            this.initInputKeyOffsets();

        return this.inputKeySize;
    }

    public void closeInputKeyOffsets() {
        if (!(this.inputKeyOffsetsInit()))
            return ;

        this.closeInputKeyOffsetsDependants();
        this.inputKeySize = -1;
    }

    public void closeInputKeyOffsetsDependants() {
        CompiledReplicatedMapQueryContext.this.closeFirstInputValueOffsets();
        CompiledReplicatedMapQueryContext.this.inputKeyBytesValue.closeInputKeyBytesDataSizeDependants();
        CompiledReplicatedMapQueryContext.this.inputKeyBytesValue.closeInputKeyBytesDataGetUsingDependants();
    }

    public long firstInputValueSize = -1;

    public long firstInputValueOffset;

    public boolean firstInputValueOffsetsInit() {
        return (this.firstInputValueSize) >= 0;
    }

    private void initFirstInputValueOffsets() {
        CompiledReplicatedMapQueryContext.this.inputBytes().position(((CompiledReplicatedMapQueryContext.this.inputKeyOffset()) + (CompiledReplicatedMapQueryContext.this.inputKeySize())));
        firstInputValueSize = CompiledReplicatedMapQueryContext.this.m().valueSizeMarshaller.readSize(CompiledReplicatedMapQueryContext.this.inputBytes());
        firstInputValueOffset = CompiledReplicatedMapQueryContext.this.inputBytes().position();
        this.closeFirstInputValueOffsetsDependants();
    }

    public long firstInputValueOffset() {
        if (!(this.firstInputValueOffsetsInit()))
            this.initFirstInputValueOffsets();

        return this.firstInputValueOffset;
    }

    public long firstInputValueSize() {
        if (!(this.firstInputValueOffsetsInit()))
            this.initFirstInputValueOffsets();

        return this.firstInputValueSize;
    }

    public void closeFirstInputValueOffsets() {
        if (!(this.firstInputValueOffsetsInit()))
            return ;

        this.closeFirstInputValueOffsetsDependants();
        this.firstInputValueSize = -1;
    }

    public void closeFirstInputValueOffsetsDependants() {
        CompiledReplicatedMapQueryContext.this.closeSecondInputValueOffsets();
        CompiledReplicatedMapQueryContext.this.inputFirstValueBytesValue.closeInputFirstValueBytesDataSizeDependants();
        CompiledReplicatedMapQueryContext.this.inputFirstValueBytesValue.closeInputFirstValueBytesDataGetUsingDependants();
    }

    public long secondInputValueSize = -1;

    public long secondInputValueOffset;

    public boolean secondInputValueOffsetsInit() {
        return (this.secondInputValueSize) >= 0;
    }

    private void initSecondInputValueOffsets() {
        CompiledReplicatedMapQueryContext.this.inputBytes().position(((firstInputValueOffset()) + (firstInputValueSize())));
        secondInputValueSize = CompiledReplicatedMapQueryContext.this.m().valueSizeMarshaller.readSize(CompiledReplicatedMapQueryContext.this.inputBytes());
        secondInputValueOffset = CompiledReplicatedMapQueryContext.this.inputBytes().position();
        this.closeSecondInputValueOffsetsDependants();
    }

    public long secondInputValueOffset() {
        if (!(this.secondInputValueOffsetsInit()))
            this.initSecondInputValueOffsets();

        return this.secondInputValueOffset;
    }

    public long secondInputValueSize() {
        if (!(this.secondInputValueOffsetsInit()))
            this.initSecondInputValueOffsets();

        return this.secondInputValueSize;
    }

    public void closeSecondInputValueOffsets() {
        if (!(this.secondInputValueOffsetsInit()))
            return ;

        this.closeSecondInputValueOffsetsDependants();
        this.secondInputValueSize = -1;
    }

    public void closeSecondInputValueOffsetsDependants() {
        CompiledReplicatedMapQueryContext.this.inputSecondValueBytesValue.closeInputSecondValueBytesDataSizeDependants();
        CompiledReplicatedMapQueryContext.this.inputSecondValueBytesValue.closeInputSecondValueBytesDataGetUsingDependants();
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
        this.closeReplicationUpdateDependants();
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

        this.closeReplicationUpdateDependants();
        this.innerRemoteIdentifier = ((byte)(0));
    }

    public void closeReplicationUpdateDependants() {
        CompiledReplicatedMapQueryContext.this.closeUpdatedReplicationState();
    }

    public Data<K, ?> inputKey = null;

    public boolean inputKeyInit() {
        return (this.inputKey) != null;
    }

    public void initInputKey(Data<K, ?> inputKey) {
        this.inputKey = inputKey;
        this.closeInputKeyDependants();
    }

    public Data<K, ?> inputKey() {
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
        CompiledReplicatedMapQueryContext.this.closeHashOfKey();
        CompiledReplicatedMapQueryContext.this.closeReplicatedMapQueryKeyEqualsDependants();
    }

    public long hashOfKey = 0;

    public boolean hashOfKeyInit() {
        return (this.hashOfKey) != 0;
    }

    void initHashOfKey() {
        hashOfKey = inputKey().hash(LongHashFunction.city_1_1());
        this.closeHashOfKeyDependants();
    }

    public long hashOfKey() {
        if (!(this.hashOfKeyInit()))
            this.initHashOfKey();

        return this.hashOfKey;
    }

    public void closeHashOfKey() {
        if (!(this.hashOfKeyInit()))
            return ;

        this.closeHashOfKeyDependants();
        this.hashOfKey = 0;
    }

    public void closeHashOfKeyDependants() {
        CompiledReplicatedMapQueryContext.this.closeTheSegmentIndex();
        CompiledReplicatedMapQueryContext.this.closeSearchKey();
    }

    public int segmentIndex = -1;

    public boolean theSegmentIndexInit() {
        return (this.segmentIndex) >= 0;
    }

    void initTheSegmentIndex() {
        segmentIndex = CompiledReplicatedMapQueryContext.this.h().hashSplitting.segmentIndex(CompiledReplicatedMapQueryContext.this.hashOfKey());
        this.closeTheSegmentIndexDependants();
    }

    public void initTheSegmentIndex(int segmentIndex) {
        this.segmentIndex = segmentIndex;
        this.closeTheSegmentIndexDependants();
    }

    public int segmentIndex() {
        if (!(this.theSegmentIndexInit()))
            this.initTheSegmentIndex();

        return this.segmentIndex;
    }

    public void closeTheSegmentIndex() {
        if (!(this.theSegmentIndexInit()))
            return ;

        this.closeTheSegmentIndexDependants();
        this.segmentIndex = -1;
    }

    public void closeTheSegmentIndexDependants() {
        CompiledReplicatedMapQueryContext.this.closeSegmentHashLookup();
        CompiledReplicatedMapQueryContext.this.closeSegment();
        CompiledReplicatedMapQueryContext.this.closeSegHeader();
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
        long hashLookupOffset = CompiledReplicatedMapQueryContext.this.h().segmentOffset(CompiledReplicatedMapQueryContext.this.segmentIndex());
        innerInitSegmentHashLookup(((CompiledReplicatedMapQueryContext.this.h().ms.address()) + hashLookupOffset), CompiledReplicatedMapQueryContext.this.h().segmentHashLookupCapacity, CompiledReplicatedMapQueryContext.this.h().segmentHashLookupEntrySize, CompiledReplicatedMapQueryContext.this.h().segmentHashLookupKeyBits, CompiledReplicatedMapQueryContext.this.h().segmentHashLookupValueBits);
        this.closeSegmentHashLookupDependants();
    }

    public void initSegmentHashLookup(long address, long capacity, int entrySize, int keyBits, int valueBits) {
        innerInitSegmentHashLookup(address, capacity, entrySize, keyBits, valueBits);
        this.closeSegmentHashLookupDependants();
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

        this.closeSegmentHashLookupDependants();
        this.address = -1;
    }

    public void closeSegmentHashLookupDependants() {
        CompiledReplicatedMapQueryContext.this.closeHashLookupEmptyDependants();
        CompiledReplicatedMapQueryContext.this.closeHashLookupReadEntryDependants();
        CompiledReplicatedMapQueryContext.this.closeHashLookupKeyDependants();
        CompiledReplicatedMapQueryContext.this.closeHashLookupIndexToPosDependants();
        CompiledReplicatedMapQueryContext.this.closeHashLookupStepBackDependants();
        CompiledReplicatedMapQueryContext.this.closeHashLookupMaskUnsetKeyDependants();
        CompiledReplicatedMapQueryContext.this.closeHashLookupValueDependants();
        CompiledReplicatedMapQueryContext.this.closeHashLookupStepDependants();
        CompiledReplicatedMapQueryContext.this.closeHashLookupHlPosDependants();
    }

    public void clearHashLookup() {
        NativeBytes.UNSAFE.setMemory(address(), ((capacityMask2()) + (hashLookupEntrySize())), ((byte)(0)));
    }

    public boolean empty(long entry) {
        return (entry & (entryMask())) == (UNSET_ENTRY);
    }

    public void closeHashLookupEmptyDependants() {
        CompiledReplicatedMapQueryContext.this.closeHashLookupSearchNextPosDependants();
    }

    public long readEntry(long pos) {
        return NativeBytes.UNSAFE.getLong(((address()) + pos));
    }

    public void closeHashLookupReadEntryDependants() {
        CompiledReplicatedMapQueryContext.this.closeHashLookupSearchCheckSlotContainsExpectedKeyAndValueDependants();
        CompiledReplicatedMapQueryContext.this.closeHashLookupSearchNextPosDependants();
    }

    void clearEntry(long pos, long prevEntry) {
        long entry = prevEntry & (~(entryMask()));
        NativeBytes.UNSAFE.putLong(((address()) + pos), entry);
    }

    public void checkValueForPut(long value) {
        assert (value & (~(valueMask()))) == 0L : "Value out of range, was " + value;
    }

    public long key(long entry) {
        return entry & (keyMask());
    }

    public void closeHashLookupKeyDependants() {
        CompiledReplicatedMapQueryContext.this.closeHashLookupSearchCheckSlotContainsExpectedKeyAndValueDependants();
        CompiledReplicatedMapQueryContext.this.closeHashLookupSearchNextPosDependants();
    }

    long indexToPos(long index) {
        return index * (hashLookupEntrySize());
    }

    public void closeHashLookupIndexToPosDependants() {
        CompiledReplicatedMapQueryContext.this.closeHashLookupHlPosDependants();
    }

    public long stepBack(long pos) {
        return (pos -= hashLookupEntrySize()) >= 0 ? pos : capacityMask2();
    }

    public void closeHashLookupStepBackDependants() {
        CompiledReplicatedMapQueryContext.this.closeHashLookupSearchFoundDependants();
    }

    public long maskUnsetKey(long key) {
        return (key &= keyMask()) != (UNSET_KEY) ? key : keyMask();
    }

    public void closeHashLookupMaskUnsetKeyDependants() {
        CompiledReplicatedMapQueryContext.this.closeSearchKey();
    }

    public long value(long entry) {
        return (entry >>> (keyBits())) & (valueMask());
    }

    public void closeHashLookupValueDependants() {
        CompiledReplicatedMapQueryContext.this.closeHashLookupSearchCheckSlotContainsExpectedKeyAndValueDependants();
        CompiledReplicatedMapQueryContext.this.closeHashLookupSearchNextPosDependants();
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

    long entry(long key, long value) {
        return key | (value << (keyBits()));
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

    public long step(long pos) {
        return (pos += hashLookupEntrySize()) <= (capacityMask2()) ? pos : 0L;
    }

    public void closeHashLookupStepDependants() {
        CompiledReplicatedMapQueryContext.this.closeHashLookupSearchNextPosDependants();
    }

    void writeEntry(long pos, long prevEntry, long anotherEntry) {
        long entry = (prevEntry & (~(entryMask()))) | (anotherEntry & (entryMask()));
        NativeBytes.UNSAFE.putLong(((address()) + pos), entry);
    }

    public void writeEntry(long pos, long prevEntry, long key, long value) {
        long entry = (prevEntry & (~(entryMask()))) | (entry(key, value));
        NativeBytes.UNSAFE.putLong(((address()) + pos), entry);
    }

    public long hlPos(long key) {
        return indexToPos((key & (capacityMask())));
    }

    public void closeHashLookupHlPosDependants() {
        CompiledReplicatedMapQueryContext.this.closeSearchKey();
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

    long entrySpaceOffset = 0;

    MultiStoreBytes freeListBytes = new MultiStoreBytes();

    public SingleThreadedDirectBitSet freeList = new SingleThreadedDirectBitSet();

    boolean segmentInit() {
        return (entrySpaceOffset) > 0;
    }

    void initSegment() {
        VanillaChronicleHash<?, ?, ?, ?, ?> h = CompiledReplicatedMapQueryContext.this.h();
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
        CompiledReplicatedMapQueryContext.this.closeEntryOffset();
    }

    long segmentHeaderAddress;

    SegmentHeader segmentHeader = null;

    public boolean segHeaderInit() {
        return (this.segmentHeader) != null;
    }

    private void initSegHeader() {
        segmentHeaderAddress = (CompiledReplicatedMapQueryContext.this.h().ms.address()) + (CompiledReplicatedMapQueryContext.this.h().segmentHeaderOffset(segmentIndex()));
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
        CompiledReplicatedMapQueryContext.this.closeQuerySegmentStagesTryFindInitLocksOfThisSegmentDependants();
        CompiledReplicatedMapQueryContext.this.closeLocks();
        CompiledReplicatedMapQueryContext.this.innerReadLock.closeReadLockLockDependants();
        CompiledReplicatedMapQueryContext.this.innerWriteLock.closeWriteLockLockDependants();
    }

    public void entries(long size) {
        segmentHeader().size(segmentHeaderAddress(), size);
    }

    public void nextPosToSearchFrom(long nextPosToSearchFrom) {
        segmentHeader().nextPosToSearchFrom(segmentHeaderAddress(), nextPosToSearchFrom);
    }

    public void updateNextPosToSearchFrom(long allocated, int chunks) {
        long nextPosToSearchFrom = allocated + chunks;
        if (nextPosToSearchFrom >= (CompiledReplicatedMapQueryContext.this.h().actualChunksPerSegment))
            nextPosToSearchFrom = 0L;

        nextPosToSearchFrom(nextPosToSearchFrom);
    }

    boolean tryFindInitLocksOfThisSegment(Object thisContext, int index) {
        CompiledReplicatedMapQueryContext c = CompiledReplicatedMapQueryContext.this.contextAtIndexInChain(index);
        if ((((c.segmentHeader()) != null) && ((c.segmentHeaderAddress()) == (segmentHeaderAddress()))) && ((c.rootContextOnThisSegment()) != null)) {
            throw new IllegalStateException("Nested context not implemented yet");
        } else {
            return false;
        }
    }

    public void closeQuerySegmentStagesTryFindInitLocksOfThisSegmentDependants() {
        CompiledReplicatedMapQueryContext.this.closeLocks();
    }

    int totalReadLockCount;

    int totalUpdateLockCount;

    int totalWriteLockCount;

    public int latestSameThreadSegmentModCount;

    public int contextModCount;

    CompiledReplicatedMapQueryContext nextNode;

    public boolean concurrentSameThreadContexts;

    LocalLockState localLockState;

    public CompiledReplicatedMapQueryContext rootContextOnThisSegment = null;

    public boolean locksInit() {
        return (this.rootContextOnThisSegment) != null;
    }

    void initLocks() {
        localLockState = LocalLockState.UNLOCKED;
        int indexOfThisContext = CompiledReplicatedMapQueryContext.this.indexInContextChain;
        for (int i = indexOfThisContext - 1 ; i >= 0 ; i--) {
            if (tryFindInitLocksOfThisSegment(this, i))
                return ;

        }
        for (int i = indexOfThisContext + 1, size = CompiledReplicatedMapQueryContext.this.contextChain.size() ; i < size ; i++) {
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
        this.closeLocksDependants();
    }

    public boolean concurrentSameThreadContexts() {
        if (!(this.locksInit()))
            this.initLocks();

        return this.concurrentSameThreadContexts;
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

    public LocalLockState localLockState() {
        if (!(this.locksInit()))
            this.initLocks();

        return this.localLockState;
    }

    public CompiledReplicatedMapQueryContext rootContextOnThisSegment() {
        if (!(this.locksInit()))
            this.initLocks();

        return this.rootContextOnThisSegment;
    }

    void closeLocks() {
        if (!(this.locksInit()))
            return ;

        this.closeLocksDependants();
        if ((rootContextOnThisSegment) == this) {
            closeRootLocks();
        } else {
            closeNestedLocks();
        }
        localLockState = null;
        rootContextOnThisSegment = null;
    }

    public void closeLocksDependants() {
        CompiledReplicatedMapQueryContext.this.innerReadLock.closeReadLockLockDependants();
        CompiledReplicatedMapQueryContext.this.closeReplicatedMapQueryDropSearchIfNestedContextsAndPresentHashLookupSlotCheckFailedDependants();
        CompiledReplicatedMapQueryContext.this.innerWriteLock.closeWriteLockLockDependants();
    }

    public long deleted() {
        return segmentHeader().deleted(segmentHeaderAddress());
    }

    long nextPosToSearchFrom() {
        return segmentHeader().nextPosToSearchFrom(segmentHeaderAddress());
    }

    public long alloc(int chunks) {
        VanillaChronicleHash<?, ?, ?, ?, ?> h = CompiledReplicatedMapQueryContext.this.h();
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

    public long entries() {
        return segmentHeader().size(segmentHeaderAddress());
    }

    public long size() {
        return (entries()) - (deleted());
    }

    public void clearSegment() {
        CompiledReplicatedMapQueryContext.this.innerWriteLock.lock();
        CompiledReplicatedMapQueryContext.this.clearHashLookup();
        freeList().clear();
        nextPosToSearchFrom(0L);
        entries(0L);
    }

    public void clear() {
        clearSegment();
    }

    long searchKey = UNSET_KEY;

    long searchStartPos;

    public boolean searchKeyInit() {
        return (this.searchKey) != (UNSET_KEY);
    }

    void initSearchKey() {
        searchKey = CompiledReplicatedMapQueryContext.this.maskUnsetKey(CompiledReplicatedMapQueryContext.this.h().hashSplitting.segmentHash(CompiledReplicatedMapQueryContext.this.hashOfKey()));
        searchStartPos = CompiledReplicatedMapQueryContext.this.hlPos(searchKey);
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
        this.searchKey = UNSET_KEY;
    }

    public void closeSearchKeyDependants() {
        CompiledReplicatedMapQueryContext.this.closeHashLookupPos();
        CompiledReplicatedMapQueryContext.this.closeHashLookupSearchCheckSlotContainsExpectedKeyAndValueDependants();
        CompiledReplicatedMapQueryContext.this.closeHashLookupSearchNextPosDependants();
    }

    public long hashLookupPos = -1;

    public boolean hashLookupPosInit() {
        return (this.hashLookupPos) >= 0;
    }

    public void initHashLookupPos() {
        CompiledReplicatedMapQueryContext.this.innerReadLock.lock();
        this.hashLookupPos = CompiledReplicatedMapQueryContext.this.searchStartPos();
        this.closeHashLookupPosDependants();
    }

    public void initHashLookupPos(long hashLookupPos) {
        this.hashLookupPos = hashLookupPos;
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
        CompiledReplicatedMapQueryContext.this.closeHashLookupSearchCheckSlotContainsExpectedKeyAndValueDependants();
        CompiledReplicatedMapQueryContext.this.closeHashLookupSearchNextPosDependants();
        CompiledReplicatedMapQueryContext.this.closeHashLookupSearchFoundDependants();
    }

    void put(long value) {
        CompiledReplicatedMapQueryContext.this.checkValueForPut(value);
        CompiledReplicatedMapQueryContext.this.writeEntry(CompiledReplicatedMapQueryContext.this.hashLookupPos(), CompiledReplicatedMapQueryContext.this.readEntry(CompiledReplicatedMapQueryContext.this.hashLookupPos()), searchKey(), value);
    }

    public boolean checkSlotContainsExpectedKeyAndValue(long value) {
        long entry = CompiledReplicatedMapQueryContext.this.readEntry(CompiledReplicatedMapQueryContext.this.hashLookupPos());
        return ((CompiledReplicatedMapQueryContext.this.key(entry)) == (searchKey())) && ((CompiledReplicatedMapQueryContext.this.value(entry)) == value);
    }

    public void closeHashLookupSearchCheckSlotContainsExpectedKeyAndValueDependants() {
        CompiledReplicatedMapQueryContext.this.closeReplicatedMapQueryDropSearchIfNestedContextsAndPresentHashLookupSlotCheckFailedDependants();
    }

    public void putNewVolatile(long value) {
        CompiledReplicatedMapQueryContext.this.checkValueForPut(value);
        long currentEntry = CompiledReplicatedMapQueryContext.this.readEntry(CompiledReplicatedMapQueryContext.this.hashLookupPos());
        CompiledReplicatedMapQueryContext.this.writeEntryVolatile(CompiledReplicatedMapQueryContext.this.hashLookupPos(), currentEntry, searchKey(), value);
    }

    public void putVolatile(long value) {
        CompiledReplicatedMapQueryContext.this.checkValueForPut(value);
        long currentEntry = CompiledReplicatedMapQueryContext.this.readEntry(CompiledReplicatedMapQueryContext.this.hashLookupPos());
        assert (CompiledReplicatedMapQueryContext.this.key(currentEntry)) == (searchKey());
        CompiledReplicatedMapQueryContext.this.writeEntryVolatile(CompiledReplicatedMapQueryContext.this.hashLookupPos(), currentEntry, searchKey(), value);
    }

    public void remove() {
        CompiledReplicatedMapQueryContext.this.setHashLookupPosGuarded(CompiledReplicatedMapQueryContext.this.remove(CompiledReplicatedMapQueryContext.this.hashLookupPos()));
    }

    public boolean checkSlotIsEmpty() {
        return CompiledReplicatedMapQueryContext.this.empty(CompiledReplicatedMapQueryContext.this.readEntry(CompiledReplicatedMapQueryContext.this.hashLookupPos()));
    }

    public void found() {
        CompiledReplicatedMapQueryContext.this.setHashLookupPosGuarded(CompiledReplicatedMapQueryContext.this.stepBack(CompiledReplicatedMapQueryContext.this.hashLookupPos()));
    }

    public void closeHashLookupSearchFoundDependants() {
        CompiledReplicatedMapQueryContext.this.closeKeySearch();
    }

    public long nextPos() {
        long pos = CompiledReplicatedMapQueryContext.this.hashLookupPos();
        while (true) {
            long entry = CompiledReplicatedMapQueryContext.this.readEntry(pos);
            if (CompiledReplicatedMapQueryContext.this.empty(entry)) {
                CompiledReplicatedMapQueryContext.this.setHashLookupPosGuarded(pos);
                return -1L;
            }
            pos = CompiledReplicatedMapQueryContext.this.step(pos);
            if (pos == (searchStartPos()))
                break;

            if ((CompiledReplicatedMapQueryContext.this.key(entry)) == (searchKey())) {
                CompiledReplicatedMapQueryContext.this.setHashLookupPosGuarded(pos);
                return CompiledReplicatedMapQueryContext.this.value(entry);
            }
        }
        throw new IllegalStateException(("MultiMap is full, that most likely means you " + ("misconfigured entrySize/chunkSize, and entries tend to take less chunks than " + "expected")));
    }

    public void closeHashLookupSearchNextPosDependants() {
        CompiledReplicatedMapQueryContext.this.closeKeySearch();
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
        CompiledReplicatedMapQueryContext.this.closeReplicatedMapEntryStagesKeyEndDependants();
        CompiledReplicatedMapQueryContext.this.closeReplicatedMapQueryKeyEqualsDependants();
        CompiledReplicatedMapQueryContext.this.entryKey.closeEntryKeyBytesDataSizeDependants();
    }

    public long keyEnd() {
        return (keyOffset()) + (keySize());
    }

    public void closeReplicatedMapEntryStagesKeyEndDependants() {
        CompiledReplicatedMapQueryContext.this.closeReplicationState();
        CompiledReplicatedMapQueryContext.this.closeReplicatedMapEntryStagesCountValueSizeOffsetDependants();
        CompiledReplicatedMapQueryContext.this.closeReplicatedMapEntryStagesEntryEndDependants();
    }

    long replicationBytesOffset = -1;

    public boolean replicationStateInit() {
        return (this.replicationBytesOffset) >= 0;
    }

    void initReplicationState() {
        replicationBytesOffset = keyEnd();
        this.closeReplicationStateDependants();
    }

    public long replicationBytesOffset() {
        if (!(this.replicationStateInit()))
            this.initReplicationState();

        return this.replicationBytesOffset;
    }

    public void closeReplicationState() {
        if (!(this.replicationStateInit()))
            return ;

        this.closeReplicationStateDependants();
        this.replicationBytesOffset = -1;
    }

    public void closeReplicationStateDependants() {
        CompiledReplicatedMapQueryContext.this.closeReplicatedMapEntryStagesIdentifierOffsetDependants();
        CompiledReplicatedMapQueryContext.this.closeReplicatedMapEntryStagesUpdateReplicationStateDependants();
        CompiledReplicatedMapQueryContext.this.closeReplicatedMapEntryStagesTimestampOffsetDependants();
    }

    private long entryDeletedOffset() {
        return (replicationBytesOffset()) + 9L;
    }

    public boolean entryDeleted() {
        return entryBytesAccess.readBoolean(entryBytesAccessHandle, entryDeletedOffset());
    }

    public void writeEntryPresent() {
        entryBytesAccess.writeBoolean(entryBytesAccessHandle, entryDeletedOffset(), false);
    }

    public void writeEntryDeleted() {
        entryBytesAccess.writeBoolean(entryBytesAccessHandle, entryDeletedOffset(), true);
    }

    private long identifierOffset() {
        return (replicationBytesOffset()) + 8L;
    }

    public void closeReplicatedMapEntryStagesIdentifierOffsetDependants() {
        CompiledReplicatedMapQueryContext.this.closeReplicatedMapEntryStagesIdentifierDependants();
    }

    byte identifier() {
        return entryBytesAccess.readByte(entryBytesAccessHandle, identifierOffset());
    }

    public void closeReplicatedMapEntryStagesIdentifierDependants() {
        CompiledReplicatedMapQueryContext.this.closeUpdatedReplicationState();
    }

    void updateReplicationState(long timestamp, byte identifier) {
        entryBytes.position(replicationBytesOffset());
        entryBytes.writeLong(timestamp);
        entryBytes.writeByte(identifier);
    }

    public void closeReplicatedMapEntryStagesUpdateReplicationStateDependants() {
        CompiledReplicatedMapQueryContext.this.closeUpdatedReplicationState();
    }

    private long timestampOffset() {
        return replicationBytesOffset();
    }

    public void closeReplicatedMapEntryStagesTimestampOffsetDependants() {
        CompiledReplicatedMapQueryContext.this.closeReplicatedMapEntryStagesTimestampDependants();
    }

    public long timestamp() {
        return entryBytesAccess.readLong(entryBytesAccessHandle, timestampOffset());
    }

    public void closeReplicatedMapEntryStagesTimestampDependants() {
        CompiledReplicatedMapQueryContext.this.closeUpdatedReplicationState();
    }

    private boolean testTimeStampInSensibleRange() {
        if ((CompiledReplicatedMapQueryContext.this.m().timeProvider) == (TimeProvider.SYSTEM)) {
            long currentTime = TimeProvider.SYSTEM.currentTime();
            assert (Math.abs((currentTime - (timestamp())))) <= 100000000 : "unrealistic timestamp: " + (timestamp());
            assert (Math.abs((currentTime - (CompiledReplicatedMapQueryContext.this.innerRemoteTimestamp())))) <= 100000000 : "unrealistic innerRemoteTimestamp: " + (CompiledReplicatedMapQueryContext.this.innerRemoteTimestamp());
        }
        return true;
    }

    private boolean updatedReplicationState = false;

    public boolean updatedReplicationStateInit() {
        return (this.updatedReplicationState) != false;
    }

    public void initUpdatedReplicationStateOnAbsentEntry() {
        CompiledReplicatedMapQueryContext.this.innerWriteLock.lock();
        if (CompiledReplicatedMapQueryContext.this.replicationUpdateInit()) {
            updateReplicationState(CompiledReplicatedMapQueryContext.this.innerRemoteTimestamp(), CompiledReplicatedMapQueryContext.this.innerRemoteIdentifier());
        } else {
            updateReplicationState(CompiledReplicatedMapQueryContext.this.m().timeProvider.currentTime(), CompiledReplicatedMapQueryContext.this.m().identifier());
        }
        updatedReplicationState = true;
    }

    public void initUpdatedReplicationStateOnPresentEntry() {
        CompiledReplicatedMapQueryContext.this.innerWriteLock.lock();
        if (CompiledReplicatedMapQueryContext.this.replicationUpdateInit()) {
            updateReplicationState(CompiledReplicatedMapQueryContext.this.innerRemoteTimestamp(), CompiledReplicatedMapQueryContext.this.innerRemoteIdentifier());
        } else {
            long timestamp;
            if ((identifier()) != (CompiledReplicatedMapQueryContext.this.m().identifier())) {
                timestamp = Math.max(((timestamp()) + 1), CompiledReplicatedMapQueryContext.this.m().timeProvider.currentTime());
            } else {
                timestamp = CompiledReplicatedMapQueryContext.this.m().timeProvider.currentTime();
            }
            updateReplicationState(timestamp, CompiledReplicatedMapQueryContext.this.m().identifier());
        }
        updatedReplicationState = true;
    }

    public void closeUpdatedReplicationState() {
        if (!(this.updatedReplicationStateInit()))
            return ;

        this.updatedReplicationState = false;
    }

    private long _MapEntryStages_countValueSizeOffset() {
        return keyEnd();
    }

    long countValueSizeOffset() {
        return (_MapEntryStages_countValueSizeOffset()) + (ReplicatedChronicleMap.ADDITIONAL_ENTRY_BYTES);
    }

    public void closeReplicatedMapEntryStagesCountValueSizeOffsetDependants() {
        CompiledReplicatedMapQueryContext.this.closeValueSizeOffset();
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
        CompiledReplicatedMapQueryContext.this.closeValSize();
    }

    public long valueSize = -1;

    public long valueOffset;

    public boolean valSizeInit() {
        return (this.valueSize) >= 0;
    }

    void initValSize() {
        entryBytes.position(valueSizeOffset());
        valueSize = CompiledReplicatedMapQueryContext.this.m().readValueSize(entryBytes);
        countValueOffset();
        this.closeValSizeDependants();
    }

    void initValSize(long valueSize) {
        this.valueSize = valueSize;
        entryBytes.position(valueSizeOffset());
        CompiledReplicatedMapQueryContext.this.m().valueSizeMarshaller.writeSize(entryBytes, valueSize);
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
        CompiledReplicatedMapQueryContext.this.entryValue.closeEntryValueBytesDataSizeDependants();
        CompiledReplicatedMapQueryContext.this.entryValue.closeEntryValueBytesDataInnerGetUsingDependants();
        CompiledReplicatedMapQueryContext.this.closeReplicatedMapEntryStagesEntryEndDependants();
    }

    private long _HashEntryStages_entryEnd() {
        return keyEnd();
    }

    protected long entryEnd() {
        return (valueOffset()) + (valueSize());
    }

    public void closeReplicatedMapEntryStagesEntryEndDependants() {
        CompiledReplicatedMapQueryContext.this.closeReplicatedMapEntryStagesEntrySizeDependants();
    }

    public void writeValue(Data<?, ?> value) {
        value.writeTo(entryBytesAccess, entryBytesAccessHandle, entryBytesAccessOffset(valueOffset()));
    }

    public void initValue(Data<?, ?> value) {
        entryBytes.position(valueSizeOffset());
        initValSize(value.size());
        writeValue(value);
    }

    public void initValueWithoutSize(Data<?, ?> value, long oldValueSizeOffset, long oldValueSize, long oldValueOffset) {
        assert oldValueSize == (value.size());
        initValSizeEqualToOld(oldValueSizeOffset, oldValueSize, oldValueOffset);
        writeValue(value);
    }

    boolean keyEquals() {
        return ((inputKey().size()) == (CompiledReplicatedMapQueryContext.this.keySize())) && (inputKey().equivalent(((ReadAccess)(CompiledReplicatedMapQueryContext.this.entryBytesAccess)), CompiledReplicatedMapQueryContext.this.entryBytesAccessHandle, CompiledReplicatedMapQueryContext.this.entryBytesAccessOffset(CompiledReplicatedMapQueryContext.this.keyOffset())));
    }

    public void closeReplicatedMapQueryKeyEqualsDependants() {
        CompiledReplicatedMapQueryContext.this.closeKeySearch();
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
        CompiledReplicatedMapQueryContext.this.closeEntryOffset();
        CompiledReplicatedMapQueryContext.this.closeReplicatedMapQueryDropSearchIfNestedContextsAndPresentHashLookupSlotCheckFailedDependants();
    }

    public void raiseChange() {
        CompiledReplicatedMapQueryContext.this.m().raiseChange(CompiledReplicatedMapQueryContext.this.segmentIndex(), CompiledReplicatedMapQueryContext.this.pos());
    }

    public long keySizeOffset = -1;

    public boolean entryOffsetInit() {
        return (this.keySizeOffset) >= 0;
    }

    public void initEntryOffset() {
        keySizeOffset = (CompiledReplicatedMapQueryContext.this.entrySpaceOffset()) + ((pos()) * (CompiledReplicatedMapQueryContext.this.h().chunkSize));
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
        CompiledReplicatedMapQueryContext.this.closeReplicatedMapEntryStagesReadExistingEntryDependants();
        CompiledReplicatedMapQueryContext.this.closeReplicatedMapEntryStagesEntrySizeDependants();
    }

    public void readExistingEntry(long pos) {
        initPos(pos);
        entryBytes.position(keySizeOffset());
        initKeySize(CompiledReplicatedMapQueryContext.this.h().keySizeMarshaller.readSize(entryBytes));
        initKeyOffset(entryBytes.position());
    }

    public void closeReplicatedMapEntryStagesReadExistingEntryDependants() {
        CompiledReplicatedMapQueryContext.this.closeKeySearch();
    }

    protected SearchState searchState = null;

    boolean keySearchInit() {
        return (this.searchState) != null;
    }

    void initKeySearch() {
        for (long pos ; (pos = CompiledReplicatedMapQueryContext.this.nextPos()) >= 0L ; ) {
            CompiledReplicatedMapQueryContext.this.readExistingEntry(pos);
            if (!(keyEquals()))
                continue;

            CompiledReplicatedMapQueryContext.this.found();
            keyFound();
            return ;
        }
        searchState = SearchState.ABSENT;
        this.closeKeySearchDependants();
    }

    public SearchState searchState() {
        if (!(this.keySearchInit()))
            this.initKeySearch();

        return this.searchState;
    }

    void closeKeySearch() {
        if (!(this.keySearchInit()))
            return ;

        this.closeKeySearchDependants();
        this.searchState = null;
    }

    public void closeKeySearchDependants() {
        CompiledReplicatedMapQueryContext.this.closeReplicatedMapQueryDropSearchIfNestedContextsAndPresentHashLookupSlotCheckFailedDependants();
    }

    public boolean searchStatePresent() {
        return (searchState()) == (SearchState.PRESENT);
    }

    private boolean _MapQuery_entryPresent() {
        return searchStatePresent();
    }

    public boolean entryPresent() {
        return (_MapQuery_entryPresent()) && (!(CompiledReplicatedMapQueryContext.this.entryDeleted()));
    }

    public boolean searchStateDeleted() {
        return (((searchState()) == (SearchState.DELETED)) && (!(CompiledReplicatedMapQueryContext.this.concurrentSameThreadContexts()))) && (CompiledReplicatedMapQueryContext.this.innerUpdateLock.isHeldByCurrentThread());
    }

    public boolean searchStateAbsent() {
        return (!(searchStatePresent())) && (!(searchStateDeleted()));
    }

    private void _AllocatedChunks_incrementSegmentEntriesIfNeeded() {
    }

    public void incrementSegmentEntriesIfNeeded() {
        if ((CompiledReplicatedMapQueryContext.this.searchState()) != (SearchState.PRESENT)) {
            CompiledReplicatedMapQueryContext.this.entries(((CompiledReplicatedMapQueryContext.this.entries()) + 1L));
        }
    }

    public void writeNewEntry(long pos, Data<?, ?> key) {
        initPos(pos);
        initKeySize(key.size());
        entryBytes.position(keySizeOffset());
        CompiledReplicatedMapQueryContext.this.h().keySizeMarshaller.writeSize(entryBytes, keySize());
        initKeyOffset(entryBytes.position());
        key.writeTo(entryBytesAccessor, entryBytes, keyOffset());
    }

    public void initEntryAndKey(long entrySize) {
        initAllocatedChunks(CompiledReplicatedMapQueryContext.this.h().inChunks(entrySize));
        CompiledReplicatedMapQueryContext.this.writeNewEntry(CompiledReplicatedMapQueryContext.this.alloc(allocatedChunks()), CompiledReplicatedMapQueryContext.this.inputKey());
        incrementSegmentEntriesIfNeeded();
    }

    long entrySize() {
        return (entryEnd()) - (keySizeOffset());
    }

    public void closeReplicatedMapEntryStagesEntrySizeDependants() {
        CompiledReplicatedMapQueryContext.this.closeTheEntrySizeInChunks();
    }

    public int entrySizeInChunks = 0;

    public boolean theEntrySizeInChunksInit() {
        return (this.entrySizeInChunks) != 0;
    }

    void initTheEntrySizeInChunks() {
        entrySizeInChunks = CompiledReplicatedMapQueryContext.this.h().inChunks(entrySize());
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

    public final void freeExtraAllocatedChunks() {
        if (((!(CompiledReplicatedMapQueryContext.this.m().constantlySizedEntry)) && (CompiledReplicatedMapQueryContext.this.m().couldNotDetermineAlignmentBeforeAllocation)) && ((entrySizeInChunks()) < (CompiledReplicatedMapQueryContext.this.allocatedChunks()))) {
            CompiledReplicatedMapQueryContext.this.free(((pos()) + (entrySizeInChunks())), ((CompiledReplicatedMapQueryContext.this.allocatedChunks()) - (entrySizeInChunks())));
        } else {
            initTheEntrySizeInChunks(CompiledReplicatedMapQueryContext.this.allocatedChunks());
        }
    }

    public long newSizeOfEverythingBeforeValue(Data<V, ?> newValue) {
        return ((valueSizeOffset()) + (CompiledReplicatedMapQueryContext.this.m().valueSizeMarshaller.sizeEncodingSize(newValue.size()))) - (keySizeOffset());
    }

    public void copyExistingEntry(long newPos, long bytesToCopy) {
        long oldKeySizeOffset = keySizeOffset();
        long oldKeyOffset = keyOffset();
        initPos(newPos);
        initKeyOffset(((keySizeOffset()) + (oldKeyOffset - oldKeySizeOffset)));
        Access.copy(entryBytesAccess, entryBytesAccessHandle, entryBytesAccessOffset(oldKeySizeOffset), entryBytesAccess, entryBytesAccessHandle, entryBytesAccessOffset(keySizeOffset()), bytesToCopy);
    }

    public void initEntryAndKeyCopying(long entrySize, long bytesToCopy) {
        initAllocatedChunks(CompiledReplicatedMapQueryContext.this.h().inChunks(entrySize));
        CompiledReplicatedMapQueryContext.this.copyExistingEntry(CompiledReplicatedMapQueryContext.this.alloc(allocatedChunks()), bytesToCopy);
        incrementSegmentEntriesIfNeeded();
    }

    private void _MapEntryStages_putValueDeletedEntry(Data<V, ?> newValue) {
        assert CompiledReplicatedMapQueryContext.this.innerUpdateLock.isHeldByCurrentThread();
        int newSizeInChunks;
        long entryStartOffset = keySizeOffset();
        long newSizeOfEverythingBeforeValue = -1;
        boolean newValueSizeIsDifferent = (newValue.size()) != (valueSize());
        if (newValueSizeIsDifferent) {
            newSizeOfEverythingBeforeValue = newSizeOfEverythingBeforeValue(newValue);
            long newValueOffset = CompiledReplicatedMapQueryContext.this.m().alignment.alignAddr((entryStartOffset + newSizeOfEverythingBeforeValue));
            long newEntrySize = (newValueOffset + (newValue.size())) - entryStartOffset;
            newSizeInChunks = CompiledReplicatedMapQueryContext.this.m().inChunks(newEntrySize);
        } else {
            newSizeInChunks = entrySizeInChunks();
        }
        if ((((pos()) + newSizeInChunks) < (CompiledReplicatedMapQueryContext.this.freeList().size())) && (CompiledReplicatedMapQueryContext.this.freeList().allClear(pos(), ((pos()) + newSizeInChunks)))) {
            CompiledReplicatedMapQueryContext.this.freeList().set(pos(), ((pos()) + newSizeInChunks));
            CompiledReplicatedMapQueryContext.this.innerWriteLock.lock();
            CompiledReplicatedMapQueryContext.this.incrementSegmentEntriesIfNeeded();
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
                CompiledReplicatedMapQueryContext.this.initEntryAndKeyCopying(entrySize, ((valueSizeOffset()) - entryStartOffset));
                initValue(newValue);
            } else {
                long oldValueSizeOffset = valueSizeOffset();
                long oldValueSize = valueSize();
                long oldValueOffset = valueOffset();
                CompiledReplicatedMapQueryContext.this.initEntryAndKeyCopying(entrySize, ((valueOffset()) - entryStartOffset));
                initValueWithoutSize(newValue, oldValueSizeOffset, oldValueSize, oldValueOffset);
            }
            freeExtraAllocatedChunks();
        }
        CompiledReplicatedMapQueryContext.this.putValueVolatile(CompiledReplicatedMapQueryContext.this.hashLookupPos(), pos());
    }

    public void putValueDeletedEntry(Data<V, ?> newValue) {
        throw new AssertionError("Replicated Map doesn\'t remove entries truly, yet");
    }

    void putEntry(Data<V, ?> value) {
        assert CompiledReplicatedMapQueryContext.this.searchStateAbsent();
        long entrySize = CompiledReplicatedMapQueryContext.this.entrySize(CompiledReplicatedMapQueryContext.this.inputKey().size(), value.size());
        CompiledReplicatedMapQueryContext.this.initEntryAndKey(entrySize);
        CompiledReplicatedMapQueryContext.this.initValue(value);
        CompiledReplicatedMapQueryContext.this.freeExtraAllocatedChunks();
        CompiledReplicatedMapQueryContext.this.putNewVolatile(CompiledReplicatedMapQueryContext.this.pos());
    }

    public void writeValueAndPutPos(Data<V, ?> value) {
        initValue(value);
        freeExtraAllocatedChunks();
        CompiledReplicatedMapQueryContext.this.putValueVolatile(CompiledReplicatedMapQueryContext.this.hashLookupPos(), pos());
    }

    public void innerRemoveEntryExceptHashLookupUpdate() {
        CompiledReplicatedMapQueryContext.this.free(pos(), entrySizeInChunks());
        CompiledReplicatedMapQueryContext.this.entries(((CompiledReplicatedMapQueryContext.this.entries()) - 1L));
        CompiledReplicatedMapQueryContext.this.incrementModCountGuarded();
    }

    public void dropSearchIfNestedContextsAndPresentHashLookupSlotCheckFailed() {
        if (CompiledReplicatedMapQueryContext.this.locksInit()) {
            if ((CompiledReplicatedMapQueryContext.this.concurrentSameThreadContexts()) && ((CompiledReplicatedMapQueryContext.this.rootContextOnThisSegment().latestSameThreadSegmentModCount()) != (CompiledReplicatedMapQueryContext.this.contextModCount()))) {
                if (keySearchInit()) {
                    if ((searchState()) == (SearchState.PRESENT)) {
                        if (!(CompiledReplicatedMapQueryContext.this.checkSlotContainsExpectedKeyAndValue(CompiledReplicatedMapQueryContext.this.pos())))
                            CompiledReplicatedMapQueryContext.this.closeHashLookupPos();

                    }
                }
            }
        }
    }

    public void closeReplicatedMapQueryDropSearchIfNestedContextsAndPresentHashLookupSlotCheckFailedDependants() {
        CompiledReplicatedMapQueryContext.this.closeQueryCheckOnEachPublicOperationCheckOnEachPublicOperationDependants();
    }

    private void _CheckOnEachPublicOperation_checkOnEachPublicOperation() {
        CompiledReplicatedMapQueryContext.this.checkAccessingFromOwnerThread();
    }

    public void checkOnEachPublicOperation() {
        _CheckOnEachPublicOperation_checkOnEachPublicOperation();
        CompiledReplicatedMapQueryContext.this.dropSearchIfNestedContextsAndPresentHashLookupSlotCheckFailed();
    }

    public void closeQueryCheckOnEachPublicOperationCheckOnEachPublicOperationDependants() {
        CompiledReplicatedMapQueryContext.this.entryValue.closeEntryValueBytesDataSizeDependants();
        CompiledReplicatedMapQueryContext.this.entryKey.closeEntryKeyBytesDataSizeDependants();
    }

    public Data<K, ?> queriedKey() {
        CompiledReplicatedMapQueryContext.this.checkOnEachPublicOperation();
        return inputKey();
    }

    @Override
    public byte remoteIdentifier() {
        CompiledReplicatedMapQueryContext.this.checkOnEachPublicOperation();
        return innerRemoteIdentifier();
    }

    @NotNull
    @Override
    public Data<K, ?> absentKey() {
        CompiledReplicatedMapQueryContext.this.checkOnEachPublicOperation();
        return CompiledReplicatedMapQueryContext.this.inputKey();
    }

    @NotNull
    @Override
    public Data<K, ?> key() {
        CompiledReplicatedMapQueryContext.this.checkOnEachPublicOperation();
        return CompiledReplicatedMapQueryContext.this.entryKey;
    }

    protected void putPrefix() {
        CompiledReplicatedMapQueryContext.this.checkOnEachPublicOperation();
        boolean underUpdatedLockIsHeld = !(CompiledReplicatedMapQueryContext.this.innerUpdateLock.isHeldByCurrentThread());
        if (underUpdatedLockIsHeld)
            CompiledReplicatedMapQueryContext.this.innerUpdateLock.lock();

        boolean searchResultsNotTrusted = underUpdatedLockIsHeld || (CompiledReplicatedMapQueryContext.this.concurrentSameThreadContexts());
        if (((CompiledReplicatedMapQueryContext.this.hashLookupPosInit()) && (searchStateAbsent())) && searchResultsNotTrusted)
            CompiledReplicatedMapQueryContext.this.closeHashLookupPos();

    }

    @Override
    public R insert(@NotNull
                    MapAbsentEntry<K, V> absentEntry, Data<V, ?> value) {
        CompiledReplicatedMapQueryContext.this.checkOnEachPublicOperation();
        return CompiledReplicatedMapQueryContext.this.m().entryOperations.insert(absentEntry, value);
    }

    @Override
    public long originTimestamp() {
        CompiledReplicatedMapQueryContext.this.checkOnEachPublicOperation();
        return timestamp();
    }

    @Override
    public Data<V, ?> wrapValueAsValue(V value) {
        CompiledReplicatedMapQueryContext.this.checkOnEachPublicOperation();
        WrappedValueInstanceData wrapped = CompiledReplicatedMapQueryContext.this.wrappedValueInstanceValue;
        wrapped = wrapped.getUnusedWrappedValueGuarded();
        wrapped.initValue(value);
        return wrapped;
    }

    @NotNull
    @Override
    public Data<V, ?> defaultValue() {
        CompiledReplicatedMapQueryContext.this.checkOnEachPublicOperation();
        return CompiledReplicatedMapQueryContext.this.wrapValueAsValue(CompiledReplicatedMapQueryContext.this.m().defaultValue(CompiledReplicatedMapQueryContext.this.deprecatedMapKeyContext));
    }

    @Override
    public R remove(@NotNull
                    MapEntry<K, V> entry) {
        CompiledReplicatedMapQueryContext.this.checkOnEachPublicOperation();
        return CompiledReplicatedMapQueryContext.this.m().entryOperations.remove(entry);
    }

    @NotNull
    @Override
    public InterProcessLock writeLock() {
        CompiledReplicatedMapQueryContext.this.checkOnEachPublicOperation();
        return CompiledReplicatedMapQueryContext.this.innerWriteLock;
    }

    @Override
    public long remoteTimestamp() {
        CompiledReplicatedMapQueryContext.this.checkOnEachPublicOperation();
        return innerRemoteTimestamp();
    }

    private MapEntry<K, V> _MapQuery_entry() {
        CompiledReplicatedMapQueryContext.this.checkOnEachPublicOperation();
        return entryPresent() ? this : null;
    }

    @Override
    public MapReplicableEntry<K, V> entry() {
        return ((MapReplicableEntry<K, V>)(_MapQuery_entry()));
    }

    @NotNull
    @Override
    public InterProcessLock updateLock() {
        CompiledReplicatedMapQueryContext.this.checkOnEachPublicOperation();
        return CompiledReplicatedMapQueryContext.this.innerUpdateLock;
    }

    @Nullable
    private MapAbsentEntry<K, V> _MapQuery_absentEntry() {
        CompiledReplicatedMapQueryContext.this.checkOnEachPublicOperation();
        return entryPresent() ? null : CompiledReplicatedMapQueryContext.this.absent();
    }

    @Nullable
    @Override
    public MapAbsentEntry<K, V> absentEntry() {
        CompiledReplicatedMapQueryContext.this.checkOnEachPublicOperation();
        if (entryPresent()) {
            return null;
        } else {
            if (!(searchStatePresent())) {
                return CompiledReplicatedMapQueryContext.this.absentDelegating;
            } else {
                assert CompiledReplicatedMapQueryContext.this.entryDeleted();
                return CompiledReplicatedMapQueryContext.this.absent();
            }
        }
    }

    @Override
    public byte originIdentifier() {
        CompiledReplicatedMapQueryContext.this.checkOnEachPublicOperation();
        return identifier();
    }

    @NotNull
    @Override
    public MapContext<K, V, ?> context() {
        CompiledReplicatedMapQueryContext.this.checkOnEachPublicOperation();
        return this;
    }

    @NotNull
    @Override
    public Data<V, ?> value() {
        CompiledReplicatedMapQueryContext.this.checkOnEachPublicOperation();
        return CompiledReplicatedMapQueryContext.this.entryValue;
    }

    @Override
    public R replaceValue(@NotNull
                          MapEntry<K, V> entry, Data<V, ?> newValue) {
        CompiledReplicatedMapQueryContext.this.checkOnEachPublicOperation();
        return CompiledReplicatedMapQueryContext.this.m().entryOperations.replaceValue(entry, newValue);
    }

    @NotNull
    @Override
    public InterProcessLock readLock() {
        CompiledReplicatedMapQueryContext.this.checkOnEachPublicOperation();
        return CompiledReplicatedMapQueryContext.this.innerReadLock;
    }

    @Override
    public Data<V, ?> defaultValue(@NotNull
                                                              MapAbsentEntry<K, V> absentEntry) {
        CompiledReplicatedMapQueryContext.this.checkOnEachPublicOperation();
        return CompiledReplicatedMapQueryContext.this.m().entryOperations.defaultValue(absentEntry);
    }

    public void dropChange() {
        CompiledReplicatedMapQueryContext.this.m().dropChange(CompiledReplicatedMapQueryContext.this.segmentIndex(), CompiledReplicatedMapQueryContext.this.pos());
    }

    private void _MapEntryStages_relocation(Data<V, ?> newValue, long newSizeOfEverythingBeforeValue) {
        CompiledReplicatedMapQueryContext.this.free(pos(), entrySizeInChunks());
        long entrySize = innerEntrySize(newSizeOfEverythingBeforeValue, newValue.size());
        CompiledReplicatedMapQueryContext.this.initEntryAndKeyCopying(entrySize, ((valueSizeOffset()) - (keySizeOffset())));
        writeValueAndPutPos(newValue);
    }

    protected void relocation(Data<V, ?> newValue, long newSizeOfEverythingBeforeValue) {
        CompiledReplicatedMapQueryContext.this.dropChange();
        _MapEntryStages_relocation(newValue, newSizeOfEverythingBeforeValue);
    }

    public void innerDefaultReplaceValue(Data<V, ?> newValue) {
        assert CompiledReplicatedMapQueryContext.this.innerUpdateLock.isHeldByCurrentThread();
        boolean newValueSizeIsDifferent = (newValue.size()) != (this.valueSize());
        if (newValueSizeIsDifferent) {
            long newSizeOfEverythingBeforeValue = newSizeOfEverythingBeforeValue(newValue);
            long entryStartOffset = keySizeOffset();
            VanillaChronicleMap<?, ?, ?, ?, ?, ?, ?> m = CompiledReplicatedMapQueryContext.this.m();
            long newValueOffset = m.alignment.alignAddr((entryStartOffset + newSizeOfEverythingBeforeValue));
            long newEntrySize = (newValueOffset + (newValue.size())) - entryStartOffset;
            int newSizeInChunks = m.inChunks(newEntrySize);
            newValueDoesNotFit : if (newSizeInChunks > (entrySizeInChunks())) {
                if (newSizeInChunks > (m.maxChunksPerEntry)) {
                    throw new IllegalArgumentException(((((("Value too large: " + "entry takes ") + newSizeInChunks) + " chunks, ") + (m.maxChunksPerEntry)) + " is maximum."));
                }
                if (CompiledReplicatedMapQueryContext.this.freeList().allClear(((pos()) + (entrySizeInChunks())), ((pos()) + newSizeInChunks))) {
                    CompiledReplicatedMapQueryContext.this.freeList().set(((pos()) + (entrySizeInChunks())), ((pos()) + newSizeInChunks));
                    break newValueDoesNotFit;
                }
                relocation(newValue, newSizeOfEverythingBeforeValue);
                return ;
            } else if (newSizeInChunks < (entrySizeInChunks())) {
                CompiledReplicatedMapQueryContext.this.freeList().clear(((pos()) + newSizeInChunks), ((pos()) + (entrySizeInChunks())));
            }
        } else {
        }
        CompiledReplicatedMapQueryContext.this.innerWriteLock.lock();
        if (newValueSizeIsDifferent) {
            initValue(newValue);
        } else {
            writeValue(newValue);
        }
        CompiledReplicatedMapQueryContext.this.putValueVolatile(CompiledReplicatedMapQueryContext.this.hashLookupPos(), pos());
    }

    public void updateChange() {
        if (replicationUpdateInit()) {
            dropChange();
        } else {
            raiseChange();
        }
    }

    private void _MapAbsent_doInsert(Data<V, ?> value) {
        CompiledReplicatedMapQueryContext.this.putPrefix();
        if (!(CompiledReplicatedMapQueryContext.this.searchStatePresent())) {
            if (CompiledReplicatedMapQueryContext.this.searchStateDeleted()) {
                CompiledReplicatedMapQueryContext.this.putValueDeletedEntry(value);
            } else {
                putEntry(value);
            }
            CompiledReplicatedMapQueryContext.this.incrementModCountGuarded();
            CompiledReplicatedMapQueryContext.this.setSearchStateGuarded(SearchState.PRESENT);
        } else {
            throw new IllegalStateException("Entry is present in the map when doInsert() is called");
        }
    }

    @Override
    public void doInsert(Data<V, ?> value) {
        CompiledReplicatedMapQueryContext.this.putPrefix();
        if (!(CompiledReplicatedMapQueryContext.this.entryPresent())) {
            if (!(CompiledReplicatedMapQueryContext.this.searchStatePresent())) {
                putEntry(value);
                CompiledReplicatedMapQueryContext.this.setSearchStateGuarded(SearchState.PRESENT);
            } else {
                CompiledReplicatedMapQueryContext.this.innerDefaultReplaceValue(value);
                CompiledReplicatedMapQueryContext.this.deleted(((CompiledReplicatedMapQueryContext.this.deleted()) - 1));
            }
            CompiledReplicatedMapQueryContext.this.incrementModCountGuarded();
            CompiledReplicatedMapQueryContext.this.writeEntryPresent();
            CompiledReplicatedMapQueryContext.this.updateChange();
            CompiledReplicatedMapQueryContext.this.initUpdatedReplicationStateOnAbsentEntry();
        } else {
            throw new IllegalStateException("Entry is absent in the map when doInsert() is called");
        }
    }

    private void _MapQuery_doRemove() {
        CompiledReplicatedMapQueryContext.this.checkOnEachPublicOperation();
        CompiledReplicatedMapQueryContext.this.innerUpdateLock.lock();
        if (searchStatePresent()) {
            CompiledReplicatedMapQueryContext.this.innerWriteLock.lock();
            CompiledReplicatedMapQueryContext.this.remove();
            CompiledReplicatedMapQueryContext.this.innerRemoveEntryExceptHashLookupUpdate();
            setSearchStateGuarded(SearchState.DELETED);
        } else {
            throw new IllegalStateException("Entry is absent when doRemove() is called");
        }
    }

    @Override
    public void doRemove() {
        CompiledReplicatedMapQueryContext.this.checkOnEachPublicOperation();
        CompiledReplicatedMapQueryContext.this.innerUpdateLock.lock();
        if (entryPresent()) {
            if ((CompiledReplicatedMapQueryContext.this.valueSize()) > (CompiledReplicatedMapQueryContext.this.dummyValue.size()))
                CompiledReplicatedMapQueryContext.this.innerDefaultReplaceValue(CompiledReplicatedMapQueryContext.this.dummyValue);

            CompiledReplicatedMapQueryContext.this.initUpdatedReplicationStateOnPresentEntry();
            CompiledReplicatedMapQueryContext.this.writeEntryDeleted();
            CompiledReplicatedMapQueryContext.this.updateChange();
            CompiledReplicatedMapQueryContext.this.deleted(((CompiledReplicatedMapQueryContext.this.deleted()) + 1));
        } else {
            throw new IllegalStateException("Entry is absent in the map when doRemove() is called");
        }
    }

    private void _MapQuery_doReplaceValue(Data<V, ?> newValue) {
        putPrefix();
        if (searchStatePresent()) {
            CompiledReplicatedMapQueryContext.this.innerDefaultReplaceValue(newValue);
            CompiledReplicatedMapQueryContext.this.incrementModCountGuarded();
            setSearchStateGuarded(SearchState.PRESENT);
        } else {
            throw new IllegalStateException("Entry is absent in the map when doReplaceValue() is called");
        }
    }

    @Override
    public void doReplaceValue(Data<V, ?> newValue) {
        _MapQuery_doReplaceValue(newValue);
        CompiledReplicatedMapQueryContext.this.updateChange();
        CompiledReplicatedMapQueryContext.this.initUpdatedReplicationStateOnPresentEntry();
    }

    public long riKeySize = -1;

    public long riValueSize;

    public long riKeyOffset;

    public long riValueOffset;

    public long riTimestamp;

    public byte riId;

    public boolean isDeleted;

    public boolean replicationInputInit() {
        return (this.riKeySize) >= 0;
    }

    public void initReplicationInput(Bytes replicatedInputBytes) {
        initReplicatedInputBytes(replicatedInputBytes);
        riKeySize = CompiledReplicatedMapQueryContext.this.m().keySizeMarshaller.readSize(replicatedInputBytes);
        riValueSize = CompiledReplicatedMapQueryContext.this.m().valueSizeMarshaller.readSize(replicatedInputBytes);
        riTimestamp = replicatedInputBytes.readStopBit();
        riId = replicatedInputBytes.readByte();
        CompiledReplicatedMapQueryContext.this.initReplicationUpdate(riTimestamp, riId);
        isDeleted = replicatedInputBytes.readBoolean();
        riKeyOffset = replicatedInputBytes.position();
        riValueOffset = (riKeyOffset) + (riKeySize);
        this.closeReplicationInputDependants();
    }

    public boolean isDeleted() {
        assert this.replicationInputInit() : "ReplicationInput should be init";
        return this.isDeleted;
    }

    public byte riId() {
        assert this.replicationInputInit() : "ReplicationInput should be init";
        return this.riId;
    }

    public long riKeyOffset() {
        assert this.replicationInputInit() : "ReplicationInput should be init";
        return this.riKeyOffset;
    }

    public long riKeySize() {
        assert this.replicationInputInit() : "ReplicationInput should be init";
        return this.riKeySize;
    }

    public long riTimestamp() {
        assert this.replicationInputInit() : "ReplicationInput should be init";
        return this.riTimestamp;
    }

    public long riValueOffset() {
        assert this.replicationInputInit() : "ReplicationInput should be init";
        return this.riValueOffset;
    }

    public long riValueSize() {
        assert this.replicationInputInit() : "ReplicationInput should be init";
        return this.riValueSize;
    }

    public void closeReplicationInput() {
        if (!(this.replicationInputInit()))
            return ;

        this.closeReplicationInputDependants();
        this.riKeySize = -1;
    }

    public void closeReplicationInputDependants() {
        CompiledReplicatedMapQueryContext.this.replicatedInputKeyBytesValue.closeReplicatedInputKeyBytesDataSizeDependants();
        CompiledReplicatedMapQueryContext.this.replicatedInputKeyBytesValue.closeReplicatedInputKeyBytesDataGetUsingDependants();
        CompiledReplicatedMapQueryContext.this.replicatedInputValueBytesValue.closeReplicatedInputValueBytesDataSizeDependants();
        CompiledReplicatedMapQueryContext.this.replicatedInputValueBytesValue.closeReplicatedInputValueBytesDataGetUsingDependants();
    }

    public void processReplicatedEvent() {
        if ((riId()) == (CompiledReplicatedMapQueryContext.this.m().identifier())) {
            return ;
        }
        CompiledReplicatedMapQueryContext.this.initInputKey(CompiledReplicatedMapQueryContext.this.replicatedInputKeyBytesValue);
        boolean debugEnabled = CompiledReplicatedMapQueryContext.this.LOG.isDebugEnabled();
        CompiledReplicatedMapQueryContext.this.innerUpdateLock.lock();
        if (isDeleted()) {
            if (debugEnabled) {
                CompiledReplicatedMapQueryContext.this.LOG.debug("READING FROM SOURCE -  into local-id={}, remote={}, remove(key={})", CompiledReplicatedMapQueryContext.this.m().identifier(), riId(), CompiledReplicatedMapQueryContext.this.inputKey());
            }
            if ((CompiledReplicatedMapQueryContext.this.m().remoteOperations.remove(this)) == (AcceptanceDecision.ACCEPT)) {
                CompiledReplicatedMapQueryContext.this.absent().doInsert(CompiledReplicatedMapQueryContext.this.dummyValue);
                CompiledReplicatedMapQueryContext.this.doRemove();
                if (!(CompiledReplicatedMapQueryContext.this.updatedReplicationStateInit()))
                    CompiledReplicatedMapQueryContext.this.initUpdatedReplicationStateOnPresentEntry();

            }
            CompiledReplicatedMapQueryContext.this.m().setLastModificationTime(riId(), riTimestamp());
            return ;
        }
        String message = null;
        if (debugEnabled) {
            message = String.format("READING FROM SOURCE -  into local-id=%d, remote-id=%d, put(key=%s,", CompiledReplicatedMapQueryContext.this.m().identifier(), riId(), CompiledReplicatedMapQueryContext.this.inputKey());
        }
        if ((CompiledReplicatedMapQueryContext.this.m().remoteOperations.put(this, CompiledReplicatedMapQueryContext.this.replicatedInputValueBytesValue)) == (AcceptanceDecision.ACCEPT)) {
            if (!(CompiledReplicatedMapQueryContext.this.updatedReplicationStateInit()))
                CompiledReplicatedMapQueryContext.this.initUpdatedReplicationStateOnPresentEntry();

        }
        CompiledReplicatedMapQueryContext.this.m().setLastModificationTime(riId(), riTimestamp());
        if (debugEnabled) {
            CompiledReplicatedMapQueryContext.this.LOG.debug((((message + "value=") + (CompiledReplicatedMapQueryContext.this.replicatedInputValueBytesValue)) + ")"));
        }
    }
}
