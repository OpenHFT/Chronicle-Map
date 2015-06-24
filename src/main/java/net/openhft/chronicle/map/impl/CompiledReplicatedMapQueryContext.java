package net.openhft.chronicle.map.impl;

import net.openhft.chronicle.bytes.*;
import net.openhft.chronicle.hash.AbstractData;
import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.HashEntry;
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
import net.openhft.chronicle.map.impl.data.instance.ValueInitableData;
import net.openhft.chronicle.map.impl.ret.InstanceReturnValue;
import net.openhft.chronicle.map.impl.ret.UsableReturnValue;
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

public class CompiledReplicatedMapQueryContext<K, KI, MKI extends net.openhft.chronicle.hash.serialization.internal.MetaBytesInterop<K, ? super KI>, V, VI, MVI extends MetaBytesInterop<V, ? super VI>, R, T> implements AutoCloseable , HashEntry<K> , InterProcessReadWriteUpdateLock , RemoteOperationContext<K> , ExternalMapQueryContext<K, V, R> , MapAbsentEntry<K, V> , MapContext<K, V, R> , MapEntry<K, V> , MapAbsentEntryHolder<K, V> , QueryContextInterface<K, V, R> , ReplicatedChronicleMapHolder<K, KI, MKI, V, VI, MVI, R> , MapRemoteQueryContext<K, V, R> , MapReplicableEntry<K, V> {
    public void close() {
        this.bytesReturnValue.closeOutput();
        this.wrappedValueInstanceValue.closeValue();
        this.inputValueInstanceValue.closeValue();
        this.closeInputBytes();
        this.closeReplicationInput();
        this.closeUsed();
        this.closeAllocatedChunks();
        this.closeReplicationUpdate();
        this.wrappedValueInstanceValue.closeNext();
        this.usingReturnValue.closeUsingReturnValue();
        this.defaultReturnValue.closeDefaultReturnedValue();
        this.closeKeySize();
        this.inputKeyInstanceValue.closeKey();
        this.closeReplicatedInputBytes();
        this.closePos();
        this.closeInputKey();
        this.closeKeyOffset();
        this.closeKeyBytesInteropKeyMetaInteropDependants();
        this.closeReplicatedChronicleMapHolderImplContextAtIndexInChainDependants();
        this.closeOwnerThreadHolderCheckAccessingFromOwnerThreadDependants();
        this.closeValueBytesInteropValueMetaInteropDependants();
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

    public void setSearchStateGuarded(CompiledReplicatedMapQueryContext.SearchState newSearchState) {
        if (!(this.keySearchInit()))
            this.initKeySearch();

        setSearchState(newSearchState);
    }

    private long _MapEntryStages_countValueSizeOffset() {
        return keyEnd();
    }

    private long _MapEntryStages_sizeOfEverythingBeforeValue(long keySize, long valueSize) {
        return (((this.m().metaDataBytes) + (this.m().keySizeMarshaller.sizeEncodingSize(keySize))) + keySize) + (this.m().valueSizeMarshaller.sizeEncodingSize(valueSize));
    }

    void keyFound() {
        searchState = CompiledReplicatedMapQueryContext.SearchState.PRESENT;
    }

    public CompiledReplicatedMapQueryContext(ReplicatedChronicleMap<K, KI, MKI, V, VI, MVI, R> m) {
        contextChain = new ArrayList<net.openhft.chronicle.map.impl.CompiledReplicatedMapQueryContext>();
        contextChain.add(this);
        indexInContextChain = 0;
        this.m = m;
        this.inputKeyBytesValue = new InputKeyBytesData();
        this.defaultReturnValue = new DefaultReturnValue();
        this.inputValueInstanceValue = new InputValueInstanceData();
        this.copies = ThreadLocalCopies.get();
        this.valueInterop = this.m().valueInteropProvider.get(this.copies, this.m().originalValueInterop);
        this.valueReader = this.m().valueReaderProvider.get(this.copies, this.m().originalValueReader);
        this.keyInterop = this.h().keyInteropProvider.get(this.copies, this.h().originalKeyInterop);
        this.keyReader = this.h().keyReaderProvider.get(this.copies, this.h().originalKeyReader);
        this.absentDelegating = new ReplicatedMapAbsentDelegating();
        this.deprecatedMapAcquireContext = new DeprecatedMapAcquireContextOnQuery();
        this.usingReturnValue = new UsingReturnValue();
        this.entryKey = new EntryKeyBytesData();
        this.innerUpdateLock = new UpdateLock();
        this.innerReadLock = new ReadLock();
        this.inputStore = new JavaLangBytesReusableBytesStore();
        this.deprecatedMapKeyContext = new DeprecatedMapKeyContextOnQuery();
        this.replicatedInputKeyBytesValue = new ReplicatedInputKeyBytesData();
        this.entryValue = new EntryValueBytesData();
        this.innerWriteLock = new WriteLock();
        this.bytesReturnValue = new BytesReturnValue();
        this.entryBytes = this.h().ms.bytes();
        this.entryBS = new NativeBytesStore<java.lang.Object>(entryBytes.address() , entryBytes.capacity() , null , false);
        this.owner = Thread.currentThread();
        this.replicatedInputStore = new JavaLangBytesReusableBytesStore();
        this.dummyValue = new DummyValueZeroData();
        this.inputFirstValueBytesValue = new InputFirstValueBytesData();
        this.inputKeyInstanceValue = new InputKeyInstanceData();
        this.replicatedInputValueBytesValue = new ReplicatedInputValueBytesData();
        this.wrappedValueInstanceValue = new WrappedValueInstanceData();
        this.inputSecondValueBytesValue = new InputSecondValueBytesData();
    }

    public CompiledReplicatedMapQueryContext(CompiledReplicatedMapQueryContext c) {
        contextChain = c.contextChain;
        indexInContextChain = contextChain.size();
        contextChain.add(this);
        this.m = ((ReplicatedChronicleMap<K, KI, MKI, V, VI, MVI, R>)(c.m));
        this.inputKeyBytesValue = new InputKeyBytesData();
        this.defaultReturnValue = new DefaultReturnValue();
        this.inputValueInstanceValue = new InputValueInstanceData();
        this.copies = ThreadLocalCopies.get();
        this.valueInterop = this.m().valueInteropProvider.get(this.copies, this.m().originalValueInterop);
        this.valueReader = this.m().valueReaderProvider.get(this.copies, this.m().originalValueReader);
        this.keyInterop = this.h().keyInteropProvider.get(this.copies, this.h().originalKeyInterop);
        this.keyReader = this.h().keyReaderProvider.get(this.copies, this.h().originalKeyReader);
        this.absentDelegating = new ReplicatedMapAbsentDelegating();
        this.deprecatedMapAcquireContext = new DeprecatedMapAcquireContextOnQuery();
        this.usingReturnValue = new UsingReturnValue();
        this.entryKey = new EntryKeyBytesData();
        this.innerUpdateLock = new UpdateLock();
        this.innerReadLock = new ReadLock();
        this.inputStore = new JavaLangBytesReusableBytesStore();
        this.deprecatedMapKeyContext = new DeprecatedMapKeyContextOnQuery();
        this.replicatedInputKeyBytesValue = new ReplicatedInputKeyBytesData();
        this.entryValue = new EntryValueBytesData();
        this.innerWriteLock = new WriteLock();
        this.bytesReturnValue = new BytesReturnValue();
        this.entryBytes = this.h().ms.bytes();
        this.entryBS = new NativeBytesStore<java.lang.Object>(entryBytes.address() , entryBytes.capacity() , null , false);
        this.owner = Thread.currentThread();
        this.replicatedInputStore = new JavaLangBytesReusableBytesStore();
        this.dummyValue = new DummyValueZeroData();
        this.inputFirstValueBytesValue = new InputFirstValueBytesData();
        this.inputKeyInstanceValue = new InputKeyInstanceData();
        this.replicatedInputValueBytesValue = new ReplicatedInputValueBytesData();
        this.wrappedValueInstanceValue = new WrappedValueInstanceData();
        this.inputSecondValueBytesValue = new InputSecondValueBytesData();
    }

    public class BytesReturnValue implements AutoCloseable , ReturnValue<V> {
        public BytesReturnValue() {
            this.outputStore = new JavaLangBytesReusableBytesStore();
        }

        private final JavaLangBytesReusableBytesStore outputStore;

        public JavaLangBytesReusableBytesStore outputStore() {
            return this.outputStore;
        }

        long startOutputPos;

        TcpReplicator.TcpSocketChannelEntryWriter output = null;

        public boolean outputInit() {
            return (this.output) != null;
        }

        public void initOutput(TcpReplicator.TcpSocketChannelEntryWriter output) {
            this.output = output;
            startOutputPos = output.in().position();
        }

        public long startOutputPos() {
            assert this.outputInit() : "Output should be init";
            return this.startOutputPos;
        }

        public TcpReplicator.TcpSocketChannelEntryWriter output() {
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
                                Data<V> value) {
            long valueSize = value.size();
            long totalSize = (1L + (CompiledReplicatedMapQueryContext.this.m().valueSizeMarshaller.sizeEncodingSize(valueSize))) + valueSize;
            output().ensureBufferSize(totalSize);
            Bytes out = output().in();
            out.writeBoolean(false);
            CompiledReplicatedMapQueryContext.this.m().valueSizeMarshaller.writeSize(out, valueSize);
            long outPosition = out.position();
            out.skip(valueSize);
            outputStore.setBytes(out);
            value.writeTo(outputStore, outPosition);
        }

        @Override
        public void close() {
            if ((output().in().position()) == (startOutputPos())) {
                output().ensureBufferSize(1L);
                output().in().writeBoolean(true);
            }
        }
    }

    public class DeprecatedMapAcquireContextOnQuery implements MapKeyContext<K, V> {
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

        @NotNull
        @Override
        public InterProcessLock updateLock() {
            return CompiledReplicatedMapQueryContext.this.updateLock();
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
        public long keyOffset() {
            CompiledReplicatedMapQueryContext.this.checkOnEachPublicOperation();
            return CompiledReplicatedMapQueryContext.this.keyOffset();
        }

        @NotNull
        @Override
        public Bytes entry() {
            CompiledReplicatedMapQueryContext.this.checkOnEachPublicOperation();
            return CompiledReplicatedMapQueryContext.this.entryBytes;
        }

        @Override
        public long keySize() {
            CompiledReplicatedMapQueryContext.this.checkOnEachPublicOperation();
            return CompiledReplicatedMapQueryContext.this.keySize();
        }

        @Override
        public long valueSize() {
            CompiledReplicatedMapQueryContext.this.checkOnEachPublicOperation();
            return CompiledReplicatedMapQueryContext.this.valueSize();
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

        @Override
        public void close() {
            CompiledReplicatedMapQueryContext.this.checkOnEachPublicOperation();
            put(CompiledReplicatedMapQueryContext.this.usingReturnValue.returnValue());
            CompiledReplicatedMapQueryContext.this.close();
        }
    }

    public class DeprecatedMapKeyContextOnQuery implements MapKeyContext<K, V> {
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

        @NotNull
        @Override
        public InterProcessLock updateLock() {
            return CompiledReplicatedMapQueryContext.this.updateLock();
        }

        @Override
        public long valueSize() {
            CompiledReplicatedMapQueryContext.this.checkOnEachPublicOperation();
            return CompiledReplicatedMapQueryContext.this.valueSize();
        }

        @Override
        public void close() {
            CompiledReplicatedMapQueryContext.this.checkOnEachPublicOperation();
            CompiledReplicatedMapQueryContext.this.close();
        }

        @NotNull
        @Override
        public K key() {
            CompiledReplicatedMapQueryContext.this.checkOnEachPublicOperation();
            return CompiledReplicatedMapQueryContext.this.queriedKey().get();
        }

        @Override
        public boolean valueEqualTo(V value) {
            CompiledReplicatedMapQueryContext.this.checkOnEachPublicOperation();
            return Data.bytesEquivalent(CompiledReplicatedMapQueryContext.this.entryValue, CompiledReplicatedMapQueryContext.this.wrapValueAsValue(value));
        }

        @Override
        public long valueOffset() {
            CompiledReplicatedMapQueryContext.this.checkOnEachPublicOperation();
            return CompiledReplicatedMapQueryContext.this.valueOffset();
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
        public long keySize() {
            CompiledReplicatedMapQueryContext.this.checkOnEachPublicOperation();
            return CompiledReplicatedMapQueryContext.this.keySize();
        }
    }

    public class DummyValueZeroData extends AbstractData<V> {
        @Override
        public long size() {
            return CompiledReplicatedMapQueryContext.this.m().valueSizeMarshaller.minEncodableSize();
        }

        @Override
        public RandomDataInput bytes() {
            return ZeroRandomDataInput.INSTANCE;
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
        public long offset() {
            return 0;
        }
    }

    public class EntryKeyBytesData extends AbstractData<K> {
        @Override
        public long offset() {
            CompiledReplicatedMapQueryContext.this.checkOnEachPublicOperation();
            return CompiledReplicatedMapQueryContext.this.keyOffset();
        }

        @Override
        public RandomDataInput bytes() {
            CompiledReplicatedMapQueryContext.this.checkOnEachPublicOperation();
            return CompiledReplicatedMapQueryContext.this.entryBS;
        }

        @Override
        public long size() {
            CompiledReplicatedMapQueryContext.this.checkOnEachPublicOperation();
            return CompiledReplicatedMapQueryContext.this.keySize();
        }

        public void closeEntryKeyBytesDataSizeDependants() {
            this.closeEntryKeyBytesDataInnerGetUsingDependants();
        }

        private K innerGetUsing(K usingKey) {
            CompiledReplicatedMapQueryContext.this.entryBytes.position(CompiledReplicatedMapQueryContext.this.keyOffset());
            return CompiledReplicatedMapQueryContext.this.keyReader.read(CompiledReplicatedMapQueryContext.this.entryBytes, size(), usingKey);
        }

        public void closeEntryKeyBytesDataInnerGetUsingDependants() {
            this.closeCachedEntryKey();
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

        @Override
        public K getUsing(K usingKey) {
            CompiledReplicatedMapQueryContext.this.checkOnEachPublicOperation();
            return innerGetUsing(usingKey);
        }
    }

    public class EntryValueBytesData extends AbstractData<V> {
        @Override
        public long offset() {
            CompiledReplicatedMapQueryContext.this.checkOnEachPublicOperation();
            return CompiledReplicatedMapQueryContext.this.valueOffset();
        }

        @Override
        public long size() {
            CompiledReplicatedMapQueryContext.this.checkOnEachPublicOperation();
            return CompiledReplicatedMapQueryContext.this.valueSize();
        }

        public void closeEntryValueBytesDataSizeDependants() {
            this.closeEntryValueBytesDataInnerGetUsingDependants();
        }

        private V innerGetUsing(V usingValue) {
            CompiledReplicatedMapQueryContext.this.entryBytes.position(CompiledReplicatedMapQueryContext.this.valueOffset());
            return CompiledReplicatedMapQueryContext.this.valueReader.read(CompiledReplicatedMapQueryContext.this.entryBytes, size(), usingValue);
        }

        public void closeEntryValueBytesDataInnerGetUsingDependants() {
            this.closeCachedEntryValue();
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
            CompiledReplicatedMapQueryContext.this.checkOnEachPublicOperation();
            return cachedEntryValue();
        }

        @Override
        public V getUsing(V usingValue) {
            CompiledReplicatedMapQueryContext.this.checkOnEachPublicOperation();
            return innerGetUsing(usingValue);
        }

        @Override
        public RandomDataInput bytes() {
            CompiledReplicatedMapQueryContext.this.checkOnEachPublicOperation();
            return CompiledReplicatedMapQueryContext.this.entryBS;
        }
    }

    public class InputFirstValueBytesData extends AbstractData<V> {
        @Override
        public RandomDataInput bytes() {
            return CompiledReplicatedMapQueryContext.this.inputStore;
        }

        @Override
        public long offset() {
            return CompiledReplicatedMapQueryContext.this.firstInputValueOffset();
        }

        @Override
        public long size() {
            return CompiledReplicatedMapQueryContext.this.firstInputValueSize();
        }

        public void closeInputFirstValueBytesDataSizeDependants() {
            this.closeInputFirstValueBytesDataGetUsingDependants();
        }

        @Override
        public V getUsing(V usingValue) {
            CompiledReplicatedMapQueryContext.this.inputBytes().position(CompiledReplicatedMapQueryContext.this.firstInputValueOffset());
            return CompiledReplicatedMapQueryContext.this.valueReader.read(CompiledReplicatedMapQueryContext.this.inputBytes(), size(), usingValue);
        }

        public void closeInputFirstValueBytesDataGetUsingDependants() {
            this.closeCachedBytesInputFirstValue();
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
    }

    public class InputKeyBytesData extends AbstractData<K> {
        @Override
        public RandomDataInput bytes() {
            return CompiledReplicatedMapQueryContext.this.inputStore;
        }

        @Override
        public long offset() {
            return CompiledReplicatedMapQueryContext.this.inputKeyOffset();
        }

        @Override
        public long size() {
            return CompiledReplicatedMapQueryContext.this.inputKeySize();
        }

        public void closeInputKeyBytesDataSizeDependants() {
            this.closeInputKeyBytesDataGetUsingDependants();
        }

        @Override
        public K getUsing(K usingKey) {
            Bytes inputBytes = CompiledReplicatedMapQueryContext.this.inputBytes();
            inputBytes.position(CompiledReplicatedMapQueryContext.this.inputKeyOffset());
            return CompiledReplicatedMapQueryContext.this.keyReader.read(inputBytes, size(), usingKey);
        }

        public void closeInputKeyBytesDataGetUsingDependants() {
            this.closeCachedBytesInputKey();
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
    }

    public class InputSecondValueBytesData extends AbstractData<V> {
        @Override
        public RandomDataInput bytes() {
            return CompiledReplicatedMapQueryContext.this.inputStore;
        }

        @Override
        public long offset() {
            return CompiledReplicatedMapQueryContext.this.secondInputValueOffset();
        }

        @Override
        public long size() {
            return CompiledReplicatedMapQueryContext.this.secondInputValueSize();
        }

        public void closeInputSecondValueBytesDataSizeDependants() {
            this.closeInputSecondValueBytesDataGetUsingDependants();
        }

        @Override
        public V getUsing(V usingValue) {
            CompiledReplicatedMapQueryContext.this.inputBytes().position(CompiledReplicatedMapQueryContext.this.secondInputValueOffset());
            return CompiledReplicatedMapQueryContext.this.valueReader.read(CompiledReplicatedMapQueryContext.this.inputBytes(), size(), usingValue);
        }

        public void closeInputSecondValueBytesDataGetUsingDependants() {
            this.closeCachedBytesInputSecondValue();
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
        public void lockInterruptibly() throws InterruptedException {
            if ((CompiledReplicatedMapQueryContext.this.localLockState()) == (LocalLockState.UNLOCKED)) {
                CompiledReplicatedMapQueryContext.this.segmentHeader().readLockInterruptibly(CompiledReplicatedMapQueryContext.this.segmentHeaderAddress());
                CompiledReplicatedMapQueryContext.this.setLocalLockStateGuarded(LocalLockState.READ_LOCKED);
            }
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
    }

    public class ReplicatedInputKeyBytesData extends AbstractData<K> {
        @Override
        public RandomDataInput bytes() {
            return CompiledReplicatedMapQueryContext.this.replicatedInputStore;
        }

        @Override
        public long offset() {
            return CompiledReplicatedMapQueryContext.this.riKeyOffset();
        }

        @Override
        public long size() {
            return CompiledReplicatedMapQueryContext.this.riKeySize();
        }

        public void closeReplicatedInputKeyBytesDataSizeDependants() {
            this.closeReplicatedInputKeyBytesDataGetUsingDependants();
        }

        @Override
        public K getUsing(K usingKey) {
            Bytes inputBytes = CompiledReplicatedMapQueryContext.this.replicatedInputBytes();
            inputBytes.position(CompiledReplicatedMapQueryContext.this.riKeyOffset());
            return CompiledReplicatedMapQueryContext.this.keyReader.read(inputBytes, size(), usingKey);
        }

        public void closeReplicatedInputKeyBytesDataGetUsingDependants() {
            this.closeCachedBytesReplicatedInputKey();
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
    }

    public class ReplicatedInputValueBytesData extends AbstractData<V> {
        @Override
        public RandomDataInput bytes() {
            return CompiledReplicatedMapQueryContext.this.replicatedInputStore;
        }

        @Override
        public long size() {
            return CompiledReplicatedMapQueryContext.this.riValueSize();
        }

        public void closeReplicatedInputValueBytesDataSizeDependants() {
            this.closeReplicatedInputValueBytesDataGetUsingDependants();
        }

        @Override
        public V getUsing(V usingValue) {
            Bytes inputBytes = CompiledReplicatedMapQueryContext.this.replicatedInputBytes();
            inputBytes.position(CompiledReplicatedMapQueryContext.this.riValueOffset());
            return CompiledReplicatedMapQueryContext.this.valueReader.read(inputBytes, size(), usingValue);
        }

        public void closeReplicatedInputValueBytesDataGetUsingDependants() {
            this.closeCachedBytesReplicatedInputValue();
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

        @Override
        public long offset() {
            return CompiledReplicatedMapQueryContext.this.riValueOffset();
        }
    }

    public class ReplicatedMapAbsentDelegating implements MapAbsentEntry<K, V> {
        @NotNull
        @Override
        public Data<K> absentKey() {
            return CompiledReplicatedMapQueryContext.this.absentKey();
        }

        @NotNull
        @Override
        public Data<V> defaultValue() {
            return CompiledReplicatedMapQueryContext.this.defaultValue();
        }

        @NotNull
        @Override
        public MapContext<K, V, ?> context() {
            return CompiledReplicatedMapQueryContext.this.context();
        }

        @Override
        public void doInsert(Data<V> value) {
            CompiledReplicatedMapQueryContext.this.doInsert(value);
        }
    }

    public class UpdateLock implements InterProcessLock {
        @NotNull
        private IllegalMonitorStateException forbiddenUpgrade() {
            return new IllegalMonitorStateException("Cannot upgrade from read to update lock");
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
        public boolean isHeldByCurrentThread() {
            return CompiledReplicatedMapQueryContext.this.localLockState().update;
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
    }

    public class WrappedValueInstanceData extends CopyingInstanceData<V> {
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
            this.closeBuffer();
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
            buf = getBuffer(this.buf, size);
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
    }

    public class WriteLock implements InterProcessLock {
        @NotNull
        private IllegalMonitorStateException forbiddenUpgrade() {
            return new IllegalMonitorStateException("Cannot upgrade from read to write lock");
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
    }

    public enum SearchState {
        PRESENT, DELETED, ABSENT;    }

    private void _AllocatedChunks_incrementSegmentEntriesIfNeeded() {
    }

    private void _CheckOnEachPublicOperation_checkOnEachPublicOperation() {
        this.checkAccessingFromOwnerThread();
    }

    private void _MapAbsent_doInsert(Data<V> value) {
        this.putPrefix();
        if (!(this.searchStatePresent())) {
            if (this.searchStateDeleted()) {
                this.putValueDeletedEntry(value);
            } else {
                putEntry(value);
            }
            this.incrementModCountGuarded();
            this.setSearchStateGuarded(CompiledReplicatedMapQueryContext.SearchState.PRESENT);
        } else {
            throw new IllegalStateException("Entry is present in the map when doInsert() is called");
        }
    }

    private void _MapEntryStages_putValueDeletedEntry(Data<V> newValue) {
        assert this.innerUpdateLock.isHeldByCurrentThread();
        int newSizeInChunks;
        long entryStartOffset = keySizeOffset();
        long newSizeOfEverythingBeforeValue = -1;
        boolean newValueSizeIsDifferent = (newValue.size()) != (valueSize());
        if (newValueSizeIsDifferent) {
            newSizeOfEverythingBeforeValue = newSizeOfEverythingBeforeValue(newValue);
            long newValueOffset = this.m().alignment.alignAddr((entryStartOffset + newSizeOfEverythingBeforeValue));
            long newEntrySize = (newValueOffset + (newValue.size())) - entryStartOffset;
            newSizeInChunks = this.m().inChunks(newEntrySize);
        } else {
            newSizeInChunks = entrySizeInChunks();
        }
        if ((((pos()) + newSizeInChunks) < (this.freeList().size())) && (this.freeList().allClear(pos(), ((pos()) + newSizeInChunks)))) {
            this.freeList().set(pos(), ((pos()) + newSizeInChunks));
            this.innerWriteLock.lock();
            this.incrementSegmentEntriesIfNeeded();
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
                this.initEntryAndKeyCopying(entrySize, ((valueSizeOffset()) - entryStartOffset));
                initValue(newValue);
            } else {
                long oldValueSizeOffset = valueSizeOffset();
                long oldValueSize = valueSize();
                long oldValueOffset = valueOffset();
                this.initEntryAndKeyCopying(entrySize, ((valueOffset()) - entryStartOffset));
                initValueWithoutSize(newValue, oldValueSizeOffset, oldValueSize, oldValueOffset);
            }
            freeExtraAllocatedChunks();
        }
        this.putValueVolatile(this.hashLookupPos(), pos());
    }

    private void _MapQuery_doRemove() {
        this.checkOnEachPublicOperation();
        this.innerUpdateLock.lock();
        if (searchStatePresent()) {
            this.innerWriteLock.lock();
            this.remove();
            this.innerRemoveEntryExceptHashLookupUpdate();
            setSearchStateGuarded(CompiledReplicatedMapQueryContext.SearchState.DELETED);
        } else {
            throw new IllegalStateException("Entry is absent when doRemove() is called");
        }
    }

    private void _MapQuery_doReplaceValue(Data<V> newValue) {
        putPrefix();
        if (searchStatePresent()) {
            this.innerDefaultReplaceValue(newValue);
            this.incrementModCountGuarded();
            setSearchStateGuarded(CompiledReplicatedMapQueryContext.SearchState.PRESENT);
        } else {
            throw new IllegalStateException("Entry is absent in the map when doReplaceValue() is called");
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

    public void setSearchState(CompiledReplicatedMapQueryContext.SearchState newSearchState) {
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
            if ((prevContext.nextNode) == (this))
                break;

            prevContext = prevContext.nextNode;
        }
        assert (nextNode) == null;
        prevContext.nextNode = null;
    }

    private boolean _MapQuery_entryPresent() {
        return searchStatePresent();
    }

    private long _HashEntryStages_entryEnd() {
        return keyEnd();
    }

    private void _MapEntryStages_relocation(Data<V> newValue, long newSizeOfEverythingBeforeValue) {
        this.free(pos(), entrySizeInChunks());
        long entrySize = innerEntrySize(newSizeOfEverythingBeforeValue, newValue.size());
        this.initEntryAndKeyCopying(entrySize, ((valueSizeOffset()) - (keySizeOffset())));
        writeValueAndPutPos(newValue);
    }

    private CompiledReplicatedMapQueryContext _Chaining_createChaining() {
        return new CompiledReplicatedMapQueryContext(this);
    }

    public final int indexInContextChain;

    public int indexInContextChain() {
        return this.indexInContextChain;
    }

    public class DefaultReturnValue implements InstanceReturnValue<V> {
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

    public class InputKeyInstanceData extends CopyingInstanceData<K> implements KeyInitableData<K> {
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
            this.closeBuffer();
        }

        @Override
        public K instance() {
            return key();
        }

        private boolean marshalled = false;

        private DirectBytes buffer;

        public boolean bufferInit() {
            return (this.marshalled) != false;
        }

        private void initBuffer() {
            MKI mki = CompiledReplicatedMapQueryContext.this.keyMetaInterop(key());
            long size = mki.size(CompiledReplicatedMapQueryContext.this.keyInterop, key());
            buffer = getBuffer(this.buffer, size);
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
    }

    public class InputValueInstanceData extends CopyingInstanceData<V> implements ValueInitableData<V> {
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
            this.closeBuffer();
        }

        @Override
        public V instance() {
            return value();
        }

        private boolean marshalled = false;

        private DirectBytes buffer;

        public boolean bufferInit() {
            return (this.marshalled) != false;
        }

        private void initBuffer() {
            MVI mvi = CompiledReplicatedMapQueryContext.this.valueMetaInterop(value());
            long size = mvi.size(CompiledReplicatedMapQueryContext.this.valueInterop, value());
            buffer = getBuffer(this.buffer, size);
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
    }

    public class UsingReturnValue implements UsableReturnValue<V> {
        @Override
        public void returnValue(@NotNull
                                Data<V> value) {
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

    private MapEntry<K, V> _MapQuery_entry() {
        this.checkOnEachPublicOperation();
        return entryPresent() ? this : null;
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

    public final List<net.openhft.chronicle.map.impl.CompiledReplicatedMapQueryContext> contextChain;

    public final UpdateLock innerUpdateLock;

    public UpdateLock innerUpdateLock() {
        return this.innerUpdateLock;
    }

    public List<net.openhft.chronicle.map.impl.CompiledReplicatedMapQueryContext> contextChain() {
        return this.contextChain;
    }

    @Nullable
    private MapAbsentEntry<K, V> _MapQuery_absentEntry() {
        this.checkOnEachPublicOperation();
        return entryPresent() ? null : this.absent();
    }

    final BytesReturnValue bytesReturnValue;

    public BytesReturnValue bytesReturnValue() {
        return this.bytesReturnValue;
    }

    public final ThreadLocalCopies copies;

    final EntryKeyBytesData entryKey;

    public EntryKeyBytesData entryKey() {
        return this.entryKey;
    }

    public ThreadLocalCopies copies() {
        return this.copies;
    }

    private void countValueOffset() {
        this.m().alignment.alignPositionAddr(entryBytes);
        valueOffset = entryBytes.position();
    }

    final DummyValueZeroData dummyValue;

    public DummyValueZeroData dummyValue() {
        return this.dummyValue;
    }

    public final UsingReturnValue usingReturnValue;

    public UsingReturnValue usingReturnValue() {
        return this.usingReturnValue;
    }

    public final InputKeyBytesData inputKeyBytesValue;

    public InputKeyBytesData inputKeyBytesValue() {
        return this.inputKeyBytesValue;
    }

    public final DefaultReturnValue defaultReturnValue;

    public DefaultReturnValue defaultReturnValue() {
        return this.defaultReturnValue;
    }

    public final EntryValueBytesData entryValue;

    public EntryValueBytesData entryValue() {
        return this.entryValue;
    }

    final ReplicatedInputKeyBytesData replicatedInputKeyBytesValue;

    public ReplicatedInputKeyBytesData replicatedInputKeyBytesValue() {
        return this.replicatedInputKeyBytesValue;
    }

    final ReplicatedInputValueBytesData replicatedInputValueBytesValue;

    public ReplicatedInputValueBytesData replicatedInputValueBytesValue() {
        return this.replicatedInputValueBytesValue;
    }

    final WrappedValueInstanceData wrappedValueInstanceValue;

    public WrappedValueInstanceData wrappedValueInstanceValue() {
        return this.wrappedValueInstanceValue;
    }

    public final InputFirstValueBytesData inputFirstValueBytesValue;

    public final JavaLangBytesReusableBytesStore replicatedInputStore;

    public final JavaLangBytesReusableBytesStore inputStore;

    public JavaLangBytesReusableBytesStore inputStore() {
        return this.inputStore;
    }

    public JavaLangBytesReusableBytesStore replicatedInputStore() {
        return this.replicatedInputStore;
    }

    public InputFirstValueBytesData inputFirstValueBytesValue() {
        return this.inputFirstValueBytesValue;
    }

    final ReplicatedMapAbsentDelegating absentDelegating;

    public final InputSecondValueBytesData inputSecondValueBytesValue;

    public InputSecondValueBytesData inputSecondValueBytesValue() {
        return this.inputSecondValueBytesValue;
    }

    public ReplicatedMapAbsentDelegating absentDelegating() {
        return this.absentDelegating;
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

    public final BytesStore entryBS;

    public BytesStore entryBS() {
        return this.entryBS;
    }

    public final BytesReader<V> valueReader;

    public BytesReader<V> valueReader() {
        return this.valueReader;
    }

    public final KI keyInterop;

    public KI keyInterop() {
        return this.keyInterop;
    }

    public final BytesReader<K> keyReader;

    public BytesReader<K> keyReader() {
        return this.keyReader;
    }

    @Override
    public MapAbsentEntry<K, V> absent() {
        return this;
    }

    @Override
    public Data<V> dummyZeroValue() {
        return this.dummyValue;
    }

    @Override
    public ReplicatedChronicleMap<K, KI, MKI, V, VI, MVI, R> m() {
        return m;
    }

    @Override
    public ChronicleMap<K, V> map() {
        return m();
    }

    long sizeOfEverythingBeforeValue(long keySize, long valueSize) {
        return (_MapEntryStages_sizeOfEverythingBeforeValue(keySize, valueSize)) + (ReplicatedChronicleMap.ADDITIONAL_ENTRY_BYTES);
    }

    public MVI valueMetaInterop(V value) {
        return this.m().metaValueInteropProvider.get(this.copies, this.m().originalMetaValueInterop, valueInterop, value);
    }

    public void closeValueBytesInteropValueMetaInteropDependants() {
        this.wrappedValueInstanceValue.closeBuffer();
        this.inputValueInstanceValue.closeBuffer();
    }

    public long innerEntrySize(long sizeOfEverythingBeforeValue, long valueSize) {
        if (this.m().constantlySizedEntry) {
            return this.m().alignment.alignAddr((sizeOfEverythingBeforeValue + valueSize));
        } else if (this.m().couldNotDetermineAlignmentBeforeAllocation) {
            return (sizeOfEverythingBeforeValue + (this.m().worstAlignment)) + valueSize;
        } else {
            return (this.m().alignment.alignAddr(sizeOfEverythingBeforeValue)) + valueSize;
        }
    }

    public final long entrySize(long keySize, long valueSize) {
        long sizeOfEverythingBeforeValue = sizeOfEverythingBeforeValue(keySize, valueSize);
        return innerEntrySize(sizeOfEverythingBeforeValue, valueSize);
    }

    public void checkAccessingFromOwnerThread() {
        if ((owner) != (Thread.currentThread())) {
            throw new ConcurrentModificationException("Context shouldn\'t be accessed from multiple threads");
        }
    }

    public void closeOwnerThreadHolderCheckAccessingFromOwnerThreadDependants() {
        this.closeQueryCheckOnEachPublicOperationCheckOnEachPublicOperationDependants();
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

    public <T>T contextAtIndexInChain(int index) {
        return ((T)(contextChain.get(index)));
    }

    public void closeReplicatedChronicleMapHolderImplContextAtIndexInChainDependants() {
        this.closeQuerySegmentStagesTryFindInitLocksOfThisSegmentDependants();
    }

    public MKI keyMetaInterop(K key) {
        return this.h().metaKeyInteropProvider.get(this.copies, this.h().originalMetaKeyInterop, keyInterop, key);
    }

    public void closeKeyBytesInteropKeyMetaInteropDependants() {
        this.inputKeyInstanceValue.closeBuffer();
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
        this.closeReplicatedMapEntryStagesKeyEndDependants();
        this.closeReplicatedMapQueryKeyEqualsDependants();
        this.entryKey.closeEntryKeyBytesDataInnerGetUsingDependants();
    }

    public Data<K> inputKey = null;

    public boolean inputKeyInit() {
        return (this.inputKey) != null;
    }

    public void initInputKey(Data<K> inputKey) {
        this.inputKey = inputKey;
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
        this.closeReplicatedMapQueryKeyEqualsDependants();
        this.closeHashOfKey();
    }

    public long hashOfKey = 0;

    public boolean hashOfKeyInit() {
        return (this.hashOfKey) != 0;
    }

    void initHashOfKey() {
        hashOfKey = inputKey().hash(net.openhft.chronicle.algo.hashing.LongHashFunction.city_1_1());
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
        this.closeTheSegmentIndex();
        this.closeSearchKey();
    }

    public int segmentIndex = -1;

    public boolean theSegmentIndexInit() {
        return (this.segmentIndex) >= 0;
    }

    void initTheSegmentIndex() {
        segmentIndex = this.h().hashSplitting.segmentIndex(this.hashOfKey());
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
        this.closeSegment();
        this.closeSegmentHashLookup();
        this.closeSegHeader();
    }

    long entrySpaceOffset = 0;

    MultiStoreBytes freeListBytes = new MultiStoreBytes();

    public SingleThreadedDirectBitSet freeList = new SingleThreadedDirectBitSet();

    boolean segmentInit() {
        return (entrySpaceOffset) > 0;
    }

    void initSegment() {
        VanillaChronicleHash<?, ?, ?, ?, ?> h = this.h();
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
        this.closeEntryOffset();
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
        long hashLookupOffset = this.h().segmentOffset(this.segmentIndex());
        innerInitSegmentHashLookup(((this.h().ms.address()) + hashLookupOffset), this.h().segmentHashLookupCapacity, this.h().segmentHashLookupEntrySize, this.h().segmentHashLookupKeyBits, this.h().segmentHashLookupValueBits);
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
        this.closeHashLookupReadEntryDependants();
        this.closeHashLookupStepDependants();
        this.closeHashLookupStepBackDependants();
        this.closeHashLookupIndexToPosDependants();
        this.closeHashLookupMaskUnsetKeyDependants();
        this.closeHashLookupKeyDependants();
        this.closeHashLookupValueDependants();
        this.closeHashLookupHlPosDependants();
        this.closeHashLookupEmptyDependants();
    }

    public long readEntry(long pos) {
        return NativeBytes.UNSAFE.getLong(((address()) + pos));
    }

    public void closeHashLookupReadEntryDependants() {
        this.closeHashLookupSearchNextPosDependants();
        this.closeHashLookupSearchCheckSlotContainsExpectedKeyAndValueDependants();
    }

    public long step(long pos) {
        return (pos += hashLookupEntrySize()) <= (capacityMask2()) ? pos : 0L;
    }

    public void closeHashLookupStepDependants() {
        this.closeHashLookupSearchNextPosDependants();
    }

    public long stepBack(long pos) {
        return (pos -= hashLookupEntrySize()) >= 0 ? pos : capacityMask2();
    }

    public void closeHashLookupStepBackDependants() {
        this.closeHashLookupSearchFoundDependants();
    }

    long indexToPos(long index) {
        return index * (hashLookupEntrySize());
    }

    public void closeHashLookupIndexToPosDependants() {
        this.closeHashLookupHlPosDependants();
    }

    public long maskUnsetKey(long key) {
        return (key &= keyMask()) != (UNSET_KEY) ? key : keyMask();
    }

    public void closeHashLookupMaskUnsetKeyDependants() {
        this.closeSearchKey();
    }

    long entry(long key, long value) {
        return key | (value << (keyBits()));
    }

    public void writeEntryVolatile(long pos, long prevEntry, long key, long value) {
        long entry = (prevEntry & (~(entryMask()))) | (entry(key, value));
        NativeBytes.UNSAFE.putLongVolatile(null, ((address()) + pos), entry);
    }

    public void clearHashLookup() {
        NativeBytes.UNSAFE.setMemory(address(), ((capacityMask2()) + (hashLookupEntrySize())), ((byte)(0)));
    }

    public long key(long entry) {
        return entry & (keyMask());
    }

    public void closeHashLookupKeyDependants() {
        this.closeHashLookupSearchNextPosDependants();
        this.closeHashLookupSearchCheckSlotContainsExpectedKeyAndValueDependants();
    }

    public void writeEntry(long pos, long prevEntry, long key, long value) {
        long entry = (prevEntry & (~(entryMask()))) | (entry(key, value));
        NativeBytes.UNSAFE.putLong(((address()) + pos), entry);
    }

    void writeEntry(long pos, long prevEntry, long anotherEntry) {
        long entry = (prevEntry & (~(entryMask()))) | (anotherEntry & (entryMask()));
        NativeBytes.UNSAFE.putLong(((address()) + pos), entry);
    }

    public long value(long entry) {
        return (entry >>> (keyBits())) & (valueMask());
    }

    public void closeHashLookupValueDependants() {
        this.closeHashLookupSearchNextPosDependants();
        this.closeHashLookupSearchCheckSlotContainsExpectedKeyAndValueDependants();
    }

    public long hlPos(long key) {
        return indexToPos((key & (capacityMask())));
    }

    public void closeHashLookupHlPosDependants() {
        this.closeSearchKey();
    }

    public void checkValueForPut(long value) {
        assert (value & (~(valueMask()))) == 0L : "Value out of range, was " + value;
    }

    public void putValueVolatile(long pos, long value) {
        checkValueForPut(value);
        long currentEntry = readEntry(pos);
        writeEntryVolatile(pos, currentEntry, key(currentEntry), value);
    }

    void clearEntry(long pos, long prevEntry) {
        long entry = prevEntry & (~(entryMask()));
        NativeBytes.UNSAFE.putLong(((address()) + pos), entry);
    }

    public boolean empty(long entry) {
        return (entry & (entryMask())) == (UNSET_ENTRY);
    }

    public void closeHashLookupEmptyDependants() {
        this.closeHashLookupSearchNextPosDependants();
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

    public void moveChange(long oldPos, long newPos) {
        this.m().moveChange(this.segmentIndex(), oldPos, newPos);
    }

    long segmentHeaderAddress;

    SegmentHeader segmentHeader = null;

    public boolean segHeaderInit() {
        return (this.segmentHeader) != null;
    }

    private void initSegHeader() {
        segmentHeaderAddress = (this.h().ms.address()) + (this.h().segmentHeaderOffset(segmentIndex()));
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
        this.closeQuerySegmentStagesTryFindInitLocksOfThisSegmentDependants();
        this.closeLocks();
        this.innerReadLock.closeReadLockLockDependants();
    }

    public void nextPosToSearchFrom(long nextPosToSearchFrom) {
        segmentHeader().nextPosToSearchFrom(segmentHeaderAddress(), nextPosToSearchFrom);
    }

    public void updateNextPosToSearchFrom(long allocated, int chunks) {
        long nextPosToSearchFrom = allocated + chunks;
        if (nextPosToSearchFrom >= (this.h().actualChunksPerSegment))
            nextPosToSearchFrom = 0L;

        nextPosToSearchFrom(nextPosToSearchFrom);
    }

    long nextPosToSearchFrom() {
        return segmentHeader().nextPosToSearchFrom(segmentHeaderAddress());
    }

    public long alloc(int chunks) {
        VanillaChronicleHash<?, ?, ?, ?, ?> h = this.h();
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

    boolean tryFindInitLocksOfThisSegment(Object thisContext, int index) {
        CompiledReplicatedMapQueryContext c = this.contextAtIndexInChain(index);
        if ((((c.segmentHeader()) != null) && ((c.segmentHeaderAddress()) == (segmentHeaderAddress()))) && ((c.rootContextOnThisSegment()) != null)) {
            throw new IllegalStateException("Nested context not implemented yet");
        } else {
            return false;
        }
    }

    public void closeQuerySegmentStagesTryFindInitLocksOfThisSegmentDependants() {
        this.closeLocks();
    }

    public void entries(long size) {
        segmentHeader().size(segmentHeaderAddress(), size);
    }

    public long entries() {
        return segmentHeader().size(segmentHeaderAddress());
    }

    public void deleted(long deleted) {
        segmentHeader().deleted(segmentHeaderAddress(), deleted);
    }

    public long deleted() {
        return segmentHeader().deleted(segmentHeaderAddress());
    }

    public long size() {
        return (entries()) - (deleted());
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
        int indexOfThisContext = this.indexInContextChain;
        for (int i = indexOfThisContext - 1 ; i >= 0 ; i--) {
            if (tryFindInitLocksOfThisSegment(this, i))
                return ;

        }
        for (int i = indexOfThisContext + 1, size = this.contextChain.size() ; i < size ; i++) {
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
        if ((rootContextOnThisSegment) == (this)) {
            closeRootLocks();
        } else {
            closeNestedLocks();
        }
        localLockState = null;
        rootContextOnThisSegment = null;
    }

    public void closeLocksDependants() {
        this.innerReadLock.closeReadLockLockDependants();
        this.closeReplicatedMapQueryDropSearchIfNestedContextsAndPresentHashLookupSlotCheckFailedDependants();
    }

    public void clearSegment() {
        this.innerWriteLock.lock();
        this.clearHashLookup();
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
        searchKey = this.maskUnsetKey(this.h().hashSplitting.segmentHash(this.hashOfKey()));
        searchStartPos = this.hlPos(searchKey);
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
        this.closeHashLookupPos();
        this.closeHashLookupSearchNextPosDependants();
        this.closeHashLookupSearchCheckSlotContainsExpectedKeyAndValueDependants();
    }

    public long hashLookupPos = -1;

    public boolean hashLookupPosInit() {
        return (this.hashLookupPos) >= 0;
    }

    public void initHashLookupPos() {
        this.innerReadLock.lock();
        this.hashLookupPos = this.searchStartPos();
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
        this.closeHashLookupSearchNextPosDependants();
        this.closeHashLookupSearchFoundDependants();
        this.closeHashLookupSearchCheckSlotContainsExpectedKeyAndValueDependants();
    }

    public void putVolatile(long value) {
        this.checkValueForPut(value);
        long currentEntry = this.readEntry(this.hashLookupPos());
        assert (this.key(currentEntry)) == (searchKey());
        this.writeEntryVolatile(this.hashLookupPos(), currentEntry, searchKey(), value);
    }

    public long nextPos() {
        long pos = this.hashLookupPos();
        while (true) {
            long entry = this.readEntry(pos);
            if (this.empty(entry)) {
                this.setHashLookupPosGuarded(pos);
                return -1L;
            }
            pos = this.step(pos);
            if (pos == (searchStartPos()))
                break;

            if ((this.key(entry)) == (searchKey())) {
                this.setHashLookupPosGuarded(pos);
                return this.value(entry);
            }
        }
        throw new IllegalStateException(("MultiMap is full, that most likely means you " + ("misconfigured entrySize/chunkSize, and entries tend to take less chunks than " + "expected")));
    }

    public void closeHashLookupSearchNextPosDependants() {
        this.closeKeySearch();
    }

    public void putNewVolatile(long value) {
        this.checkValueForPut(value);
        long currentEntry = this.readEntry(this.hashLookupPos());
        this.writeEntryVolatile(this.hashLookupPos(), currentEntry, searchKey(), value);
    }

    void put(long value) {
        this.checkValueForPut(value);
        this.writeEntry(this.hashLookupPos(), this.readEntry(this.hashLookupPos()), searchKey(), value);
    }

    public void found() {
        this.setHashLookupPosGuarded(this.stepBack(this.hashLookupPos()));
    }

    public void closeHashLookupSearchFoundDependants() {
        this.closeKeySearch();
    }

    public boolean checkSlotContainsExpectedKeyAndValue(long value) {
        long entry = this.readEntry(this.hashLookupPos());
        return ((this.key(entry)) == (searchKey())) && ((this.value(entry)) == value);
    }

    public void closeHashLookupSearchCheckSlotContainsExpectedKeyAndValueDependants() {
        this.closeReplicatedMapQueryDropSearchIfNestedContextsAndPresentHashLookupSlotCheckFailedDependants();
    }

    public void remove() {
        this.setHashLookupPosGuarded(this.remove(this.hashLookupPos()));
    }

    public boolean checkSlotIsEmpty() {
        return this.empty(this.readEntry(this.hashLookupPos()));
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
        this.closeEntryOffset();
        this.closeReplicatedMapQueryDropSearchIfNestedContextsAndPresentHashLookupSlotCheckFailedDependants();
    }

    public void dropChange() {
        this.m().dropChange(this.segmentIndex(), this.pos());
    }

    public long keySizeOffset = -1;

    public boolean entryOffsetInit() {
        return (this.keySizeOffset) >= 0;
    }

    public void initEntryOffset() {
        keySizeOffset = (this.entrySpaceOffset()) + ((pos()) * (this.h().chunkSize));
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
        this.closeReplicatedMapEntryStagesEntrySizeDependants();
        this.closeReplicatedMapEntryStagesReadExistingEntryDependants();
    }

    public void copyExistingEntry(long newPos, long bytesToCopy) {
        long oldKeySizeOffset = keySizeOffset();
        long oldKeyOffset = keyOffset();
        initPos(newPos);
        initKeyOffset(((keySizeOffset()) + (oldKeyOffset - oldKeySizeOffset)));
        entryBS.write(keySizeOffset(), entryBS, oldKeySizeOffset, bytesToCopy);
    }

    public void readExistingEntry(long pos) {
        initPos(pos);
        entryBytes.position(keySizeOffset());
        initKeySize(this.h().keySizeMarshaller.readSize(entryBytes));
        initKeyOffset(entryBytes.position());
    }

    public void closeReplicatedMapEntryStagesReadExistingEntryDependants() {
        this.closeKeySearch();
    }

    public void raiseChange() {
        this.m().raiseChange(this.segmentIndex(), this.pos());
    }

    public void updateChange() {
        if (!(replicationUpdateInit())) {
            raiseChange();
        }
    }

    public Bytes replicatedInputBytes = null;

    public boolean replicatedInputBytesInit() {
        return (this.replicatedInputBytes) != null;
    }

    public void initReplicatedInputBytes(Bytes replicatedInputBytes) {
        this.replicatedInputBytes = replicatedInputBytes;
        replicatedInputStore.setBytes(replicatedInputBytes);
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
        this.replicatedInputKeyBytesValue.closeReplicatedInputKeyBytesDataGetUsingDependants();
        this.replicatedInputValueBytesValue.closeReplicatedInputValueBytesDataGetUsingDependants();
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
        this.closeReplicatedMapEntryStagesKeyEndDependants();
        this.closeReplicatedMapQueryKeyEqualsDependants();
        this.entryKey.closeEntryKeyBytesDataSizeDependants();
    }

    public long keyEnd() {
        return (keyOffset()) + (keySize());
    }

    public void closeReplicatedMapEntryStagesKeyEndDependants() {
        this.closeReplicatedMapEntryStagesCountValueSizeOffsetDependants();
        this.closeReplicationState();
        this.closeReplicatedMapEntryStagesEntryEndDependants();
    }

    long countValueSizeOffset() {
        return (_MapEntryStages_countValueSizeOffset()) + (ReplicatedChronicleMap.ADDITIONAL_ENTRY_BYTES);
    }

    public void closeReplicatedMapEntryStagesCountValueSizeOffsetDependants() {
        this.closeValueSizeOffset();
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
        this.closeValSize();
    }

    public long valueSize = -1;

    public long valueOffset;

    public boolean valSizeInit() {
        return (this.valueSize) >= 0;
    }

    void initValSize() {
        entryBytes.position(valueSizeOffset());
        valueSize = this.m().readValueSize(entryBytes);
        countValueOffset();
        this.closeValSizeDependants();
    }

    void initValSize(long valueSize) {
        this.valueSize = valueSize;
        entryBytes.position(valueSizeOffset());
        this.m().valueSizeMarshaller.writeSize(entryBytes, valueSize);
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
        this.closeReplicatedMapEntryStagesEntryEndDependants();
        this.entryValue.closeEntryValueBytesDataSizeDependants();
        this.entryValue.closeEntryValueBytesDataInnerGetUsingDependants();
    }

    public void writeValue(Data<?> value) {
        value.writeTo(entryBS, valueOffset());
    }

    public void initValueWithoutSize(Data<?> value, long oldValueSizeOffset, long oldValueSize, long oldValueOffset) {
        assert oldValueSize == (value.size());
        initValSizeEqualToOld(oldValueSizeOffset, oldValueSize, oldValueOffset);
        writeValue(value);
    }

    public long newSizeOfEverythingBeforeValue(Data<V> newValue) {
        return ((valueSizeOffset()) + (this.m().valueSizeMarshaller.sizeEncodingSize(newValue.size()))) - (keySizeOffset());
    }

    public void initValue(Data<?> value) {
        entryBytes.position(valueSizeOffset());
        initValSize(value.size());
        writeValue(value);
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

    void updateReplicationState(long timestamp, byte identifier) {
        entryBytes.position(replicationBytesOffset());
        entryBytes.writeLong(timestamp);
        entryBytes.writeByte(identifier);
    }

    public void updatedReplicationStateOnAbsentEntry() {
        if (!(this.replicationUpdateInit())) {
            this.innerWriteLock.lock();
            updateReplicationState(this.m().timeProvider.currentTime(), this.m().identifier());
        }
    }

    private long entryDeletedOffset() {
        return (replicationBytesOffset()) + 9L;
    }

    public void writeEntryDeleted() {
        entryBS.writeBoolean(entryDeletedOffset(), true);
    }

    public void writeEntryPresent() {
        entryBS.writeBoolean(entryDeletedOffset(), false);
    }

    public boolean entryDeleted() {
        return entryBS.readBoolean(entryDeletedOffset());
    }

    public long timestamp() {
        return entryBS.readLong(replicationBytesOffset());
    }

    private long timestampOffset() {
        return replicationBytesOffset();
    }

    private long identifierOffset() {
        return (replicationBytesOffset()) + 8L;
    }

    byte identifier() {
        return entryBS.readByte(identifierOffset());
    }

    public void updatedReplicationStateOnPresentEntry() {
        if (!(this.replicationUpdateInit())) {
            this.innerWriteLock.lock();
            long timestamp;
            if ((identifier()) != (this.m().identifier())) {
                timestamp = Math.max(((timestamp()) + 1), this.m().timeProvider.currentTime());
            } else {
                timestamp = this.m().timeProvider.currentTime();
            }
            updateReplicationState(timestamp, this.m().identifier());
        }
    }

    protected long entryEnd() {
        return (valueOffset()) + (valueSize());
    }

    public void closeReplicatedMapEntryStagesEntryEndDependants() {
        this.closeReplicatedMapEntryStagesEntrySizeDependants();
    }

    long entrySize() {
        return (entryEnd()) - (keySizeOffset());
    }

    public void closeReplicatedMapEntryStagesEntrySizeDependants() {
        this.closeTheEntrySizeInChunks();
    }

    public int entrySizeInChunks = 0;

    public boolean theEntrySizeInChunksInit() {
        return (this.entrySizeInChunks) != 0;
    }

    void initTheEntrySizeInChunks() {
        entrySizeInChunks = this.h().inChunks(entrySize());
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
        this.free(pos(), entrySizeInChunks());
        this.entries(((this.entries()) - 1L));
        this.incrementModCountGuarded();
    }

    boolean keyEquals() {
        return ((inputKey().size()) == (this.keySize())) && (BytesUtil.bytesEqual(this.entryBS, this.keyOffset(), inputKey().bytes(), inputKey().offset(), this.keySize()));
    }

    public void closeReplicatedMapQueryKeyEqualsDependants() {
        this.closeKeySearch();
    }

    protected CompiledReplicatedMapQueryContext.SearchState searchState = null;

    boolean keySearchInit() {
        return (this.searchState) != null;
    }

    void initKeySearch() {
        for (long pos ; (pos = this.nextPos()) >= 0L ; ) {
            this.readExistingEntry(pos);
            if (!(keyEquals()))
                continue;

            this.found();
            keyFound();
            return ;
        }
        searchState = CompiledReplicatedMapQueryContext.SearchState.ABSENT;
        this.closeKeySearchDependants();
    }

    public CompiledReplicatedMapQueryContext.SearchState searchState() {
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
        this.closeReplicatedMapQueryDropSearchIfNestedContextsAndPresentHashLookupSlotCheckFailedDependants();
    }

    public void incrementSegmentEntriesIfNeeded() {
        if ((this.searchState()) != (CompiledReplicatedMapQueryContext.SearchState.PRESENT)) {
            this.entries(((this.entries()) + 1L));
        }
    }

    public void dropSearchIfNestedContextsAndPresentHashLookupSlotCheckFailed() {
        if (this.locksInit()) {
            if ((this.concurrentSameThreadContexts()) && ((this.rootContextOnThisSegment().latestSameThreadSegmentModCount()) != (this.contextModCount()))) {
                if (keySearchInit()) {
                    if ((searchState()) == (CompiledReplicatedMapQueryContext.SearchState.PRESENT)) {
                        if (!(this.checkSlotContainsExpectedKeyAndValue(this.pos())))
                            this.closeHashLookupPos();

                    }
                }
            }
        }
    }

    public void closeReplicatedMapQueryDropSearchIfNestedContextsAndPresentHashLookupSlotCheckFailedDependants() {
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
    public Data<K> absentKey() {
        this.checkOnEachPublicOperation();
        return this.inputKey();
    }

    @NotNull
    @Override
    public InterProcessLock readLock() {
        this.checkOnEachPublicOperation();
        return this.innerReadLock;
    }

    public Data<K> queriedKey() {
        this.checkOnEachPublicOperation();
        return inputKey();
    }

    @NotNull
    @Override
    public Data<V> defaultValue() {
        this.checkOnEachPublicOperation();
        return this.wrapValueAsValue(this.m().defaultValue(this.deprecatedMapKeyContext));
    }

    @Override
    public void raiseChanged() {
        this.checkOnEachPublicOperation();
        this.innerUpdateLock.lock();
        this.raiseChange();
    }

    @NotNull
    @Override
    public Data<K> key() {
        this.checkOnEachPublicOperation();
        return this.entryKey;
    }

    @NotNull
    @Override
    public MapContext<K, V, ?> context() {
        this.checkOnEachPublicOperation();
        return this;
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
        this.checkOnEachPublicOperation();
        return this.innerWriteLock;
    }

    @Override
    public void updateOrigin(byte newIdentifier, long newTimestamp) {
        this.checkOnEachPublicOperation();
        this.innerWriteLock.lock();
        updateReplicationState(newTimestamp, newIdentifier);
    }

    @Override
    public R insert(@NotNull
                    MapAbsentEntry<K, V> absentEntry, Data<V> value) {
        this.checkOnEachPublicOperation();
        return this.m().entryOperations.insert(absentEntry, value);
    }

    @Override
    public void dropChanged() {
        this.checkOnEachPublicOperation();
        this.innerUpdateLock.lock();
        this.dropChange();
    }

    @Override
    public Data<V> wrapValueAsValue(V value) {
        this.checkOnEachPublicOperation();
        WrappedValueInstanceData wrapped = this.wrappedValueInstanceValue;
        wrapped = wrapped.getUnusedWrappedValueGuarded();
        wrapped.initValue(value);
        return wrapped;
    }

    @NotNull
    @Override
    public Data<V> value() {
        this.checkOnEachPublicOperation();
        return this.entryValue;
    }

    @Override
    public Data<V> defaultValue(@NotNull
                                MapAbsentEntry<K, V> absentEntry) {
        this.checkOnEachPublicOperation();
        return this.m().entryOperations.defaultValue(absentEntry);
    }

    @Override
    public long originTimestamp() {
        this.checkOnEachPublicOperation();
        return timestamp();
    }

    @Override
    public byte originIdentifier() {
        this.checkOnEachPublicOperation();
        return identifier();
    }

    public boolean searchStateDeleted() {
        return (((searchState()) == (CompiledReplicatedMapQueryContext.SearchState.DELETED)) && (!(this.concurrentSameThreadContexts()))) && (this.innerUpdateLock.isHeldByCurrentThread());
    }

    public boolean searchStatePresent() {
        return (searchState()) == (CompiledReplicatedMapQueryContext.SearchState.PRESENT);
    }

    public boolean entryPresent() {
        return (_MapQuery_entryPresent()) && (!(this.entryDeleted()));
    }

    @Nullable
    @Override
    public MapAbsentEntry<K, V> absentEntry() {
        this.checkOnEachPublicOperation();
        if (entryPresent()) {
            return null;
        } else {
            if (!(searchStatePresent())) {
                return this.absentDelegating;
            } else {
                assert this.entryDeleted();
                return this.absent();
            }
        }
    }

    @Override
    public MapReplicableEntry<K, V> entry() {
        return ((MapReplicableEntry<K, V>)(_MapQuery_entry()));
    }

    public boolean searchStateAbsent() {
        return (!(searchStatePresent())) && (!(searchStateDeleted()));
    }

    protected void putPrefix() {
        this.checkOnEachPublicOperation();
        boolean underUpdatedLockIsHeld = !(this.innerUpdateLock.isHeldByCurrentThread());
        if (underUpdatedLockIsHeld)
            this.innerUpdateLock.lock();

        boolean searchResultsNotTrusted = underUpdatedLockIsHeld || (this.concurrentSameThreadContexts());
        if (((this.hashLookupPosInit()) && (searchStateAbsent())) && searchResultsNotTrusted)
            this.closeHashLookupPos();

    }

    public void writeNewEntry(long pos, Data<?> key) {
        initPos(pos);
        initKeySize(key.size());
        entryBytes.position(keySizeOffset());
        this.h().keySizeMarshaller.writeSize(entryBytes, keySize());
        initKeyOffset(entryBytes.position());
        key.writeTo(entryBS, keyOffset());
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
    public long remoteTimestamp() {
        this.checkOnEachPublicOperation();
        return innerRemoteTimestamp();
    }

    private boolean testTimeStampInSensibleRange() {
        if ((this.m().timeProvider) == (TimeProvider.SYSTEM)) {
            long currentTime = TimeProvider.SYSTEM.currentTime();
            assert (Math.abs((currentTime - (timestamp())))) <= 100000000 : "unrealistic timestamp: " + (timestamp());
            assert (Math.abs((currentTime - (this.innerRemoteTimestamp())))) <= 100000000 : "unrealistic innerRemoteTimestamp: " + (this.innerRemoteTimestamp());
        }
        return true;
    }

    @Override
    public byte remoteIdentifier() {
        this.checkOnEachPublicOperation();
        return innerRemoteIdentifier();
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

    public void initEntryAndKeyCopying(long entrySize, long bytesToCopy) {
        initAllocatedChunks(this.h().inChunks(entrySize));
        this.copyExistingEntry(this.alloc(allocatedChunks()), bytesToCopy);
        incrementSegmentEntriesIfNeeded();
    }

    public final void freeExtraAllocatedChunks() {
        if (((!(this.m().constantlySizedEntry)) && (this.m().couldNotDetermineAlignmentBeforeAllocation)) && ((entrySizeInChunks()) < (this.allocatedChunks()))) {
            this.free(((pos()) + (entrySizeInChunks())), ((this.allocatedChunks()) - (entrySizeInChunks())));
        } else {
            initTheEntrySizeInChunks(this.allocatedChunks());
        }
    }

    public void writeValueAndPutPos(Data<V> value) {
        initValue(value);
        freeExtraAllocatedChunks();
        this.putValueVolatile(this.hashLookupPos(), pos());
    }

    protected void relocation(Data<V> newValue, long newSizeOfEverythingBeforeValue) {
        long oldPos = pos();
        _MapEntryStages_relocation(newValue, newSizeOfEverythingBeforeValue);
        this.moveChange(oldPos, pos());
    }

    public void innerDefaultReplaceValue(Data<V> newValue) {
        assert this.innerUpdateLock.isHeldByCurrentThread();
        boolean newValueSizeIsDifferent = (newValue.size()) != (this.valueSize());
        if (newValueSizeIsDifferent) {
            long newSizeOfEverythingBeforeValue = newSizeOfEverythingBeforeValue(newValue);
            long entryStartOffset = keySizeOffset();
            VanillaChronicleMap<?, ?, ?, ?, ?, ?, ?> m = this.m();
            long newValueOffset = m.alignment.alignAddr((entryStartOffset + newSizeOfEverythingBeforeValue));
            long newEntrySize = (newValueOffset + (newValue.size())) - entryStartOffset;
            int newSizeInChunks = m.inChunks(newEntrySize);
            newValueDoesNotFit : if (newSizeInChunks > (entrySizeInChunks())) {
                if (newSizeInChunks > (m.maxChunksPerEntry)) {
                    throw new IllegalArgumentException(((((("Value too large: " + "entry takes ") + newSizeInChunks) + " chunks, ") + (m.maxChunksPerEntry)) + " is maximum."));
                }
                if (this.freeList().allClear(((pos()) + (entrySizeInChunks())), ((pos()) + newSizeInChunks))) {
                    this.freeList().set(((pos()) + (entrySizeInChunks())), ((pos()) + newSizeInChunks));
                    break newValueDoesNotFit;
                }
                relocation(newValue, newSizeOfEverythingBeforeValue);
                return ;
            } else if (newSizeInChunks < (entrySizeInChunks())) {
                this.freeList().clear(((pos()) + newSizeInChunks), ((pos()) + (entrySizeInChunks())));
            }
        } else {
        }
        this.innerWriteLock.lock();
        if (newValueSizeIsDifferent) {
            initValue(newValue);
        } else {
            writeValue(newValue);
        }
        this.putValueVolatile(this.hashLookupPos(), pos());
    }

    @Override
    public void doReplaceValue(Data<V> newValue) {
        _MapQuery_doReplaceValue(newValue);
        this.updateChange();
        this.updatedReplicationStateOnPresentEntry();
    }

    @Override
    public void doRemove() {
        this.checkOnEachPublicOperation();
        this.innerUpdateLock.lock();
        if (entryPresent()) {
            if ((this.valueSize()) > (this.dummyValue.size()))
                this.innerDefaultReplaceValue(this.dummyValue);

            this.updatedReplicationStateOnPresentEntry();
            this.writeEntryDeleted();
            this.updateChange();
            this.deleted(((this.deleted()) + 1));
        } else {
            throw new IllegalStateException("Entry is absent in the map when doRemove() is called");
        }
    }

    public void putValueDeletedEntry(Data<V> newValue) {
        throw new AssertionError("Replicated Map doesn\'t remove entries truly, yet");
    }

    public void initEntryAndKey(long entrySize) {
        initAllocatedChunks(this.h().inChunks(entrySize));
        this.writeNewEntry(this.alloc(allocatedChunks()), this.inputKey());
        incrementSegmentEntriesIfNeeded();
    }

    void putEntry(Data<V> value) {
        assert this.searchStateAbsent();
        long entrySize = this.entrySize(this.inputKey().size(), value.size());
        this.initEntryAndKey(entrySize);
        this.initValue(value);
        this.freeExtraAllocatedChunks();
        this.putNewVolatile(this.pos());
    }

    @Override
    public void doInsert(Data<V> value) {
        this.putPrefix();
        if (!(this.entryPresent())) {
            if (!(this.searchStatePresent())) {
                putEntry(value);
                this.setSearchStateGuarded(CompiledReplicatedMapQueryContext.SearchState.PRESENT);
            } else {
                this.innerDefaultReplaceValue(value);
                this.deleted(((this.deleted()) - 1));
            }
            this.incrementModCountGuarded();
            this.writeEntryPresent();
            this.updateChange();
            this.updatedReplicationStateOnAbsentEntry();
        } else {
            throw new IllegalStateException("Entry is absent in the map when doInsert() is called");
        }
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
        riKeySize = this.m().keySizeMarshaller.readSize(replicatedInputBytes);
        riValueSize = this.m().valueSizeMarshaller.readSize(replicatedInputBytes);
        riTimestamp = replicatedInputBytes.readStopBit();
        riId = replicatedInputBytes.readByte();
        this.initReplicationUpdate(riTimestamp, riId);
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
        this.replicatedInputKeyBytesValue.closeReplicatedInputKeyBytesDataSizeDependants();
        this.replicatedInputValueBytesValue.closeReplicatedInputValueBytesDataSizeDependants();
        this.replicatedInputKeyBytesValue.closeReplicatedInputKeyBytesDataGetUsingDependants();
        this.replicatedInputValueBytesValue.closeReplicatedInputValueBytesDataGetUsingDependants();
    }

    public void processReplicatedEvent() {
        if ((riId()) == (this.m().identifier())) {
            return ;
        }
        this.initInputKey(this.replicatedInputKeyBytesValue);
        boolean debugEnabled = this.LOG.isDebugEnabled();
        this.innerUpdateLock.lock();
        if (isDeleted()) {
            if (debugEnabled) {
                this.LOG.debug("READING FROM SOURCE -  into local-id={}, remote={}, remove(key={})", this.m().identifier(), riId(), this.inputKey());
            }
            this.m().remoteOperations.remove(this);
            this.m().setLastModificationTime(riId(), riTimestamp());
            return ;
        }
        String message = null;
        if (debugEnabled) {
            message = String.format("READING FROM SOURCE -  into local-id=%d, remote-id=%d, put(key=%s,", this.m().identifier(), riId(), this.inputKey());
        }
        this.m().remoteOperations.put(this, this.replicatedInputValueBytesValue);
        this.m().setLastModificationTime(riId(), riTimestamp());
        if (debugEnabled) {
            this.LOG.debug((((message + "value=") + (this.replicatedInputValueBytesValue)) + ")"));
        }
    }

    public Bytes inputBytes = null;

    public boolean inputBytesInit() {
        return (this.inputBytes) != null;
    }

    public void initInputBytes(Bytes inputBytes) {
        this.inputBytes = inputBytes;
        inputStore.setBytes(inputBytes);
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
        this.closeInputKeyOffsets();
        this.closeFirstInputValueOffsets();
        this.inputFirstValueBytesValue.closeInputFirstValueBytesDataGetUsingDependants();
        this.inputKeyBytesValue.closeInputKeyBytesDataGetUsingDependants();
        this.closeSecondInputValueOffsets();
        this.inputSecondValueBytesValue.closeInputSecondValueBytesDataGetUsingDependants();
    }

    public long inputKeySize = -1;

    public long inputKeyOffset;

    public boolean inputKeyOffsetsInit() {
        return (this.inputKeySize) >= 0;
    }

    private void initInputKeyOffsets() {
        inputKeySize = this.h().keySizeMarshaller.readSize(inputBytes());
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
        this.closeFirstInputValueOffsets();
        this.inputKeyBytesValue.closeInputKeyBytesDataSizeDependants();
        this.inputKeyBytesValue.closeInputKeyBytesDataGetUsingDependants();
    }

    public long firstInputValueSize = -1;

    public long firstInputValueOffset;

    public boolean firstInputValueOffsetsInit() {
        return (this.firstInputValueSize) >= 0;
    }

    private void initFirstInputValueOffsets() {
        this.inputBytes().position(((this.inputKeyOffset()) + (this.inputKeySize())));
        firstInputValueSize = this.m().valueSizeMarshaller.readSize(this.inputBytes());
        firstInputValueOffset = this.inputBytes().position();
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
        this.closeSecondInputValueOffsets();
        this.inputFirstValueBytesValue.closeInputFirstValueBytesDataSizeDependants();
        this.inputFirstValueBytesValue.closeInputFirstValueBytesDataGetUsingDependants();
    }

    public long secondInputValueSize = -1;

    public long secondInputValueOffset;

    public boolean secondInputValueOffsetsInit() {
        return (this.secondInputValueSize) >= 0;
    }

    private void initSecondInputValueOffsets() {
        this.inputBytes().position(((firstInputValueOffset()) + (firstInputValueSize())));
        secondInputValueSize = this.m().valueSizeMarshaller.readSize(this.inputBytes());
        secondInputValueOffset = this.inputBytes().position();
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
        this.inputSecondValueBytesValue.closeInputSecondValueBytesDataSizeDependants();
        this.inputSecondValueBytesValue.closeInputSecondValueBytesDataGetUsingDependants();
    }
}
