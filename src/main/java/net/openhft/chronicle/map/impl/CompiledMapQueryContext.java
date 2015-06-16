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

package net.openhft.chronicle.map.impl;

import net.openhft.chronicle.algo.hashing.LongHashFunction;
import net.openhft.chronicle.bytes.Access;
import net.openhft.chronicle.bytes.Accessor;
import net.openhft.chronicle.bytes.Accessor.Full;
import net.openhft.chronicle.bytes.ReadAccess;
import net.openhft.chronicle.hash.AbstractData;
import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.HashEntry;
import net.openhft.chronicle.hash.impl.*;
import net.openhft.chronicle.hash.impl.hashlookup.EntryConsumer;
import net.openhft.chronicle.hash.impl.value.instance.KeyInitableData;
import net.openhft.chronicle.hash.locks.InterProcessLock;
import net.openhft.chronicle.hash.locks.InterProcessReadWriteUpdateLock;
import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.chronicle.hash.serialization.internal.MetaBytesInterop;
import net.openhft.chronicle.map.*;
import net.openhft.chronicle.map.impl.ret.InstanceReturnValue;
import net.openhft.chronicle.map.impl.ret.UsableReturnValue;
import net.openhft.chronicle.map.impl.value.instance.ValueInitableData;
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

import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class CompiledMapQueryContext<K, KI, MKI extends MetaBytesInterop<K, ? super KI>, V, VI, MVI extends MetaBytesInterop<V, ? super VI>, R, T> implements AutoCloseable , HashEntry<K> , InterProcessReadWriteUpdateLock , ExternalMapQueryContext<K, V, R> , MapAbsentEntry<K, V> , MapContext<K, V, R> , MapEntry<K, V> , MapAbsentEntryHolder<K, V> , QueryContextInterface<K, V, R> , VanillaChronicleMapHolder<K, KI, MKI, V, VI, MVI, R> {
    public void close() {
        CompiledMapQueryContext.this.closeKeyOffset();
        CompiledMapQueryContext.this.closeInputKey();
        CompiledMapQueryContext.this.wrappedValueInstanceValue.closeNext();
        CompiledMapQueryContext.this.wrappedValueInstanceValue.closeValue();
        CompiledMapQueryContext.this.closeInputBytes();
        CompiledMapQueryContext.this.defaultReturnValue.closeDefaultReturnedValue();
        CompiledMapQueryContext.this.closePos();
        CompiledMapQueryContext.this.closeKeySize();
        CompiledMapQueryContext.this.inputValueInstanceValue.closeValue();
        CompiledMapQueryContext.this.usingReturnValue.closeUsingReturnValue();
        CompiledMapQueryContext.this.inputKeyInstanceValue.closeKey();
        CompiledMapQueryContext.this.closeAllocatedChunks();
        CompiledMapQueryContext.this.closeUsed();
        CompiledMapQueryContext.this.closeVanillaChronicleMapHolderImplContextAtIndexInChainDependants();
        CompiledMapQueryContext.this.closeValueBytesInteropValueMetaInteropDependants();
        CompiledMapQueryContext.this.closeMapEntryStagesEntryBytesAccessOffsetDependants();
        CompiledMapQueryContext.this.closeKeyBytesInteropKeyMetaInteropDependants();
        CompiledMapQueryContext.this.closeOwnerThreadHolderCheckAccessingFromOwnerThreadDependants();
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

    public CompiledMapQueryContext(VanillaChronicleMap<K, KI, MKI, V, VI, MVI, R> m) {
        contextChain = new ArrayList<CompiledMapQueryContext>();
        contextChain.add(this);
        indexInContextChain = 0;
        this.m = m;
        this.deprecatedMapKeyContext = new DeprecatedMapKeyContextOnQuery();
        this.entryKey = new EntryKeyBytesData();
        this.inputValueInstanceValue = new InputValueInstanceData();
        this.inputFirstValueBytesValue = new InputFirstValueBytesData();
        this.innerWriteLock = new WriteLock();
        this.copies = ThreadLocalCopies.get();
        this.valueInterop = CompiledMapQueryContext.this.m().valueInteropProvider.get(CompiledMapQueryContext.this.copies, CompiledMapQueryContext.this.m().originalValueInterop);
        this.valueReader = CompiledMapQueryContext.this.m().valueReaderProvider.get(CompiledMapQueryContext.this.copies, CompiledMapQueryContext.this.m().originalValueReader);
        this.keyInterop = CompiledMapQueryContext.this.h().keyInteropProvider.get(CompiledMapQueryContext.this.copies, CompiledMapQueryContext.this.h().originalKeyInterop);
        this.keyReader = CompiledMapQueryContext.this.h().keyReaderProvider.get(CompiledMapQueryContext.this.copies, CompiledMapQueryContext.this.h().originalKeyReader);
        this.usingReturnValue = new UsingReturnValue();
        this.inputKeyBytesValue = new InputKeyBytesData();
        this.defaultReturnValue = new DefaultReturnValue();
        this.inputSecondValueBytesValue = new InputSecondValueBytesData();
        this.innerUpdateLock = new UpdateLock();
        this.entryBytes = CompiledMapQueryContext.this.h().ms.bytes();
        this.entryBytesAccessor = JavaLangBytesAccessors.uncheckedBytesAccessor(entryBytes);
        this.entryBytesAccessHandle = ((T)(entryBytesAccessor.handle(entryBytes)));
        this.entryBytesAccess = ((Access<T>)(entryBytesAccessor.access(entryBytes)));
        this.innerReadLock = new ReadLock();
        this.wrappedValueInstanceValue = new WrappedValueInstanceData();
        this.entryValue = new EntryValueBytesData();
        this.owner = Thread.currentThread();
        this.inputKeyInstanceValue = new InputKeyInstanceData();
        this.deprecatedMapAcquireContext = new DeprecatedMapAcquireContextOnQuery();
    }

    public CompiledMapQueryContext(CompiledMapQueryContext c) {
        contextChain = c.contextChain;
        indexInContextChain = contextChain.size();
        contextChain.add(this);
        this.m = ((VanillaChronicleMap<K, KI, MKI, V, VI, MVI, R>)(c.m));
        this.deprecatedMapKeyContext = new DeprecatedMapKeyContextOnQuery();
        this.entryKey = new EntryKeyBytesData();
        this.inputValueInstanceValue = new InputValueInstanceData();
        this.inputFirstValueBytesValue = new InputFirstValueBytesData();
        this.innerWriteLock = new WriteLock();
        this.copies = ThreadLocalCopies.get();
        this.valueInterop = CompiledMapQueryContext.this.m().valueInteropProvider.get(CompiledMapQueryContext.this.copies, CompiledMapQueryContext.this.m().originalValueInterop);
        this.valueReader = CompiledMapQueryContext.this.m().valueReaderProvider.get(CompiledMapQueryContext.this.copies, CompiledMapQueryContext.this.m().originalValueReader);
        this.keyInterop = CompiledMapQueryContext.this.h().keyInteropProvider.get(CompiledMapQueryContext.this.copies, CompiledMapQueryContext.this.h().originalKeyInterop);
        this.keyReader = CompiledMapQueryContext.this.h().keyReaderProvider.get(CompiledMapQueryContext.this.copies, CompiledMapQueryContext.this.h().originalKeyReader);
        this.usingReturnValue = new UsingReturnValue();
        this.inputKeyBytesValue = new InputKeyBytesData();
        this.defaultReturnValue = new DefaultReturnValue();
        this.inputSecondValueBytesValue = new InputSecondValueBytesData();
        this.innerUpdateLock = new UpdateLock();
        this.entryBytes = CompiledMapQueryContext.this.h().ms.bytes();
        this.entryBytesAccessor = JavaLangBytesAccessors.uncheckedBytesAccessor(entryBytes);
        this.entryBytesAccessHandle = ((T)(entryBytesAccessor.handle(entryBytes)));
        this.entryBytesAccess = ((Access<T>)(entryBytesAccessor.access(entryBytes)));
        this.innerReadLock = new ReadLock();
        this.wrappedValueInstanceValue = new WrappedValueInstanceData();
        this.entryValue = new EntryValueBytesData();
        this.owner = Thread.currentThread();
        this.inputKeyInstanceValue = new InputKeyInstanceData();
        this.deprecatedMapAcquireContext = new DeprecatedMapAcquireContextOnQuery();
    }

    public class DeprecatedMapAcquireContextOnQuery implements MapKeyContext<K, V> {
        @NotNull
        @Override
        public InterProcessLock writeLock() {
            return CompiledMapQueryContext.this.writeLock();
        }

        @NotNull
        @Override
        public InterProcessLock updateLock() {
            return CompiledMapQueryContext.this.updateLock();
        }

        @NotNull
        @Override
        public InterProcessLock readLock() {
            return CompiledMapQueryContext.this.readLock();
        }

        @NotNull
        @Override
        public Bytes entry() {
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            return CompiledMapQueryContext.this.entryBytes;
        }

        @NotNull
        @Override
        public K key() {
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            return CompiledMapQueryContext.this.queriedKey().get();
        }

        @Override
        public long keySize() {
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            return CompiledMapQueryContext.this.keySize();
        }

        @Override
        public boolean valueEqualTo(V value) {
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            return Data.bytesEquivalent(CompiledMapQueryContext.this.entryValue, CompiledMapQueryContext.this.wrapValueAsValue(value));
        }

        @Override
        public boolean containsKey() {
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            return (CompiledMapQueryContext.this.entry()) != null;
        }

        @Override
        public V getUsing(V usingValue) {
            return containsKey() ? CompiledMapQueryContext.this.value().getUsing(usingValue) : null;
        }

        @Override
        public V get() {
            assert containsKey();
            return CompiledMapQueryContext.this.usingReturnValue.returnValue();
        }

        @Override
        public boolean put(V newValue) {
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            updateLock().lock();
            MapEntry<K, V> entry = CompiledMapQueryContext.this.entry();
            if (entry != null) {
                CompiledMapQueryContext.this.replaceValue(entry, CompiledMapQueryContext.this.wrapValueAsValue(newValue));
            } else {
                CompiledMapQueryContext.this.insert(CompiledMapQueryContext.this.absentEntry(), CompiledMapQueryContext.this.wrapValueAsValue(newValue));
            }
            return true;
        }

        @Override
        public void close() {
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            put(CompiledMapQueryContext.this.usingReturnValue.returnValue());
            CompiledMapQueryContext.this.close();
        }

        @Override
        public boolean remove() {
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            updateLock().lock();
            MapEntry<K, V> entry = CompiledMapQueryContext.this.entry();
            if (entry != null) {
                CompiledMapQueryContext.this.remove(entry);
                return true;
            } else {
                return false;
            }
        }

        @Override
        public long valueOffset() {
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            return CompiledMapQueryContext.this.valueOffset();
        }

        @Override
        public long valueSize() {
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            return CompiledMapQueryContext.this.valueSize();
        }

        @Override
        public long keyOffset() {
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            return CompiledMapQueryContext.this.keyOffset();
        }
    }

    public class DeprecatedMapKeyContextOnQuery implements MapKeyContext<K, V> {
        @NotNull
        @Override
        public InterProcessLock writeLock() {
            return CompiledMapQueryContext.this.writeLock();
        }

        @NotNull
        @Override
        public InterProcessLock updateLock() {
            return CompiledMapQueryContext.this.updateLock();
        }

        @NotNull
        @Override
        public InterProcessLock readLock() {
            return CompiledMapQueryContext.this.readLock();
        }

        @NotNull
        @Override
        public Bytes entry() {
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            return CompiledMapQueryContext.this.entryBytes;
        }

        @NotNull
        @Override
        public K key() {
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            return CompiledMapQueryContext.this.queriedKey().get();
        }

        @Override
        public void close() {
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            CompiledMapQueryContext.this.close();
        }

        @Override
        public long keySize() {
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            return CompiledMapQueryContext.this.keySize();
        }

        @Override
        public boolean valueEqualTo(V value) {
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            return Data.bytesEquivalent(CompiledMapQueryContext.this.entryValue, CompiledMapQueryContext.this.wrapValueAsValue(value));
        }

        @Override
        public boolean put(V newValue) {
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            updateLock().lock();
            MapEntry<K, V> entry = CompiledMapQueryContext.this.entry();
            if (entry != null) {
                CompiledMapQueryContext.this.replaceValue(entry, CompiledMapQueryContext.this.wrapValueAsValue(newValue));
            } else {
                CompiledMapQueryContext.this.insert(CompiledMapQueryContext.this.absentEntry(), CompiledMapQueryContext.this.wrapValueAsValue(newValue));
            }
            return true;
        }

        @Override
        public boolean remove() {
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            updateLock().lock();
            MapEntry<K, V> entry = CompiledMapQueryContext.this.entry();
            if (entry != null) {
                CompiledMapQueryContext.this.remove(entry);
                return true;
            } else {
                return false;
            }
        }

        @Override
        public boolean containsKey() {
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            return (CompiledMapQueryContext.this.entry()) != null;
        }

        @Override
        public V get() {
            return containsKey() ? CompiledMapQueryContext.this.value().get() : null;
        }

        @Override
        public V getUsing(V usingValue) {
            return containsKey() ? CompiledMapQueryContext.this.value().getUsing(usingValue) : null;
        }

        @Override
        public long valueOffset() {
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            return CompiledMapQueryContext.this.valueOffset();
        }

        @Override
        public long valueSize() {
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            return CompiledMapQueryContext.this.valueSize();
        }

        @Override
        public long keyOffset() {
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            return CompiledMapQueryContext.this.keyOffset();
        }
    }

    public class EntryKeyBytesData extends AbstractData<K, T> {
        @Override
        public long size() {
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            return CompiledMapQueryContext.this.keySize();
        }

        public void closeEntryKeyBytesDataSizeDependants() {
            EntryKeyBytesData.this.closeEntryKeyBytesDataInnerGetUsingDependants();
        }

        @Override
        public ReadAccess<T> access() {
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            return CompiledMapQueryContext.this.entryBytesAccess;
        }

        @Override
        public T handle() {
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            return CompiledMapQueryContext.this.entryBytesAccessHandle;
        }

        @Override
        public long offset() {
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            return CompiledMapQueryContext.this.entryBytesAccessOffset(CompiledMapQueryContext.this.keyOffset());
        }

        private K innerGetUsing(K usingKey) {
            CompiledMapQueryContext.this.entryBytes.position(CompiledMapQueryContext.this.keyOffset());
            return CompiledMapQueryContext.this.keyReader.read(CompiledMapQueryContext.this.entryBytes, size(), usingKey);
        }

        public void closeEntryKeyBytesDataInnerGetUsingDependants() {
            EntryKeyBytesData.this.closeCachedEntryKey();
        }

        @Override
        public K getUsing(K usingKey) {
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
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
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            return cachedEntryKey();
        }
    }

    public class EntryValueBytesData extends AbstractData<V, T> {
        @Override
        public ReadAccess<T> access() {
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            return CompiledMapQueryContext.this.entryBytesAccess;
        }

        @Override
        public T handle() {
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            return CompiledMapQueryContext.this.entryBytesAccessHandle;
        }

        @Override
        public long offset() {
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            return CompiledMapQueryContext.this.entryBytesAccessOffset(CompiledMapQueryContext.this.valueOffset());
        }

        @Override
        public long size() {
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            return CompiledMapQueryContext.this.valueSize();
        }

        public void closeEntryValueBytesDataSizeDependants() {
            EntryValueBytesData.this.closeEntryValueBytesDataInnerGetUsingDependants();
        }

        private V innerGetUsing(V usingValue) {
            CompiledMapQueryContext.this.entryBytes.position(CompiledMapQueryContext.this.valueOffset());
            return CompiledMapQueryContext.this.valueReader.read(CompiledMapQueryContext.this.entryBytes, size(), usingValue);
        }

        public void closeEntryValueBytesDataInnerGetUsingDependants() {
            EntryValueBytesData.this.closeCachedEntryValue();
        }

        @Override
        public V getUsing(V usingValue) {
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
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
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            return cachedEntryValue();
        }
    }

    public class InputFirstValueBytesData extends AbstractData<V, T> {
        @Override
        public long size() {
            return CompiledMapQueryContext.this.firstInputValueSize();
        }

        public void closeInputFirstValueBytesDataSizeDependants() {
            InputFirstValueBytesData.this.closeInputFirstValueBytesDataGetUsingDependants();
        }

        @Override
        public V getUsing(V usingValue) {
            CompiledMapQueryContext.this.inputBytes().position(CompiledMapQueryContext.this.firstInputValueOffset());
            return CompiledMapQueryContext.this.valueReader.read(CompiledMapQueryContext.this.inputBytes(), size(), usingValue);
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
        public T handle() {
            return ((T)(JavaLangBytesAccessors.uncheckedBytesAccessor(CompiledMapQueryContext.this.inputBytes()).handle(CompiledMapQueryContext.this.inputBytes())));
        }

        @Override
        public long offset() {
            return JavaLangBytesAccessors.uncheckedBytesAccessor(CompiledMapQueryContext.this.inputBytes()).offset(CompiledMapQueryContext.this.inputBytes(), CompiledMapQueryContext.this.firstInputValueOffset());
        }

        @Override
        public ReadAccess<T> access() {
            return ((ReadAccess<T>)(JavaLangBytesAccessors.uncheckedBytesAccessor(CompiledMapQueryContext.this.inputBytes()).access(CompiledMapQueryContext.this.inputBytes())));
        }
    }

    public class InputKeyBytesData extends AbstractData<K, T> {
        @Override
        public long size() {
            return CompiledMapQueryContext.this.inputKeySize();
        }

        public void closeInputKeyBytesDataSizeDependants() {
            InputKeyBytesData.this.closeInputKeyBytesDataGetUsingDependants();
        }

        @Override
        public K getUsing(K usingKey) {
            Bytes inputBytes = CompiledMapQueryContext.this.inputBytes();
            inputBytes.position(CompiledMapQueryContext.this.inputKeyOffset());
            return CompiledMapQueryContext.this.keyReader.read(inputBytes, size(), usingKey);
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
        public long offset() {
            return JavaLangBytesAccessors.uncheckedBytesAccessor(CompiledMapQueryContext.this.inputBytes()).offset(CompiledMapQueryContext.this.inputBytes(), CompiledMapQueryContext.this.inputKeyOffset());
        }

        @Override
        public ReadAccess<T> access() {
            return ((ReadAccess<T>)(JavaLangBytesAccessors.uncheckedBytesAccessor(CompiledMapQueryContext.this.inputBytes()).access(CompiledMapQueryContext.this.inputBytes())));
        }

        @Override
        public T handle() {
            return ((T)(JavaLangBytesAccessors.uncheckedBytesAccessor(CompiledMapQueryContext.this.inputBytes()).handle(CompiledMapQueryContext.this.inputBytes())));
        }
    }

    public class InputSecondValueBytesData extends AbstractData<V, T> {
        @Override
        public T handle() {
            return ((T)(JavaLangBytesAccessors.uncheckedBytesAccessor(CompiledMapQueryContext.this.inputBytes()).handle(CompiledMapQueryContext.this.inputBytes())));
        }

        @Override
        public long size() {
            return CompiledMapQueryContext.this.secondInputValueSize();
        }

        public void closeInputSecondValueBytesDataSizeDependants() {
            InputSecondValueBytesData.this.closeInputSecondValueBytesDataGetUsingDependants();
        }

        @Override
        public long offset() {
            return JavaLangBytesAccessors.uncheckedBytesAccessor(CompiledMapQueryContext.this.inputBytes()).offset(CompiledMapQueryContext.this.inputBytes(), CompiledMapQueryContext.this.secondInputValueOffset());
        }

        @Override
        public ReadAccess<T> access() {
            return ((ReadAccess<T>)(JavaLangBytesAccessors.uncheckedBytesAccessor(CompiledMapQueryContext.this.inputBytes()).access(CompiledMapQueryContext.this.inputBytes())));
        }

        @Override
        public V getUsing(V usingValue) {
            CompiledMapQueryContext.this.inputBytes().position(CompiledMapQueryContext.this.secondInputValueOffset());
            return CompiledMapQueryContext.this.valueReader.read(CompiledMapQueryContext.this.inputBytes(), size(), usingValue);
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
        public boolean isHeldByCurrentThread() {
            return CompiledMapQueryContext.this.localLockState().read;
        }

        @Override
        public void lock() {
            if ((CompiledMapQueryContext.this.localLockState()) == (LocalLockState.UNLOCKED)) {
                CompiledMapQueryContext.this.segmentHeader().readLock(CompiledMapQueryContext.this.segmentHeaderAddress());
                CompiledMapQueryContext.this.setLocalLockStateGuarded(LocalLockState.READ_LOCKED);
            }
        }

        public void closeReadLockLockDependants() {
            CompiledMapQueryContext.this.closeHashLookupPos();
        }

        @Override
        public boolean tryLock(long time, @NotNull
        TimeUnit unit) throws InterruptedException {
            if ((CompiledMapQueryContext.this.localLockState()) == (LocalLockState.UNLOCKED)) {
                if (CompiledMapQueryContext.this.segmentHeader().tryReadLock(CompiledMapQueryContext.this.segmentHeaderAddress(), time, unit)) {
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
        public void unlock() {
            switch (CompiledMapQueryContext.this.localLockState()) {
                case UNLOCKED :
                    return ;
                case READ_LOCKED :
                    CompiledMapQueryContext.this.segmentHeader().readUnlock(CompiledMapQueryContext.this.segmentHeaderAddress());
                    break;
                case UPDATE_LOCKED :
                    CompiledMapQueryContext.this.segmentHeader().updateUnlock(CompiledMapQueryContext.this.segmentHeaderAddress());
                    break;
                case WRITE_LOCKED :
                    CompiledMapQueryContext.this.segmentHeader().writeUnlock(CompiledMapQueryContext.this.segmentHeaderAddress());
            }
            CompiledMapQueryContext.this.setLocalLockStateGuarded(LocalLockState.UNLOCKED);
            CompiledMapQueryContext.this.closeHashLookupPos();
            CompiledMapQueryContext.this.closePos();
        }

        @Override
        public boolean tryLock() {
            if ((CompiledMapQueryContext.this.localLockState()) == (LocalLockState.UNLOCKED)) {
                if (CompiledMapQueryContext.this.segmentHeader().tryReadLock(CompiledMapQueryContext.this.segmentHeaderAddress())) {
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
        public void lockInterruptibly() throws InterruptedException {
            if ((CompiledMapQueryContext.this.localLockState()) == (LocalLockState.UNLOCKED)) {
                CompiledMapQueryContext.this.segmentHeader().readLockInterruptibly(CompiledMapQueryContext.this.segmentHeaderAddress());
                CompiledMapQueryContext.this.setLocalLockStateGuarded(LocalLockState.READ_LOCKED);
            }
        }
    }

    public class UpdateLock implements InterProcessLock {
        @NotNull
        private IllegalMonitorStateException forbiddenUpgrade() {
            return new IllegalMonitorStateException("Cannot upgrade from read to update lock");
        }

        @Override
        public void lockInterruptibly() throws InterruptedException {
            switch (CompiledMapQueryContext.this.localLockState()) {
                case UNLOCKED :
                    CompiledMapQueryContext.this.segmentHeader().updateLockInterruptibly(CompiledMapQueryContext.this.segmentHeaderAddress());
                    CompiledMapQueryContext.this.setLocalLockStateGuarded(LocalLockState.UPDATE_LOCKED);
                    return ;
                case READ_LOCKED :
                    throw forbiddenUpgrade();
                case UPDATE_LOCKED :
                case WRITE_LOCKED :
            }
        }

        @Override
        public boolean isHeldByCurrentThread() {
            return CompiledMapQueryContext.this.localLockState().update;
        }

        @Override
        public void lock() {
            switch (CompiledMapQueryContext.this.localLockState()) {
                case UNLOCKED :
                    CompiledMapQueryContext.this.segmentHeader().updateLock(CompiledMapQueryContext.this.segmentHeaderAddress());
                    CompiledMapQueryContext.this.setLocalLockStateGuarded(LocalLockState.UPDATE_LOCKED);
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
            switch (CompiledMapQueryContext.this.localLockState()) {
                case UNLOCKED :
                    if (CompiledMapQueryContext.this.segmentHeader().tryUpdateLock(CompiledMapQueryContext.this.segmentHeaderAddress(), time, unit)) {
                        CompiledMapQueryContext.this.setLocalLockStateGuarded(LocalLockState.UPDATE_LOCKED);
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
            switch (CompiledMapQueryContext.this.localLockState()) {
                case UNLOCKED :
                case READ_LOCKED :
                    return ;
                case UPDATE_LOCKED :
                    CompiledMapQueryContext.this.segmentHeader().downgradeUpdateToReadLock(CompiledMapQueryContext.this.segmentHeaderAddress());
                    break;
                case WRITE_LOCKED :
                    CompiledMapQueryContext.this.segmentHeader().downgradeWriteToReadLock(CompiledMapQueryContext.this.segmentHeaderAddress());
            }
            CompiledMapQueryContext.this.setLocalLockStateGuarded(LocalLockState.READ_LOCKED);
        }

        @Override
        public boolean tryLock() {
            switch (CompiledMapQueryContext.this.localLockState()) {
                case UNLOCKED :
                    if (CompiledMapQueryContext.this.segmentHeader().tryUpdateLock(CompiledMapQueryContext.this.segmentHeaderAddress())) {
                        CompiledMapQueryContext.this.setLocalLockStateGuarded(LocalLockState.UPDATE_LOCKED);
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
            CompiledMapQueryContext.this.m().checkValue(value);
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
            MVI mvi = CompiledMapQueryContext.this.valueMetaInterop(value());
            long size = mvi.size(CompiledMapQueryContext.this.valueInterop, value());
            buf = CopyingInstanceData.getBuffer(this.buf, size);
            mvi.write(CompiledMapQueryContext.this.valueInterop, buf, value());
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
            return CompiledMapQueryContext.this.valueReader.read(buf(), buf().limit(), usingValue);
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
        public void lock() {
            switch (CompiledMapQueryContext.this.localLockState()) {
                case UNLOCKED :
                    CompiledMapQueryContext.this.segmentHeader().writeLock(CompiledMapQueryContext.this.segmentHeaderAddress());
                    CompiledMapQueryContext.this.setLocalLockStateGuarded(LocalLockState.WRITE_LOCKED);
                    return ;
                case READ_LOCKED :
                    throw forbiddenUpgrade();
                case UPDATE_LOCKED :
                    CompiledMapQueryContext.this.segmentHeader().upgradeUpdateToWriteLock(CompiledMapQueryContext.this.segmentHeaderAddress());
                    CompiledMapQueryContext.this.setLocalLockStateGuarded(LocalLockState.WRITE_LOCKED);
                case WRITE_LOCKED :
            }
        }

        @Override
        public boolean isHeldByCurrentThread() {
            return CompiledMapQueryContext.this.localLockState().write;
        }

        @Override
        public void lockInterruptibly() throws InterruptedException {
            switch (CompiledMapQueryContext.this.localLockState()) {
                case UNLOCKED :
                    CompiledMapQueryContext.this.segmentHeader().writeLockInterruptibly(CompiledMapQueryContext.this.segmentHeaderAddress());
                    CompiledMapQueryContext.this.setLocalLockStateGuarded(LocalLockState.WRITE_LOCKED);
                    return ;
                case READ_LOCKED :
                    throw forbiddenUpgrade();
                case UPDATE_LOCKED :
                    CompiledMapQueryContext.this.segmentHeader().upgradeUpdateToWriteLockInterruptibly(CompiledMapQueryContext.this.segmentHeaderAddress());
                    CompiledMapQueryContext.this.setLocalLockStateGuarded(LocalLockState.WRITE_LOCKED);
                case WRITE_LOCKED :
            }
        }

        @Override
        public boolean tryLock() {
            switch (CompiledMapQueryContext.this.localLockState()) {
                case UNLOCKED :
                    if (CompiledMapQueryContext.this.segmentHeader().tryWriteLock(CompiledMapQueryContext.this.segmentHeaderAddress())) {
                        CompiledMapQueryContext.this.setLocalLockStateGuarded(LocalLockState.WRITE_LOCKED);
                        return true;
                    } else {
                        return false;
                    }
                case READ_LOCKED :
                    throw forbiddenUpgrade();
                case UPDATE_LOCKED :
                    if (CompiledMapQueryContext.this.segmentHeader().tryUpgradeUpdateToWriteLock(CompiledMapQueryContext.this.segmentHeaderAddress())) {
                        CompiledMapQueryContext.this.setLocalLockStateGuarded(LocalLockState.WRITE_LOCKED);
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
        public boolean tryLock(long time, @NotNull
        TimeUnit unit) throws InterruptedException {
            switch (CompiledMapQueryContext.this.localLockState()) {
                case UNLOCKED :
                    if (CompiledMapQueryContext.this.segmentHeader().tryWriteLock(CompiledMapQueryContext.this.segmentHeaderAddress(), time, unit)) {
                        CompiledMapQueryContext.this.setLocalLockStateGuarded(LocalLockState.WRITE_LOCKED);
                        return true;
                    } else {
                        return false;
                    }
                case READ_LOCKED :
                    throw forbiddenUpgrade();
                case UPDATE_LOCKED :
                    if (CompiledMapQueryContext.this.segmentHeader().tryUpgradeUpdateToWriteLock(CompiledMapQueryContext.this.segmentHeaderAddress(), time, unit)) {
                        CompiledMapQueryContext.this.setLocalLockStateGuarded(LocalLockState.WRITE_LOCKED);
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
            switch (CompiledMapQueryContext.this.localLockState()) {
                case UNLOCKED :
                case READ_LOCKED :
                case UPDATE_LOCKED :
                    return ;
                case WRITE_LOCKED :
                    CompiledMapQueryContext.this.segmentHeader().downgradeWriteToUpdateLock(CompiledMapQueryContext.this.segmentHeaderAddress());
            }
            CompiledMapQueryContext.this.setLocalLockStateGuarded(LocalLockState.UPDATE_LOCKED);
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
        this.keyMask = CompiledMapQueryContext.mask(keyBits);
        this.valueMask = CompiledMapQueryContext.mask(valueBits);
        this.entryMask = CompiledMapQueryContext.mask((keyBits + valueBits));
    }

    private void unlinkFromSegmentContextsChain() {
        CompiledMapQueryContext prevContext = this.rootContextOnThisSegment;
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
            MKI mki = CompiledMapQueryContext.this.keyMetaInterop(key());
            long size = mki.size(CompiledMapQueryContext.this.keyInterop, key());
            buffer = CopyingInstanceData.getBuffer(this.buffer, size);
            mki.write(CompiledMapQueryContext.this.keyInterop, buffer, key());
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
            return CompiledMapQueryContext.this.keyReader.read(buffer(), buffer().limit(), usingKey);
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
            MVI mvi = CompiledMapQueryContext.this.valueMetaInterop(value());
            long size = mvi.size(CompiledMapQueryContext.this.valueInterop, value());
            buffer = CopyingInstanceData.getBuffer(this.buffer, size);
            mvi.write(CompiledMapQueryContext.this.valueInterop, buffer, value());
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
            return CompiledMapQueryContext.this.valueReader.read(buffer(), buffer().limit(), usingValue);
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

    public final WriteLock innerWriteLock;

    public WriteLock innerWriteLock() {
        return this.innerWriteLock;
    }

    public final List<CompiledMapQueryContext> contextChain;

    public final UpdateLock innerUpdateLock;

    public UpdateLock innerUpdateLock() {
        return this.innerUpdateLock;
    }

    public List<CompiledMapQueryContext> contextChain() {
        return this.contextChain;
    }

    public final ThreadLocalCopies copies;

    public ThreadLocalCopies copies() {
        return this.copies;
    }

    private void countValueOffset() {
        CompiledMapQueryContext.this.m().alignment.alignPositionAddr(entryBytes);
        valueOffset = entryBytes.position();
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
        return (((CompiledMapQueryContext.this.m().metaDataBytes) + (CompiledMapQueryContext.this.m().keySizeMarshaller.sizeEncodingSize(keySize))) + keySize) + (CompiledMapQueryContext.this.m().valueSizeMarshaller.sizeEncodingSize(valueSize));
    }

    public void checkAccessingFromOwnerThread() {
        if ((owner) != (Thread.currentThread())) {
            throw new ConcurrentModificationException("Context shouldn\'t be accessed from multiple threads");
        }
    }

    public void closeOwnerThreadHolderCheckAccessingFromOwnerThreadDependants() {
        CompiledMapQueryContext.this.closeQueryCheckOnEachPublicOperationCheckOnEachPublicOperationDependants();
    }

    public MKI keyMetaInterop(K key) {
        return CompiledMapQueryContext.this.h().metaKeyInteropProvider.get(CompiledMapQueryContext.this.copies, CompiledMapQueryContext.this.h().originalMetaKeyInterop, keyInterop, key);
    }

    public void closeKeyBytesInteropKeyMetaInteropDependants() {
        CompiledMapQueryContext.this.inputKeyInstanceValue.closeBuffer();
    }

    public long entryBytesAccessOffset(long offset) {
        return entryBytesAccessor.offset(entryBytes, offset);
    }

    public void closeMapEntryStagesEntryBytesAccessOffsetDependants() {
        CompiledMapQueryContext.this.closeMapQueryKeyEqualsDependants();
    }

    @Override
    public MapAbsentEntry<K, V> absent() {
        return CompiledMapQueryContext.this;
    }

    public long innerEntrySize(long sizeOfEverythingBeforeValue, long valueSize) {
        if (CompiledMapQueryContext.this.m().constantlySizedEntry) {
            return CompiledMapQueryContext.this.m().alignment.alignAddr((sizeOfEverythingBeforeValue + valueSize));
        } else if (CompiledMapQueryContext.this.m().couldNotDetermineAlignmentBeforeAllocation) {
            return (sizeOfEverythingBeforeValue + (CompiledMapQueryContext.this.m().worstAlignment)) + valueSize;
        } else {
            return (CompiledMapQueryContext.this.m().alignment.alignAddr(sizeOfEverythingBeforeValue)) + valueSize;
        }
    }

    public final long entrySize(long keySize, long valueSize) {
        long sizeOfEverythingBeforeValue = sizeOfEverythingBeforeValue(keySize, valueSize);
        return innerEntrySize(sizeOfEverythingBeforeValue, valueSize);
    }

    @Override
    public VanillaChronicleMap<K, KI, MKI, V, VI, MVI, R> m() {
        return m;
    }

    @Override
    public ChronicleMap<K, V> map() {
        return m();
    }

    private CompiledMapQueryContext _Chaining_createChaining() {
        return new CompiledMapQueryContext(this);
    }

    public CompiledMapQueryContext createChaining() {
        return new CompiledMapQueryContext(this);
    }

    public <T>T getContext() {
        for (CompiledMapQueryContext context : contextChain) {
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

    public MVI valueMetaInterop(V value) {
        return CompiledMapQueryContext.this.m().metaValueInteropProvider.get(CompiledMapQueryContext.this.copies, CompiledMapQueryContext.this.m().originalMetaValueInterop, valueInterop, value);
    }

    public void closeValueBytesInteropValueMetaInteropDependants() {
        CompiledMapQueryContext.this.wrappedValueInstanceValue.closeBuffer();
        CompiledMapQueryContext.this.inputValueInstanceValue.closeBuffer();
    }

    public <T>T contextAtIndexInChain(int index) {
        return ((T)(contextChain.get(index)));
    }

    public void closeVanillaChronicleMapHolderImplContextAtIndexInChainDependants() {
        CompiledMapQueryContext.this.closeQuerySegmentStagesTryFindInitLocksOfThisSegmentDependants();
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
        CompiledMapQueryContext.this.closeMapQueryKeyEqualsDependants();
        CompiledMapQueryContext.this.closeMapEntryStagesKeyEndDependants();
        CompiledMapQueryContext.this.entryKey.closeEntryKeyBytesDataSizeDependants();
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
        CompiledMapQueryContext.this.closeEntryOffset();
        CompiledMapQueryContext.this.closeMapQueryDropSearchIfNestedContextsAndPresentHashLookupSlotCheckFailedDependants();
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
        CompiledMapQueryContext.this.closeInputKeyOffsets();
        CompiledMapQueryContext.this.closeFirstInputValueOffsets();
        CompiledMapQueryContext.this.inputFirstValueBytesValue.closeInputFirstValueBytesDataGetUsingDependants();
        CompiledMapQueryContext.this.closeSecondInputValueOffsets();
        CompiledMapQueryContext.this.inputKeyBytesValue.closeInputKeyBytesDataGetUsingDependants();
        CompiledMapQueryContext.this.inputSecondValueBytesValue.closeInputSecondValueBytesDataGetUsingDependants();
    }

    public long inputKeySize = -1;

    public long inputKeyOffset;

    public boolean inputKeyOffsetsInit() {
        return (this.inputKeySize) >= 0;
    }

    private void initInputKeyOffsets() {
        inputKeySize = CompiledMapQueryContext.this.h().keySizeMarshaller.readSize(inputBytes());
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
        CompiledMapQueryContext.this.inputKeyBytesValue.closeInputKeyBytesDataSizeDependants();
        CompiledMapQueryContext.this.closeFirstInputValueOffsets();
        CompiledMapQueryContext.this.inputKeyBytesValue.closeInputKeyBytesDataGetUsingDependants();
    }

    public long firstInputValueSize = -1;

    public long firstInputValueOffset;

    public boolean firstInputValueOffsetsInit() {
        return (this.firstInputValueSize) >= 0;
    }

    private void initFirstInputValueOffsets() {
        CompiledMapQueryContext.this.inputBytes().position(((CompiledMapQueryContext.this.inputKeyOffset()) + (CompiledMapQueryContext.this.inputKeySize())));
        firstInputValueSize = CompiledMapQueryContext.this.m().valueSizeMarshaller.readSize(CompiledMapQueryContext.this.inputBytes());
        firstInputValueOffset = CompiledMapQueryContext.this.inputBytes().position();
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
        CompiledMapQueryContext.this.inputFirstValueBytesValue.closeInputFirstValueBytesDataSizeDependants();
        CompiledMapQueryContext.this.inputFirstValueBytesValue.closeInputFirstValueBytesDataGetUsingDependants();
        CompiledMapQueryContext.this.closeSecondInputValueOffsets();
    }

    public long secondInputValueSize = -1;

    public long secondInputValueOffset;

    public boolean secondInputValueOffsetsInit() {
        return (this.secondInputValueSize) >= 0;
    }

    private void initSecondInputValueOffsets() {
        CompiledMapQueryContext.this.inputBytes().position(((firstInputValueOffset()) + (firstInputValueSize())));
        secondInputValueSize = CompiledMapQueryContext.this.m().valueSizeMarshaller.readSize(CompiledMapQueryContext.this.inputBytes());
        secondInputValueOffset = CompiledMapQueryContext.this.inputBytes().position();
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
        CompiledMapQueryContext.this.inputSecondValueBytesValue.closeInputSecondValueBytesDataSizeDependants();
        CompiledMapQueryContext.this.inputSecondValueBytesValue.closeInputSecondValueBytesDataGetUsingDependants();
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
        CompiledMapQueryContext.this.closeMapQueryKeyEqualsDependants();
        CompiledMapQueryContext.this.closeHashOfKey();
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
        CompiledMapQueryContext.this.closeTheSegmentIndex();
        CompiledMapQueryContext.this.closeSearchKey();
    }

    public int segmentIndex = -1;

    public boolean theSegmentIndexInit() {
        return (this.segmentIndex) >= 0;
    }

    void initTheSegmentIndex() {
        segmentIndex = CompiledMapQueryContext.this.h().hashSplitting.segmentIndex(CompiledMapQueryContext.this.hashOfKey());
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
        CompiledMapQueryContext.this.closeSegHeader();
        CompiledMapQueryContext.this.closeSegmentHashLookup();
        CompiledMapQueryContext.this.closeSegment();
    }

    long segmentHeaderAddress;

    SegmentHeader segmentHeader = null;

    public boolean segHeaderInit() {
        return (this.segmentHeader) != null;
    }

    private void initSegHeader() {
        segmentHeaderAddress = (CompiledMapQueryContext.this.h().ms.address()) + (CompiledMapQueryContext.this.h().segmentHeaderOffset(segmentIndex()));
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
        CompiledMapQueryContext.this.closeQuerySegmentStagesTryFindInitLocksOfThisSegmentDependants();
        CompiledMapQueryContext.this.closeLocks();
        CompiledMapQueryContext.this.innerReadLock.closeReadLockLockDependants();
    }

    public void entries(long size) {
        segmentHeader().size(segmentHeaderAddress(), size);
    }

    long nextPosToSearchFrom() {
        return segmentHeader().nextPosToSearchFrom(segmentHeaderAddress());
    }

    public void nextPosToSearchFrom(long nextPosToSearchFrom) {
        segmentHeader().nextPosToSearchFrom(segmentHeaderAddress(), nextPosToSearchFrom);
    }

    public void updateNextPosToSearchFrom(long allocated, int chunks) {
        long nextPosToSearchFrom = allocated + chunks;
        if (nextPosToSearchFrom >= (CompiledMapQueryContext.this.h().actualChunksPerSegment))
            nextPosToSearchFrom = 0L;

        nextPosToSearchFrom(nextPosToSearchFrom);
    }

    public long entries() {
        return segmentHeader().size(segmentHeaderAddress());
    }

    boolean tryFindInitLocksOfThisSegment(Object thisContext, int index) {
        CompiledMapQueryContext c = CompiledMapQueryContext.this.contextAtIndexInChain(index);
        if ((((c.segmentHeader()) != null) && ((c.segmentHeaderAddress()) == (segmentHeaderAddress()))) && ((c.rootContextOnThisSegment()) != null)) {
            throw new IllegalStateException("Nested context not implemented yet");
        } else {
            return false;
        }
    }

    public void closeQuerySegmentStagesTryFindInitLocksOfThisSegmentDependants() {
        CompiledMapQueryContext.this.closeLocks();
    }

    int totalReadLockCount;

    int totalUpdateLockCount;

    int totalWriteLockCount;

    public int latestSameThreadSegmentModCount;

    public int contextModCount;

    CompiledMapQueryContext nextNode;

    public boolean concurrentSameThreadContexts;

    LocalLockState localLockState;

    public CompiledMapQueryContext rootContextOnThisSegment = null;

    public boolean locksInit() {
        return (this.rootContextOnThisSegment) != null;
    }

    void initLocks() {
        localLockState = LocalLockState.UNLOCKED;
        int indexOfThisContext = CompiledMapQueryContext.this.indexInContextChain;
        for (int i = indexOfThisContext - 1 ; i >= 0 ; i--) {
            if (tryFindInitLocksOfThisSegment(this, i))
                return ;

        }
        for (int i = indexOfThisContext + 1, size = CompiledMapQueryContext.this.contextChain.size() ; i < size ; i++) {
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

    public CompiledMapQueryContext rootContextOnThisSegment() {
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
        CompiledMapQueryContext.this.innerReadLock.closeReadLockLockDependants();
        CompiledMapQueryContext.this.closeMapQueryDropSearchIfNestedContextsAndPresentHashLookupSlotCheckFailedDependants();
    }

    public long deleted() {
        return segmentHeader().deleted(segmentHeaderAddress());
    }

    public long size() {
        return (entries()) - (deleted());
    }

    public void deleted(long deleted) {
        segmentHeader().deleted(segmentHeaderAddress(), deleted);
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
        long hashLookupOffset = CompiledMapQueryContext.this.h().segmentOffset(CompiledMapQueryContext.this.segmentIndex());
        innerInitSegmentHashLookup(((CompiledMapQueryContext.this.h().ms.address()) + hashLookupOffset), CompiledMapQueryContext.this.h().segmentHashLookupCapacity, CompiledMapQueryContext.this.h().segmentHashLookupEntrySize, CompiledMapQueryContext.this.h().segmentHashLookupKeyBits, CompiledMapQueryContext.this.h().segmentHashLookupValueBits);
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
        CompiledMapQueryContext.this.closeHashLookupStepBackDependants();
        CompiledMapQueryContext.this.closeHashLookupKeyDependants();
        CompiledMapQueryContext.this.closeHashLookupReadEntryDependants();
        CompiledMapQueryContext.this.closeHashLookupMaskUnsetKeyDependants();
        CompiledMapQueryContext.this.closeHashLookupValueDependants();
        CompiledMapQueryContext.this.closeHashLookupIndexToPosDependants();
        CompiledMapQueryContext.this.closeHashLookupHlPosDependants();
        CompiledMapQueryContext.this.closeHashLookupEmptyDependants();
        CompiledMapQueryContext.this.closeHashLookupStepDependants();
    }

    public long stepBack(long pos) {
        return (pos -= hashLookupEntrySize()) >= 0 ? pos : capacityMask2();
    }

    public void closeHashLookupStepBackDependants() {
        CompiledMapQueryContext.this.closeHashLookupSearchFoundDependants();
    }

    void clearEntry(long pos, long prevEntry) {
        long entry = prevEntry & (~(entryMask()));
        NativeBytes.UNSAFE.putLong(((address()) + pos), entry);
    }

    public long key(long entry) {
        return entry & (keyMask());
    }

    public void closeHashLookupKeyDependants() {
        CompiledMapQueryContext.this.closeHashLookupSearchCheckSlotContainsExpectedKeyAndValueDependants();
        CompiledMapQueryContext.this.closeHashLookupSearchNextPosDependants();
    }

    public long readEntry(long pos) {
        return NativeBytes.UNSAFE.getLong(((address()) + pos));
    }

    public void closeHashLookupReadEntryDependants() {
        CompiledMapQueryContext.this.closeHashLookupSearchCheckSlotContainsExpectedKeyAndValueDependants();
        CompiledMapQueryContext.this.closeHashLookupSearchNextPosDependants();
    }

    public long maskUnsetKey(long key) {
        return (key &= keyMask()) != (UNSET_KEY) ? key : keyMask();
    }

    public void closeHashLookupMaskUnsetKeyDependants() {
        CompiledMapQueryContext.this.closeSearchKey();
    }

    public void checkValueForPut(long value) {
        assert (value & (~(valueMask()))) == 0L : "Value out of range, was " + value;
    }

    public long value(long entry) {
        return (entry >>> (keyBits())) & (valueMask());
    }

    public void closeHashLookupValueDependants() {
        CompiledMapQueryContext.this.closeHashLookupSearchCheckSlotContainsExpectedKeyAndValueDependants();
        CompiledMapQueryContext.this.closeHashLookupSearchNextPosDependants();
    }

    void writeEntry(long pos, long prevEntry, long anotherEntry) {
        long entry = (prevEntry & (~(entryMask()))) | (anotherEntry & (entryMask()));
        NativeBytes.UNSAFE.putLong(((address()) + pos), entry);
    }

    long indexToPos(long index) {
        return index * (hashLookupEntrySize());
    }

    public void closeHashLookupIndexToPosDependants() {
        CompiledMapQueryContext.this.closeHashLookupHlPosDependants();
    }

    public long hlPos(long key) {
        return indexToPos((key & (capacityMask())));
    }

    public void closeHashLookupHlPosDependants() {
        CompiledMapQueryContext.this.closeSearchKey();
    }

    public boolean empty(long entry) {
        return (entry & (entryMask())) == (UNSET_ENTRY);
    }

    public void closeHashLookupEmptyDependants() {
        CompiledMapQueryContext.this.closeHashLookupSearchNextPosDependants();
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
        CompiledMapQueryContext.this.closeHashLookupSearchNextPosDependants();
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

    public void clearHashLookup() {
        NativeBytes.UNSAFE.setMemory(address(), ((capacityMask2()) + (hashLookupEntrySize())), ((byte)(0)));
    }

    long entrySpaceOffset = 0;

    MultiStoreBytes freeListBytes = new MultiStoreBytes();

    public SingleThreadedDirectBitSet freeList = new SingleThreadedDirectBitSet();

    boolean segmentInit() {
        return (entrySpaceOffset) > 0;
    }

    void initSegment() {
        VanillaChronicleHash<?, ?, ?, ?, ?> h = CompiledMapQueryContext.this.h();
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
        CompiledMapQueryContext.this.closeEntryOffset();
    }

    public long keySizeOffset = -1;

    public boolean entryOffsetInit() {
        return (this.keySizeOffset) >= 0;
    }

    public void initEntryOffset() {
        keySizeOffset = (CompiledMapQueryContext.this.entrySpaceOffset()) + ((pos()) * (CompiledMapQueryContext.this.h().chunkSize));
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
        CompiledMapQueryContext.this.closeMapEntryStagesReadExistingEntryDependants();
        CompiledMapQueryContext.this.closeMapEntryStagesEntrySizeDependants();
    }

    public void readExistingEntry(long pos) {
        initPos(pos);
        entryBytes.position(keySizeOffset());
        initKeySize(CompiledMapQueryContext.this.h().keySizeMarshaller.readSize(entryBytes));
        initKeyOffset(entryBytes.position());
    }

    public void closeMapEntryStagesReadExistingEntryDependants() {
        CompiledMapQueryContext.this.closeKeySearch();
    }

    public long alloc(int chunks) {
        VanillaChronicleHash<?, ?, ?, ?, ?> h = CompiledMapQueryContext.this.h();
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

    public void clearSegment() {
        CompiledMapQueryContext.this.innerWriteLock.lock();
        CompiledMapQueryContext.this.clearHashLookup();
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
        searchKey = CompiledMapQueryContext.this.maskUnsetKey(CompiledMapQueryContext.this.h().hashSplitting.segmentHash(CompiledMapQueryContext.this.hashOfKey()));
        searchStartPos = CompiledMapQueryContext.this.hlPos(searchKey);
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
        CompiledMapQueryContext.this.closeHashLookupPos();
        CompiledMapQueryContext.this.closeHashLookupSearchCheckSlotContainsExpectedKeyAndValueDependants();
        CompiledMapQueryContext.this.closeHashLookupSearchNextPosDependants();
    }

    public long hashLookupPos = -1;

    public boolean hashLookupPosInit() {
        return (this.hashLookupPos) >= 0;
    }

    public void initHashLookupPos() {
        CompiledMapQueryContext.this.innerReadLock.lock();
        this.hashLookupPos = CompiledMapQueryContext.this.searchStartPos();
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
        CompiledMapQueryContext.this.closeHashLookupSearchFoundDependants();
        CompiledMapQueryContext.this.closeHashLookupSearchCheckSlotContainsExpectedKeyAndValueDependants();
        CompiledMapQueryContext.this.closeHashLookupSearchNextPosDependants();
    }

    public void found() {
        CompiledMapQueryContext.this.setHashLookupPosGuarded(CompiledMapQueryContext.this.stepBack(CompiledMapQueryContext.this.hashLookupPos()));
    }

    public void closeHashLookupSearchFoundDependants() {
        CompiledMapQueryContext.this.closeKeySearch();
    }

    public void putVolatile(long value) {
        CompiledMapQueryContext.this.checkValueForPut(value);
        long currentEntry = CompiledMapQueryContext.this.readEntry(CompiledMapQueryContext.this.hashLookupPos());
        assert (CompiledMapQueryContext.this.key(currentEntry)) == (searchKey());
        CompiledMapQueryContext.this.writeEntryVolatile(CompiledMapQueryContext.this.hashLookupPos(), currentEntry, searchKey(), value);
    }

    public void remove() {
        CompiledMapQueryContext.this.setHashLookupPosGuarded(CompiledMapQueryContext.this.remove(CompiledMapQueryContext.this.hashLookupPos()));
    }

    public boolean checkSlotIsEmpty() {
        return CompiledMapQueryContext.this.empty(CompiledMapQueryContext.this.readEntry(CompiledMapQueryContext.this.hashLookupPos()));
    }

    void put(long value) {
        CompiledMapQueryContext.this.checkValueForPut(value);
        CompiledMapQueryContext.this.writeEntry(CompiledMapQueryContext.this.hashLookupPos(), CompiledMapQueryContext.this.readEntry(CompiledMapQueryContext.this.hashLookupPos()), searchKey(), value);
    }

    public void putNewVolatile(long value) {
        CompiledMapQueryContext.this.checkValueForPut(value);
        long currentEntry = CompiledMapQueryContext.this.readEntry(CompiledMapQueryContext.this.hashLookupPos());
        CompiledMapQueryContext.this.writeEntryVolatile(CompiledMapQueryContext.this.hashLookupPos(), currentEntry, searchKey(), value);
    }

    public boolean checkSlotContainsExpectedKeyAndValue(long value) {
        long entry = CompiledMapQueryContext.this.readEntry(CompiledMapQueryContext.this.hashLookupPos());
        return ((CompiledMapQueryContext.this.key(entry)) == (searchKey())) && ((CompiledMapQueryContext.this.value(entry)) == value);
    }

    public void closeHashLookupSearchCheckSlotContainsExpectedKeyAndValueDependants() {
        CompiledMapQueryContext.this.closeMapQueryDropSearchIfNestedContextsAndPresentHashLookupSlotCheckFailedDependants();
    }

    public long nextPos() {
        long pos = CompiledMapQueryContext.this.hashLookupPos();
        while (true) {
            long entry = CompiledMapQueryContext.this.readEntry(pos);
            if (CompiledMapQueryContext.this.empty(entry)) {
                CompiledMapQueryContext.this.setHashLookupPosGuarded(pos);
                return -1L;
            }
            pos = CompiledMapQueryContext.this.step(pos);
            if (pos == (searchStartPos()))
                break;

            if ((CompiledMapQueryContext.this.key(entry)) == (searchKey())) {
                CompiledMapQueryContext.this.setHashLookupPosGuarded(pos);
                return CompiledMapQueryContext.this.value(entry);
            }
        }
        throw new IllegalStateException(("MultiMap is full, that most likely means you " + ("misconfigured entrySize/chunkSize, and entries tend to take less chunks than " + "expected")));
    }

    public void closeHashLookupSearchNextPosDependants() {
        CompiledMapQueryContext.this.closeKeySearch();
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
        CompiledMapQueryContext.this.closeMapQueryKeyEqualsDependants();
        CompiledMapQueryContext.this.closeMapEntryStagesKeyEndDependants();
        CompiledMapQueryContext.this.entryKey.closeEntryKeyBytesDataInnerGetUsingDependants();
    }

    public void writeNewEntry(long pos, Data<?, ?> key) {
        initPos(pos);
        initKeySize(key.size());
        entryBytes.position(keySizeOffset());
        CompiledMapQueryContext.this.h().keySizeMarshaller.writeSize(entryBytes, keySize());
        initKeyOffset(entryBytes.position());
        key.writeTo(entryBytesAccessor, entryBytes, keyOffset());
    }

    boolean keyEquals() {
        return ((inputKey().size()) == (CompiledMapQueryContext.this.keySize())) && (inputKey().equivalent(((ReadAccess)(CompiledMapQueryContext.this.entryBytesAccess)), CompiledMapQueryContext.this.entryBytesAccessHandle, CompiledMapQueryContext.this.entryBytesAccessOffset(CompiledMapQueryContext.this.keyOffset())));
    }

    public void closeMapQueryKeyEqualsDependants() {
        CompiledMapQueryContext.this.closeKeySearch();
    }

    protected SearchState searchState = null;

    boolean keySearchInit() {
        return (this.searchState) != null;
    }

    void initKeySearch() {
        for (long pos ; (pos = CompiledMapQueryContext.this.nextPos()) >= 0L ; ) {
            CompiledMapQueryContext.this.readExistingEntry(pos);
            if (!(keyEquals()))
                continue;

            CompiledMapQueryContext.this.found();
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
        CompiledMapQueryContext.this.closeMapQueryDropSearchIfNestedContextsAndPresentHashLookupSlotCheckFailedDependants();
    }

    private void _AllocatedChunks_incrementSegmentEntriesIfNeeded() {
    }

    public void incrementSegmentEntriesIfNeeded() {
        if ((CompiledMapQueryContext.this.searchState()) != (SearchState.PRESENT)) {
            CompiledMapQueryContext.this.entries(((CompiledMapQueryContext.this.entries()) + 1L));
        }
    }

    public void initEntryAndKey(long entrySize) {
        initAllocatedChunks(CompiledMapQueryContext.this.h().inChunks(entrySize));
        CompiledMapQueryContext.this.writeNewEntry(CompiledMapQueryContext.this.alloc(allocatedChunks()), CompiledMapQueryContext.this.inputKey());
        incrementSegmentEntriesIfNeeded();
    }

    public boolean searchStateDeleted() {
        return (((searchState()) == (SearchState.DELETED)) && (!(CompiledMapQueryContext.this.concurrentSameThreadContexts()))) && (CompiledMapQueryContext.this.innerUpdateLock.isHeldByCurrentThread());
    }

    public void dropSearchIfNestedContextsAndPresentHashLookupSlotCheckFailed() {
        if (CompiledMapQueryContext.this.locksInit()) {
            if ((CompiledMapQueryContext.this.concurrentSameThreadContexts()) && ((CompiledMapQueryContext.this.rootContextOnThisSegment().latestSameThreadSegmentModCount()) != (CompiledMapQueryContext.this.contextModCount()))) {
                if (keySearchInit()) {
                    if ((searchState()) == (SearchState.PRESENT)) {
                        if (!(CompiledMapQueryContext.this.checkSlotContainsExpectedKeyAndValue(CompiledMapQueryContext.this.pos())))
                            CompiledMapQueryContext.this.closeHashLookupPos();

                    }
                }
            }
        }
    }

    public void closeMapQueryDropSearchIfNestedContextsAndPresentHashLookupSlotCheckFailedDependants() {
        CompiledMapQueryContext.this.closeQueryCheckOnEachPublicOperationCheckOnEachPublicOperationDependants();
    }

    private void _CheckOnEachPublicOperation_checkOnEachPublicOperation() {
        CompiledMapQueryContext.this.checkAccessingFromOwnerThread();
    }

    public void checkOnEachPublicOperation() {
        _CheckOnEachPublicOperation_checkOnEachPublicOperation();
        CompiledMapQueryContext.this.dropSearchIfNestedContextsAndPresentHashLookupSlotCheckFailed();
    }

    public void closeQueryCheckOnEachPublicOperationCheckOnEachPublicOperationDependants() {
        CompiledMapQueryContext.this.entryKey.closeEntryKeyBytesDataSizeDependants();
        CompiledMapQueryContext.this.entryValue.closeEntryValueBytesDataSizeDependants();
    }

    @Override
    public R insert(@NotNull
                    MapAbsentEntry<K, V> absentEntry, Data<V, ?> value) {
        CompiledMapQueryContext.this.checkOnEachPublicOperation();
        return CompiledMapQueryContext.this.m().entryOperations.insert(absentEntry, value);
    }

    @Override
    public R remove(@NotNull
                    MapEntry<K, V> entry) {
        CompiledMapQueryContext.this.checkOnEachPublicOperation();
        return CompiledMapQueryContext.this.m().entryOperations.remove(entry);
    }

    @Override
    public Data<V, ?> defaultValue(@NotNull
                                                              MapAbsentEntry<K, V> absentEntry) {
        CompiledMapQueryContext.this.checkOnEachPublicOperation();
        return CompiledMapQueryContext.this.m().entryOperations.defaultValue(absentEntry);
    }

    @NotNull
    @Override
    public InterProcessLock writeLock() {
        CompiledMapQueryContext.this.checkOnEachPublicOperation();
        return CompiledMapQueryContext.this.innerWriteLock;
    }

    @Override
    public Data<V, ?> wrapValueAsValue(V value) {
        CompiledMapQueryContext.this.checkOnEachPublicOperation();
        WrappedValueInstanceData wrapped = CompiledMapQueryContext.this.wrappedValueInstanceValue;
        wrapped = wrapped.getUnusedWrappedValueGuarded();
        wrapped.initValue(value);
        return wrapped;
    }

    @NotNull
    @Override
    public InterProcessLock updateLock() {
        CompiledMapQueryContext.this.checkOnEachPublicOperation();
        return CompiledMapQueryContext.this.innerUpdateLock;
    }

    @NotNull
    @Override
    public Data<V, ?> value() {
        CompiledMapQueryContext.this.checkOnEachPublicOperation();
        return CompiledMapQueryContext.this.entryValue;
    }

    public Data<K, ?> queriedKey() {
        CompiledMapQueryContext.this.checkOnEachPublicOperation();
        return inputKey();
    }

    @NotNull
    @Override
    public Data<K, ?> absentKey() {
        CompiledMapQueryContext.this.checkOnEachPublicOperation();
        return CompiledMapQueryContext.this.inputKey();
    }

    @NotNull
    @Override
    public Data<K, ?> key() {
        CompiledMapQueryContext.this.checkOnEachPublicOperation();
        return CompiledMapQueryContext.this.entryKey;
    }

    @NotNull
    @Override
    public InterProcessLock readLock() {
        CompiledMapQueryContext.this.checkOnEachPublicOperation();
        return CompiledMapQueryContext.this.innerReadLock;
    }

    @Override
    public R replaceValue(@NotNull
                          MapEntry<K, V> entry, Data<V, ?> newValue) {
        CompiledMapQueryContext.this.checkOnEachPublicOperation();
        return CompiledMapQueryContext.this.m().entryOperations.replaceValue(entry, newValue);
    }

    @NotNull
    @Override
    public Data<V, ?> defaultValue() {
        CompiledMapQueryContext.this.checkOnEachPublicOperation();
        return CompiledMapQueryContext.this.wrapValueAsValue(CompiledMapQueryContext.this.m().defaultValue(CompiledMapQueryContext.this.deprecatedMapKeyContext));
    }

    @NotNull
    @Override
    public MapContext<K, V, ?> context() {
        CompiledMapQueryContext.this.checkOnEachPublicOperation();
        return this;
    }

    public boolean searchStatePresent() {
        return (searchState()) == (SearchState.PRESENT);
    }

    public boolean searchStateAbsent() {
        return (!(searchStatePresent())) && (!(searchStateDeleted()));
    }

    protected void putPrefix() {
        CompiledMapQueryContext.this.checkOnEachPublicOperation();
        boolean underUpdatedLockIsHeld = !(CompiledMapQueryContext.this.innerUpdateLock.isHeldByCurrentThread());
        if (underUpdatedLockIsHeld)
            CompiledMapQueryContext.this.innerUpdateLock.lock();

        boolean searchResultsNotTrusted = underUpdatedLockIsHeld || (CompiledMapQueryContext.this.concurrentSameThreadContexts());
        if (((CompiledMapQueryContext.this.hashLookupPosInit()) && (searchStateAbsent())) && searchResultsNotTrusted)
            CompiledMapQueryContext.this.closeHashLookupPos();

    }

    public boolean entryPresent() {
        return searchStatePresent();
    }

    @Nullable
    @Override
    public MapAbsentEntry<K, V> absentEntry() {
        CompiledMapQueryContext.this.checkOnEachPublicOperation();
        return entryPresent() ? null : CompiledMapQueryContext.this.absent();
    }

    @Override
    public MapEntry<K, V> entry() {
        CompiledMapQueryContext.this.checkOnEachPublicOperation();
        return entryPresent() ? this : null;
    }

    public long keyEnd() {
        return (keyOffset()) + (keySize());
    }

    public void closeMapEntryStagesKeyEndDependants() {
        CompiledMapQueryContext.this.closeMapEntryStagesCountValueSizeOffsetDependants();
        CompiledMapQueryContext.this.closeMapEntryStagesEntryEndDependants();
    }

    long countValueSizeOffset() {
        return keyEnd();
    }

    public void closeMapEntryStagesCountValueSizeOffsetDependants() {
        CompiledMapQueryContext.this.closeValueSizeOffset();
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
        CompiledMapQueryContext.this.closeValSize();
    }

    public long newSizeOfEverythingBeforeValue(Data<V, ?> newValue) {
        return ((valueSizeOffset()) + (CompiledMapQueryContext.this.m().valueSizeMarshaller.sizeEncodingSize(newValue.size()))) - (keySizeOffset());
    }

    public long valueSize = -1;

    public long valueOffset;

    public boolean valSizeInit() {
        return (this.valueSize) >= 0;
    }

    void initValSize() {
        entryBytes.position(valueSizeOffset());
        valueSize = CompiledMapQueryContext.this.m().readValueSize(entryBytes);
        countValueOffset();
        this.closeValSizeDependants();
    }

    void initValSize(long valueSize) {
        this.valueSize = valueSize;
        entryBytes.position(valueSizeOffset());
        CompiledMapQueryContext.this.m().valueSizeMarshaller.writeSize(entryBytes, valueSize);
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
        CompiledMapQueryContext.this.closeMapEntryStagesEntryEndDependants();
        CompiledMapQueryContext.this.entryValue.closeEntryValueBytesDataSizeDependants();
        CompiledMapQueryContext.this.entryValue.closeEntryValueBytesDataInnerGetUsingDependants();
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
        CompiledMapQueryContext.this.closeMapEntryStagesEntrySizeDependants();
    }

    long entrySize() {
        return (entryEnd()) - (keySizeOffset());
    }

    public void closeMapEntryStagesEntrySizeDependants() {
        CompiledMapQueryContext.this.closeTheEntrySizeInChunks();
    }

    public int entrySizeInChunks = 0;

    public boolean theEntrySizeInChunksInit() {
        return (this.entrySizeInChunks) != 0;
    }

    void initTheEntrySizeInChunks() {
        entrySizeInChunks = CompiledMapQueryContext.this.h().inChunks(entrySize());
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
        if (((!(CompiledMapQueryContext.this.m().constantlySizedEntry)) && (CompiledMapQueryContext.this.m().couldNotDetermineAlignmentBeforeAllocation)) && ((entrySizeInChunks()) < (CompiledMapQueryContext.this.allocatedChunks()))) {
            CompiledMapQueryContext.this.free(((pos()) + (entrySizeInChunks())), ((CompiledMapQueryContext.this.allocatedChunks()) - (entrySizeInChunks())));
        } else {
            initTheEntrySizeInChunks(CompiledMapQueryContext.this.allocatedChunks());
        }
    }

    void putEntry(Data<V, ?> value) {
        assert CompiledMapQueryContext.this.searchStateAbsent();
        long entrySize = CompiledMapQueryContext.this.entrySize(CompiledMapQueryContext.this.inputKey().size(), value.size());
        CompiledMapQueryContext.this.initEntryAndKey(entrySize);
        CompiledMapQueryContext.this.initValue(value);
        CompiledMapQueryContext.this.freeExtraAllocatedChunks();
        CompiledMapQueryContext.this.putNewVolatile(CompiledMapQueryContext.this.pos());
    }

    public void writeValueAndPutPos(Data<V, ?> value) {
        initValue(value);
        freeExtraAllocatedChunks();
        CompiledMapQueryContext.this.putValueVolatile(CompiledMapQueryContext.this.hashLookupPos(), pos());
    }

    public void innerRemoveEntryExceptHashLookupUpdate() {
        CompiledMapQueryContext.this.free(pos(), entrySizeInChunks());
        CompiledMapQueryContext.this.entries(((CompiledMapQueryContext.this.entries()) - 1L));
        CompiledMapQueryContext.this.incrementModCountGuarded();
    }

    @Override
    public void doRemove() {
        CompiledMapQueryContext.this.checkOnEachPublicOperation();
        CompiledMapQueryContext.this.innerUpdateLock.lock();
        if (searchStatePresent()) {
            CompiledMapQueryContext.this.innerWriteLock.lock();
            CompiledMapQueryContext.this.remove();
            CompiledMapQueryContext.this.innerRemoveEntryExceptHashLookupUpdate();
            setSearchStateGuarded(SearchState.DELETED);
        } else {
            throw new IllegalStateException("Entry is absent when doRemove() is called");
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
        initAllocatedChunks(CompiledMapQueryContext.this.h().inChunks(entrySize));
        CompiledMapQueryContext.this.copyExistingEntry(CompiledMapQueryContext.this.alloc(allocatedChunks()), bytesToCopy);
        incrementSegmentEntriesIfNeeded();
    }

    public void putValueDeletedEntry(Data<V, ?> newValue) {
        assert CompiledMapQueryContext.this.innerUpdateLock.isHeldByCurrentThread();
        int newSizeInChunks;
        long entryStartOffset = keySizeOffset();
        long newSizeOfEverythingBeforeValue = -1;
        boolean newValueSizeIsDifferent = (newValue.size()) != (valueSize());
        if (newValueSizeIsDifferent) {
            newSizeOfEverythingBeforeValue = newSizeOfEverythingBeforeValue(newValue);
            long newValueOffset = CompiledMapQueryContext.this.m().alignment.alignAddr((entryStartOffset + newSizeOfEverythingBeforeValue));
            long newEntrySize = (newValueOffset + (newValue.size())) - entryStartOffset;
            newSizeInChunks = CompiledMapQueryContext.this.m().inChunks(newEntrySize);
        } else {
            newSizeInChunks = entrySizeInChunks();
        }
        if ((((pos()) + newSizeInChunks) < (CompiledMapQueryContext.this.freeList().size())) && (CompiledMapQueryContext.this.freeList().allClear(pos(), ((pos()) + newSizeInChunks)))) {
            CompiledMapQueryContext.this.freeList().set(pos(), ((pos()) + newSizeInChunks));
            CompiledMapQueryContext.this.innerWriteLock.lock();
            CompiledMapQueryContext.this.incrementSegmentEntriesIfNeeded();
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
                CompiledMapQueryContext.this.initEntryAndKeyCopying(entrySize, ((valueSizeOffset()) - entryStartOffset));
                initValue(newValue);
            } else {
                long oldValueSizeOffset = valueSizeOffset();
                long oldValueSize = valueSize();
                long oldValueOffset = valueOffset();
                CompiledMapQueryContext.this.initEntryAndKeyCopying(entrySize, ((valueOffset()) - entryStartOffset));
                initValueWithoutSize(newValue, oldValueSizeOffset, oldValueSize, oldValueOffset);
            }
            freeExtraAllocatedChunks();
        }
        CompiledMapQueryContext.this.putValueVolatile(CompiledMapQueryContext.this.hashLookupPos(), pos());
    }

    @Override
    public void doInsert(Data<V, ?> value) {
        CompiledMapQueryContext.this.putPrefix();
        if (!(CompiledMapQueryContext.this.searchStatePresent())) {
            if (CompiledMapQueryContext.this.searchStateDeleted()) {
                CompiledMapQueryContext.this.putValueDeletedEntry(value);
            } else {
                putEntry(value);
            }
            CompiledMapQueryContext.this.incrementModCountGuarded();
            CompiledMapQueryContext.this.setSearchStateGuarded(SearchState.PRESENT);
        } else {
            throw new IllegalStateException("Entry is present in the map when doInsert() is called");
        }
    }

    protected void relocation(Data<V, ?> newValue, long newSizeOfEverythingBeforeValue) {
        CompiledMapQueryContext.this.free(pos(), entrySizeInChunks());
        long entrySize = innerEntrySize(newSizeOfEverythingBeforeValue, newValue.size());
        CompiledMapQueryContext.this.initEntryAndKeyCopying(entrySize, ((valueSizeOffset()) - (keySizeOffset())));
        writeValueAndPutPos(newValue);
    }

    public void innerDefaultReplaceValue(Data<V, ?> newValue) {
        assert CompiledMapQueryContext.this.innerUpdateLock.isHeldByCurrentThread();
        boolean newValueSizeIsDifferent = (newValue.size()) != (this.valueSize());
        if (newValueSizeIsDifferent) {
            long newSizeOfEverythingBeforeValue = newSizeOfEverythingBeforeValue(newValue);
            long entryStartOffset = keySizeOffset();
            VanillaChronicleMap<?, ?, ?, ?, ?, ?, ?> m = CompiledMapQueryContext.this.m();
            long newValueOffset = m.alignment.alignAddr((entryStartOffset + newSizeOfEverythingBeforeValue));
            long newEntrySize = (newValueOffset + (newValue.size())) - entryStartOffset;
            int newSizeInChunks = m.inChunks(newEntrySize);
            newValueDoesNotFit : if (newSizeInChunks > (entrySizeInChunks())) {
                if (newSizeInChunks > (m.maxChunksPerEntry)) {
                    throw new IllegalArgumentException(((((("Value too large: " + "entry takes ") + newSizeInChunks) + " chunks, ") + (m.maxChunksPerEntry)) + " is maximum."));
                }
                if (CompiledMapQueryContext.this.freeList().allClear(((pos()) + (entrySizeInChunks())), ((pos()) + newSizeInChunks))) {
                    CompiledMapQueryContext.this.freeList().set(((pos()) + (entrySizeInChunks())), ((pos()) + newSizeInChunks));
                    break newValueDoesNotFit;
                }
                relocation(newValue, newSizeOfEverythingBeforeValue);
                return ;
            } else if (newSizeInChunks < (entrySizeInChunks())) {
                CompiledMapQueryContext.this.freeList().clear(((pos()) + newSizeInChunks), ((pos()) + (entrySizeInChunks())));
            }
        } else {
        }
        CompiledMapQueryContext.this.innerWriteLock.lock();
        if (newValueSizeIsDifferent) {
            initValue(newValue);
        } else {
            writeValue(newValue);
        }
        CompiledMapQueryContext.this.putValueVolatile(CompiledMapQueryContext.this.hashLookupPos(), pos());
    }

    @Override
    public void doReplaceValue(Data<V, ?> newValue) {
        putPrefix();
        if (searchStatePresent()) {
            CompiledMapQueryContext.this.innerDefaultReplaceValue(newValue);
            CompiledMapQueryContext.this.incrementModCountGuarded();
            setSearchStateGuarded(SearchState.PRESENT);
        } else {
            throw new IllegalStateException("Entry is absent in the map when doReplaceValue() is called");
        }
    }
}
