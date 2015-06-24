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

import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.bytes.BytesUtil;
import net.openhft.chronicle.bytes.NativeBytesStore;
import net.openhft.chronicle.bytes.RandomDataInput;
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
import net.openhft.chronicle.map.impl.data.instance.ValueInitableData;
import net.openhft.chronicle.map.impl.ret.InstanceReturnValue;
import net.openhft.chronicle.map.impl.ret.UsableReturnValue;
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

public class CompiledMapQueryContext<K, KI, MKI extends net.openhft.chronicle.hash.serialization.internal.MetaBytesInterop<K, ? super KI>, V, VI, MVI extends MetaBytesInterop<V, ? super VI>, R> implements AutoCloseable , HashEntry<K> , InterProcessReadWriteUpdateLock , ExternalMapQueryContext<K, V, R> , MapAbsentEntry<K, V> , MapContext<K, V, R> , MapEntry<K, V> , MapAbsentEntryHolder<K, V> , QueryContextInterface<K, V, R> , VanillaChronicleMapHolder<K, KI, MKI, V, VI, MVI, R> {
    public void close() {
        this.wrappedValueInstanceValue.closeValue();
        this.inputValueInstanceValue.closeValue();
        this.closeInputBytes();
        this.wrappedValueInstanceValue.closeNext();
        this.usingReturnValue.closeUsingReturnValue();
        this.defaultReturnValue.closeDefaultReturnedValue();
        this.closeUsed();
        this.closeKeySize();
        this.inputKeyInstanceValue.closeKey();
        this.closePos();
        this.closeInputKey();
        this.closeAllocatedChunks();
        this.closeKeyOffset();
        this.closeKeyBytesInteropKeyMetaInteropDependants();
        this.closeVanillaChronicleMapHolderImplContextAtIndexInChainDependants();
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

    public void setSearchStateGuarded(CompiledMapQueryContext.SearchState newSearchState) {
        if (!(this.keySearchInit()))
            this.initKeySearch();

        setSearchState(newSearchState);
    }

    void keyFound() {
        searchState = CompiledMapQueryContext.SearchState.PRESENT;
    }

    public CompiledMapQueryContext(VanillaChronicleMap<K, KI, MKI, V, VI, MVI, R> m) {
        contextChain = new ArrayList<net.openhft.chronicle.map.impl.CompiledMapQueryContext>();
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
        this.deprecatedMapAcquireContext = new DeprecatedMapAcquireContextOnQuery();
        this.usingReturnValue = new UsingReturnValue();
        this.entryKey = new EntryKeyBytesData();
        this.innerUpdateLock = new UpdateLock();
        this.innerReadLock = new ReadLock();
        this.inputStore = new JavaLangBytesReusableBytesStore();
        this.deprecatedMapKeyContext = new DeprecatedMapKeyContextOnQuery();
        this.entryValue = new EntryValueBytesData();
        this.innerWriteLock = new WriteLock();
        this.entryBytes = this.h().ms.bytes();
        this.entryBS = new NativeBytesStore<java.lang.Object>(entryBytes.address() , entryBytes.capacity() , null , false);
        this.owner = Thread.currentThread();
        this.inputFirstValueBytesValue = new InputFirstValueBytesData();
        this.inputKeyInstanceValue = new InputKeyInstanceData();
        this.wrappedValueInstanceValue = new WrappedValueInstanceData();
        this.inputSecondValueBytesValue = new InputSecondValueBytesData();
    }

    public CompiledMapQueryContext(CompiledMapQueryContext c) {
        contextChain = c.contextChain;
        indexInContextChain = contextChain.size();
        contextChain.add(this);
        this.m = ((VanillaChronicleMap<K, KI, MKI, V, VI, MVI, R>)(c.m));
        this.inputKeyBytesValue = new InputKeyBytesData();
        this.defaultReturnValue = new DefaultReturnValue();
        this.inputValueInstanceValue = new InputValueInstanceData();
        this.copies = ThreadLocalCopies.get();
        this.valueInterop = this.m().valueInteropProvider.get(this.copies, this.m().originalValueInterop);
        this.valueReader = this.m().valueReaderProvider.get(this.copies, this.m().originalValueReader);
        this.keyInterop = this.h().keyInteropProvider.get(this.copies, this.h().originalKeyInterop);
        this.keyReader = this.h().keyReaderProvider.get(this.copies, this.h().originalKeyReader);
        this.deprecatedMapAcquireContext = new DeprecatedMapAcquireContextOnQuery();
        this.usingReturnValue = new UsingReturnValue();
        this.entryKey = new EntryKeyBytesData();
        this.innerUpdateLock = new UpdateLock();
        this.innerReadLock = new ReadLock();
        this.inputStore = new JavaLangBytesReusableBytesStore();
        this.deprecatedMapKeyContext = new DeprecatedMapKeyContextOnQuery();
        this.entryValue = new EntryValueBytesData();
        this.innerWriteLock = new WriteLock();
        this.entryBytes = this.h().ms.bytes();
        this.entryBS = new NativeBytesStore<java.lang.Object>(entryBytes.address() , entryBytes.capacity() , null , false);
        this.owner = Thread.currentThread();
        this.inputFirstValueBytesValue = new InputFirstValueBytesData();
        this.inputKeyInstanceValue = new InputKeyInstanceData();
        this.wrappedValueInstanceValue = new WrappedValueInstanceData();
        this.inputSecondValueBytesValue = new InputSecondValueBytesData();
    }

    public class DeprecatedMapAcquireContextOnQuery implements MapKeyContext<K, V> {
        @NotNull
        @Override
        public InterProcessLock writeLock() {
            return CompiledMapQueryContext.this.writeLock();
        }

        @NotNull
        @Override
        public InterProcessLock readLock() {
            return CompiledMapQueryContext.this.readLock();
        }

        @NotNull
        @Override
        public InterProcessLock updateLock() {
            return CompiledMapQueryContext.this.updateLock();
        }

        @Override
        public long valueOffset() {
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            return CompiledMapQueryContext.this.valueOffset();
        }

        @Override
        public long keyOffset() {
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            return CompiledMapQueryContext.this.keyOffset();
        }

        @NotNull
        @Override
        public Bytes entry() {
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            return CompiledMapQueryContext.this.entryBytes;
        }

        @Override
        public long keySize() {
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            return CompiledMapQueryContext.this.keySize();
        }

        @NotNull
        @Override
        public K key() {
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            return CompiledMapQueryContext.this.queriedKey().get();
        }

        @Override
        public long valueSize() {
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            return CompiledMapQueryContext.this.valueSize();
        }

        @Override
        public boolean valueEqualTo(V value) {
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            return Data.bytesEquivalent(CompiledMapQueryContext.this.entryValue, CompiledMapQueryContext.this.wrapValueAsValue(value));
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
        public V getUsing(V usingValue) {
            return containsKey() ? CompiledMapQueryContext.this.value().getUsing(usingValue) : null;
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
        public V get() {
            assert containsKey();
            return CompiledMapQueryContext.this.usingReturnValue.returnValue();
        }

        @Override
        public void close() {
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            put(CompiledMapQueryContext.this.usingReturnValue.returnValue());
            CompiledMapQueryContext.this.close();
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
        public InterProcessLock readLock() {
            return CompiledMapQueryContext.this.readLock();
        }

        @NotNull
        @Override
        public InterProcessLock updateLock() {
            return CompiledMapQueryContext.this.updateLock();
        }

        @Override
        public long keySize() {
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            return CompiledMapQueryContext.this.keySize();
        }

        @Override
        public long valueSize() {
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            return CompiledMapQueryContext.this.valueSize();
        }

        @Override
        public void close() {
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            CompiledMapQueryContext.this.close();
        }

        @Override
        public boolean valueEqualTo(V value) {
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            return Data.bytesEquivalent(CompiledMapQueryContext.this.entryValue, CompiledMapQueryContext.this.wrapValueAsValue(value));
        }

        @Override
        public long valueOffset() {
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            return CompiledMapQueryContext.this.valueOffset();
        }

        @NotNull
        @Override
        public Bytes entry() {
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            return CompiledMapQueryContext.this.entryBytes;
        }

        @Override
        public long keyOffset() {
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            return CompiledMapQueryContext.this.keyOffset();
        }

        @NotNull
        @Override
        public K key() {
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            return CompiledMapQueryContext.this.queriedKey().get();
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
        public V getUsing(V usingValue) {
            return containsKey() ? CompiledMapQueryContext.this.value().getUsing(usingValue) : null;
        }

        @Override
        public V get() {
            return containsKey() ? CompiledMapQueryContext.this.value().get() : null;
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
    }

    public class EntryKeyBytesData extends AbstractData<K> {
        @Override
        public long offset() {
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            return CompiledMapQueryContext.this.keyOffset();
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
            CompiledMapQueryContext.this.entryBytes.position(CompiledMapQueryContext.this.keyOffset());
            return CompiledMapQueryContext.this.keyReader.read(CompiledMapQueryContext.this.entryBytes, size(), usingKey);
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
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            return cachedEntryKey();
        }

        @Override
        public K getUsing(K usingKey) {
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            return innerGetUsing(usingKey);
        }

        @Override
        public RandomDataInput bytes() {
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            return CompiledMapQueryContext.this.entryBS;
        }
    }

    public class EntryValueBytesData extends AbstractData<V> {
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
            CompiledMapQueryContext.this.entryBytes.position(CompiledMapQueryContext.this.valueOffset());
            return CompiledMapQueryContext.this.valueReader.read(CompiledMapQueryContext.this.entryBytes, size(), usingValue);
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
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            return cachedEntryValue();
        }

        @Override
        public V getUsing(V usingValue) {
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            return innerGetUsing(usingValue);
        }

        @Override
        public RandomDataInput bytes() {
            CompiledMapQueryContext.this.checkOnEachPublicOperation();
            return CompiledMapQueryContext.this.entryBS;
        }
    }

    public class InputFirstValueBytesData extends AbstractData<V> {
        @Override
        public RandomDataInput bytes() {
            return CompiledMapQueryContext.this.inputStore;
        }

        @Override
        public long offset() {
            return CompiledMapQueryContext.this.firstInputValueOffset();
        }

        @Override
        public long size() {
            return CompiledMapQueryContext.this.firstInputValueSize();
        }

        public void closeInputFirstValueBytesDataSizeDependants() {
            this.closeInputFirstValueBytesDataGetUsingDependants();
        }

        @Override
        public V getUsing(V usingValue) {
            CompiledMapQueryContext.this.inputBytes().position(CompiledMapQueryContext.this.firstInputValueOffset());
            return CompiledMapQueryContext.this.valueReader.read(CompiledMapQueryContext.this.inputBytes(), size(), usingValue);
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
            return CompiledMapQueryContext.this.inputStore;
        }

        @Override
        public long offset() {
            return CompiledMapQueryContext.this.inputKeyOffset();
        }

        @Override
        public long size() {
            return CompiledMapQueryContext.this.inputKeySize();
        }

        public void closeInputKeyBytesDataSizeDependants() {
            this.closeInputKeyBytesDataGetUsingDependants();
        }

        @Override
        public K getUsing(K usingKey) {
            Bytes inputBytes = CompiledMapQueryContext.this.inputBytes();
            inputBytes.position(CompiledMapQueryContext.this.inputKeyOffset());
            return CompiledMapQueryContext.this.keyReader.read(inputBytes, size(), usingKey);
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
            return CompiledMapQueryContext.this.inputStore;
        }

        @Override
        public long offset() {
            return CompiledMapQueryContext.this.secondInputValueOffset();
        }

        @Override
        public long size() {
            return CompiledMapQueryContext.this.secondInputValueSize();
        }

        public void closeInputSecondValueBytesDataSizeDependants() {
            this.closeInputSecondValueBytesDataGetUsingDependants();
        }

        @Override
        public V getUsing(V usingValue) {
            CompiledMapQueryContext.this.inputBytes().position(CompiledMapQueryContext.this.secondInputValueOffset());
            return CompiledMapQueryContext.this.valueReader.read(CompiledMapQueryContext.this.inputBytes(), size(), usingValue);
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
            if ((CompiledMapQueryContext.this.localLockState()) == (LocalLockState.UNLOCKED)) {
                CompiledMapQueryContext.this.segmentHeader().readLockInterruptibly(CompiledMapQueryContext.this.segmentHeaderAddress());
                CompiledMapQueryContext.this.setLocalLockStateGuarded(LocalLockState.READ_LOCKED);
            }
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
        public boolean isHeldByCurrentThread() {
            return CompiledMapQueryContext.this.localLockState().read;
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
            MVI mvi = CompiledMapQueryContext.this.valueMetaInterop(value());
            long size = mvi.size(CompiledMapQueryContext.this.valueInterop, value());
            buf = getBuffer(this.buf, size);
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
        public V getUsing(V usingValue) {
            buf().position(0);
            return CompiledMapQueryContext.this.valueReader.read(buf(), buf().limit(), usingValue);
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

        @Override
        public boolean isHeldByCurrentThread() {
            return CompiledMapQueryContext.this.localLockState().write;
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
    }

    public enum SearchState {
        PRESENT, DELETED, ABSENT;    }

    private void _AllocatedChunks_incrementSegmentEntriesIfNeeded() {
    }

    private void _CheckOnEachPublicOperation_checkOnEachPublicOperation() {
        this.checkAccessingFromOwnerThread();
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

    public void setSearchState(CompiledMapQueryContext.SearchState newSearchState) {
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
            if ((prevContext.nextNode) == (this))
                break;

            prevContext = prevContext.nextNode;
        }
        assert (nextNode) == null;
        prevContext.nextNode = null;
    }

    private long _HashEntryStages_entryEnd() {
        return keyEnd();
    }

    private CompiledMapQueryContext _Chaining_createChaining() {
        return new CompiledMapQueryContext(this);
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
            MKI mki = CompiledMapQueryContext.this.keyMetaInterop(key());
            long size = mki.size(CompiledMapQueryContext.this.keyInterop, key());
            buffer = getBuffer(this.buffer, size);
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
            MVI mvi = CompiledMapQueryContext.this.valueMetaInterop(value());
            long size = mvi.size(CompiledMapQueryContext.this.valueInterop, value());
            buffer = getBuffer(this.buffer, size);
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

    public final List<net.openhft.chronicle.map.impl.CompiledMapQueryContext> contextChain;

    public final UpdateLock innerUpdateLock;

    public UpdateLock innerUpdateLock() {
        return this.innerUpdateLock;
    }

    public List<net.openhft.chronicle.map.impl.CompiledMapQueryContext> contextChain() {
        return this.contextChain;
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

    final WrappedValueInstanceData wrappedValueInstanceValue;

    public WrappedValueInstanceData wrappedValueInstanceValue() {
        return this.wrappedValueInstanceValue;
    }

    public final InputFirstValueBytesData inputFirstValueBytesValue;

    public final JavaLangBytesReusableBytesStore inputStore;

    public JavaLangBytesReusableBytesStore inputStore() {
        return this.inputStore;
    }

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
    public VanillaChronicleMap<K, KI, MKI, V, VI, MVI, R> m() {
        return m;
    }

    @Override
    public ChronicleMap<K, V> map() {
        return m();
    }

    long sizeOfEverythingBeforeValue(long keySize, long valueSize) {
        return (((this.m().metaDataBytes) + (this.m().keySizeMarshaller.sizeEncodingSize(keySize))) + keySize) + (this.m().valueSizeMarshaller.sizeEncodingSize(valueSize));
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

    public <T>T contextAtIndexInChain(int index) {
        return ((T)(contextChain.get(index)));
    }

    public void closeVanillaChronicleMapHolderImplContextAtIndexInChainDependants() {
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
        this.closeMapEntryStagesKeyEndDependants();
        this.closeMapQueryKeyEqualsDependants();
        this.entryKey.closeEntryKeyBytesDataInnerGetUsingDependants();
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
        this.closeHashOfKey();
        this.closeMapQueryKeyEqualsDependants();
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
        CompiledMapQueryContext c = this.contextAtIndexInChain(index);
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

    CompiledMapQueryContext nextNode;

    public boolean concurrentSameThreadContexts;

    LocalLockState localLockState;

    public CompiledMapQueryContext rootContextOnThisSegment = null;

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

    public CompiledMapQueryContext rootContextOnThisSegment() {
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
        this.closeMapQueryDropSearchIfNestedContextsAndPresentHashLookupSlotCheckFailedDependants();
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
        this.closeMapQueryDropSearchIfNestedContextsAndPresentHashLookupSlotCheckFailedDependants();
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
        this.closeMapQueryDropSearchIfNestedContextsAndPresentHashLookupSlotCheckFailedDependants();
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
        this.closeMapEntryStagesEntrySizeDependants();
        this.closeMapEntryStagesReadExistingEntryDependants();
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

    public void closeMapEntryStagesReadExistingEntryDependants() {
        this.closeKeySearch();
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
        this.closeMapEntryStagesKeyEndDependants();
        this.closeMapQueryKeyEqualsDependants();
        this.entryKey.closeEntryKeyBytesDataSizeDependants();
    }

    public void writeNewEntry(long pos, Data<?> key) {
        initPos(pos);
        initKeySize(key.size());
        entryBytes.position(keySizeOffset());
        this.h().keySizeMarshaller.writeSize(entryBytes, keySize());
        initKeyOffset(entryBytes.position());
        key.writeTo(entryBS, keyOffset());
    }

    public long keyEnd() {
        return (keyOffset()) + (keySize());
    }

    public void closeMapEntryStagesKeyEndDependants() {
        this.closeMapEntryStagesCountValueSizeOffsetDependants();
        this.closeMapEntryStagesEntryEndDependants();
    }

    long countValueSizeOffset() {
        return keyEnd();
    }

    public void closeMapEntryStagesCountValueSizeOffsetDependants() {
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
        this.closeMapEntryStagesEntryEndDependants();
        this.entryValue.closeEntryValueBytesDataSizeDependants();
        this.entryValue.closeEntryValueBytesDataInnerGetUsingDependants();
    }

    protected long entryEnd() {
        return (valueOffset()) + (valueSize());
    }

    public void closeMapEntryStagesEntryEndDependants() {
        this.closeMapEntryStagesEntrySizeDependants();
    }

    long entrySize() {
        return (entryEnd()) - (keySizeOffset());
    }

    public void closeMapEntryStagesEntrySizeDependants() {
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

    public final void freeExtraAllocatedChunks() {
        if (((!(this.m().constantlySizedEntry)) && (this.m().couldNotDetermineAlignmentBeforeAllocation)) && ((entrySizeInChunks()) < (this.allocatedChunks()))) {
            this.free(((pos()) + (entrySizeInChunks())), ((this.allocatedChunks()) - (entrySizeInChunks())));
        } else {
            initTheEntrySizeInChunks(this.allocatedChunks());
        }
    }

    public void innerRemoveEntryExceptHashLookupUpdate() {
        this.free(pos(), entrySizeInChunks());
        this.entries(((this.entries()) - 1L));
        this.incrementModCountGuarded();
    }

    public void writeValue(Data<?> value) {
        value.writeTo(entryBS, valueOffset());
    }

    public void initValue(Data<?> value) {
        entryBytes.position(valueSizeOffset());
        initValSize(value.size());
        writeValue(value);
    }

    public void writeValueAndPutPos(Data<V> value) {
        initValue(value);
        freeExtraAllocatedChunks();
        this.putValueVolatile(this.hashLookupPos(), pos());
    }

    public void initValueWithoutSize(Data<?> value, long oldValueSizeOffset, long oldValueSize, long oldValueOffset) {
        assert oldValueSize == (value.size());
        initValSizeEqualToOld(oldValueSizeOffset, oldValueSize, oldValueOffset);
        writeValue(value);
    }

    public long newSizeOfEverythingBeforeValue(Data<V> newValue) {
        return ((valueSizeOffset()) + (this.m().valueSizeMarshaller.sizeEncodingSize(newValue.size()))) - (keySizeOffset());
    }

    boolean keyEquals() {
        return ((inputKey().size()) == (this.keySize())) && (BytesUtil.bytesEqual(this.entryBS, this.keyOffset(), inputKey().bytes(), inputKey().offset(), this.keySize()));
    }

    public void closeMapQueryKeyEqualsDependants() {
        this.closeKeySearch();
    }

    protected CompiledMapQueryContext.SearchState searchState = null;

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
        searchState = CompiledMapQueryContext.SearchState.ABSENT;
        this.closeKeySearchDependants();
    }

    public CompiledMapQueryContext.SearchState searchState() {
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
        this.closeMapQueryDropSearchIfNestedContextsAndPresentHashLookupSlotCheckFailedDependants();
    }

    public void incrementSegmentEntriesIfNeeded() {
        if ((this.searchState()) != (CompiledMapQueryContext.SearchState.PRESENT)) {
            this.entries(((this.entries()) + 1L));
        }
    }

    public void initEntryAndKeyCopying(long entrySize, long bytesToCopy) {
        initAllocatedChunks(this.h().inChunks(entrySize));
        this.copyExistingEntry(this.alloc(allocatedChunks()), bytesToCopy);
        incrementSegmentEntriesIfNeeded();
    }

    protected void relocation(Data<V> newValue, long newSizeOfEverythingBeforeValue) {
        this.free(pos(), entrySizeInChunks());
        long entrySize = innerEntrySize(newSizeOfEverythingBeforeValue, newValue.size());
        this.initEntryAndKeyCopying(entrySize, ((valueSizeOffset()) - (keySizeOffset())));
        writeValueAndPutPos(newValue);
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

    public void putValueDeletedEntry(Data<V> newValue) {
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

    public void initEntryAndKey(long entrySize) {
        initAllocatedChunks(this.h().inChunks(entrySize));
        this.writeNewEntry(this.alloc(allocatedChunks()), this.inputKey());
        incrementSegmentEntriesIfNeeded();
    }

    public boolean searchStateDeleted() {
        return (((searchState()) == (CompiledMapQueryContext.SearchState.DELETED)) && (!(this.concurrentSameThreadContexts()))) && (this.innerUpdateLock.isHeldByCurrentThread());
    }

    public void dropSearchIfNestedContextsAndPresentHashLookupSlotCheckFailed() {
        if (this.locksInit()) {
            if ((this.concurrentSameThreadContexts()) && ((this.rootContextOnThisSegment().latestSameThreadSegmentModCount()) != (this.contextModCount()))) {
                if (keySearchInit()) {
                    if ((searchState()) == (CompiledMapQueryContext.SearchState.PRESENT)) {
                        if (!(this.checkSlotContainsExpectedKeyAndValue(this.pos())))
                            this.closeHashLookupPos();

                    }
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
    public R remove(@NotNull
                    MapEntry<K, V> entry) {
        this.checkOnEachPublicOperation();
        return this.m().entryOperations.remove(entry);
    }

    @NotNull
    @Override
    public Data<V> defaultValue() {
        this.checkOnEachPublicOperation();
        return this.wrapValueAsValue(this.m().defaultValue(this.deprecatedMapKeyContext));
    }

    @NotNull
    @Override
    public InterProcessLock writeLock() {
        this.checkOnEachPublicOperation();
        return this.innerWriteLock;
    }

    @NotNull
    @Override
    public MapContext<K, V, ?> context() {
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
    public Data<V> wrapValueAsValue(V value) {
        this.checkOnEachPublicOperation();
        WrappedValueInstanceData wrapped = this.wrappedValueInstanceValue;
        wrapped = wrapped.getUnusedWrappedValueGuarded();
        wrapped.initValue(value);
        return wrapped;
    }

    public Data<K> queriedKey() {
        this.checkOnEachPublicOperation();
        return inputKey();
    }

    @Override
    public Data<V> defaultValue(@NotNull
                                MapAbsentEntry<K, V> absentEntry) {
        this.checkOnEachPublicOperation();
        return this.m().entryOperations.defaultValue(absentEntry);
    }

    @NotNull
    @Override
    public Data<K> absentKey() {
        this.checkOnEachPublicOperation();
        return this.inputKey();
    }

    @NotNull
    @Override
    public Data<K> key() {
        this.checkOnEachPublicOperation();
        return this.entryKey;
    }

    public boolean searchStatePresent() {
        return (searchState()) == (CompiledMapQueryContext.SearchState.PRESENT);
    }

    public boolean entryPresent() {
        return searchStatePresent();
    }

    @Override
    public MapEntry<K, V> entry() {
        this.checkOnEachPublicOperation();
        return entryPresent() ? this : null;
    }

    @Nullable
    @Override
    public MapAbsentEntry<K, V> absentEntry() {
        this.checkOnEachPublicOperation();
        return entryPresent() ? null : this.absent();
    }

    public boolean searchStateAbsent() {
        return (!(searchStatePresent())) && (!(searchStateDeleted()));
    }

    void putEntry(Data<V> value) {
        assert this.searchStateAbsent();
        long entrySize = this.entrySize(this.inputKey().size(), value.size());
        this.initEntryAndKey(entrySize);
        this.initValue(value);
        this.freeExtraAllocatedChunks();
        this.putNewVolatile(this.pos());
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

    @Override
    public void doInsert(Data<V> value) {
        this.putPrefix();
        if (!(this.searchStatePresent())) {
            if (this.searchStateDeleted()) {
                this.putValueDeletedEntry(value);
            } else {
                putEntry(value);
            }
            this.incrementModCountGuarded();
            this.setSearchStateGuarded(CompiledMapQueryContext.SearchState.PRESENT);
        } else {
            throw new IllegalStateException("Entry is present in the map when doInsert() is called");
        }
    }

    @Override
    public void doReplaceValue(Data<V> newValue) {
        putPrefix();
        if (searchStatePresent()) {
            this.innerDefaultReplaceValue(newValue);
            this.incrementModCountGuarded();
            setSearchStateGuarded(CompiledMapQueryContext.SearchState.PRESENT);
        } else {
            throw new IllegalStateException("Entry is absent in the map when doReplaceValue() is called");
        }
    }

    @Override
    public void doRemove() {
        this.checkOnEachPublicOperation();
        this.innerUpdateLock.lock();
        if (searchStatePresent()) {
            this.innerWriteLock.lock();
            this.remove();
            this.innerRemoveEntryExceptHashLookupUpdate();
            setSearchStateGuarded(CompiledMapQueryContext.SearchState.DELETED);
        } else {
            throw new IllegalStateException("Entry is absent when doRemove() is called");
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
