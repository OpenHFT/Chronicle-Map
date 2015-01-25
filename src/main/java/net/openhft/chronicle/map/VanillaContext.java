/*
 * Copyright 2014 Higher Frequency Trading http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.map;

import net.openhft.chronicle.hash.locks.IllegalInterProcessLockStateException;
import net.openhft.chronicle.hash.locks.InterProcessLock;
import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.chronicle.hash.serialization.internal.BytesBytesInterop;
import net.openhft.chronicle.hash.serialization.internal.DelegatingMetaBytesInterop;
import net.openhft.chronicle.hash.serialization.internal.MetaBytesInterop;
import net.openhft.lang.collection.DirectBitSet;
import net.openhft.lang.collection.SingleThreadedDirectBitSet;
import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.MultiStoreBytes;
import net.openhft.lang.threadlocal.ThreadLocalCopies;
import org.jetbrains.annotations.NotNull;

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

import static net.openhft.chronicle.map.VanillaContext.SearchState.*;
import static net.openhft.lang.io.NativeBytes.UNSAFE;

class VanillaContext<K, KI, MKI extends MetaBytesInterop<K, ? super KI>,
        V, VI, MVI extends MetaBytesInterop<V, ? super VI>>
        implements MapKeyContext<K, V> {

    static interface ContextFactory<T extends VanillaContext> {
        T createContext(VanillaContext root, int indexInContextCache);
        T createRootContext();
        Class<T> contextClass();
    }

    static final ThreadLocal<VanillaContext> threadLocalContextCache =
            new ThreadLocal<>();

    public static <T extends VanillaContext> T get(ContextFactory<T> contextFactory) {
        VanillaContext cache = threadLocalContextCache.get();
        if (cache != null)
            return (T) cache.getContext(contextFactory);
        T rootContext = contextFactory.createRootContext();
        threadLocalContextCache.set(rootContext);
        return rootContext;
    }

    enum VanillaChronicleMapContextFactory implements ContextFactory<VanillaContext> {
        INSTANCE;

        @Override
        public VanillaContext createContext(VanillaContext root,
                                                        int indexInContextCache) {
            return new VanillaContext(root, indexInContextCache);
        }

        @Override
        public VanillaContext createRootContext() {
            return new VanillaContext();
        }

        @Override
        public Class<VanillaContext> contextClass() {
            return VanillaContext.class;
        }
    }

    /////////////////////////////////////////////////
    // Inner state
    final ArrayList<VanillaContext> contexts;
    final ReferenceQueue<ChronicleMap> mapAndContextLocalsQueue;
    final Thread owner;
    final ThreadLocalCopies copies;
    final VanillaContext<K, KI, MKI, V, VI, MVI> contextCache;
    final int indexInContextCache;
    boolean used;

    VanillaContext() {
        contexts = new ArrayList<>(1);
        contextCache = this;
        indexInContextCache = 0;
        contexts.add(this);
        mapAndContextLocalsQueue = new ReferenceQueue<>();
        owner = Thread.currentThread();
        copies = new ThreadLocalCopies();
        used = true;
    }

    VanillaContext(VanillaContext contextCache, int indexInContextCache) {
        contexts = null;
        mapAndContextLocalsQueue = null;
        owner = Thread.currentThread();
        copies = new ThreadLocalCopies();
        this.contextCache = contextCache;
        this.indexInContextCache = indexInContextCache;
    }

    <T extends VanillaContext> T getContext(ContextFactory<T> contextFactory) {
        for (VanillaContext context : contexts) {
            if (context.getClass() == contextFactory.contextClass() &&
                    !context.used) {
                context.used = true;
                return (T) context;
            }
        }
        if (contexts.size() > (1 << 16)) {
            throw new IllegalStateException("More than " + (1 << 16) +
                    " nested ChronicleHash contexts are not supported. Very probable that " +
                    "you simply forgot to close context somewhere (recommended to use " +
                    "try-with-resources statement). " +
                    "Otherwise this is a bug, please report with this " +
                    "stack trace on https://github.com/OpenHFT/Chronicle-Map/issues");
        }
        T context = contextFactory.createContext(this, contexts.size());
        context.used = true;
        contexts.add(context);
        return context;
    }

    boolean used() {
        return used;
    }

    void checkOnEachPublicOperation() {
        if (owner != Thread.currentThread()) {
            throw new ConcurrentModificationException(
                    "Context shouldn't be accessed from multiple threads");
        }
        if (!used)
            throw new IllegalStateException("Context shouldn't be accessed after close()");
    }


    /////////////////////////////////////////////////
    // Map
    VanillaChronicleMap<K, KI, MKI, V, VI, MVI> m;

    boolean mapInit() {
        return m != null;
    }

    void initMap(VanillaChronicleMap map) {
        initMapDependencies();
        initMap0(map);
    }

    void initMapDependencies() {
        // no dependencies
    }

    void initMap0(VanillaChronicleMap<K, KI, MKI, V, VI, MVI> map) {
        m = map;
    }

    void checkMapInit() {
        if (!mapInit())
            throw new IllegalStateException("Map should be init");
    }

    void closeMap() {
        if (!mapInit())
            return;
        closeMapDependants();
        closeMap0();
    }

    void closeMapDependants() {
        // TODO update dependants
        closeMapAndContextLocals();
        closeSegmentHeader();
        closeKeyModel();
        closeValueModel();
        closeKey();
        closeValue();
        closeKeyReader();
        closeValueReader();
        closeSegmentIndex();
        closeLocks();
    }

    void closeMap0() {
        m = null;
    }

    @Override
    public void close() {
        checkOnEachPublicOperation();
        doClose();
    }

    void doClose() {
        closeMap();
        used = false;
        totalCheckClosed();
    }

    void totalCheckClosed() {
        assert !mapInit() : "map not closed";
        assert !mapAndContextLocalsInit() : "map&context locals not closed";
        assert !keyModelInit() : "key model not closed";
        assert !keyReaderInit() : "key reader not closed";
        assert !keyInit() : "key not closed";
        assert !keyHashInit() : "key hash not closed";
        assert !segmentIndexInit() : "segment index not closed";
        assert !segmentHeaderInit() : "segment header not closed";
        assert !locksInit() : "locks not closed";
        assert !segmentInit() : "segment not closed";
        assert !hashLookupInit() : "hash lookup not closed";
        assert !keySearchInit() : "key search not closed";
        assert !valueBytesInit() : "value bytes not closed";
        assert !valueReaderInit() : "value reader not closed";
        assert !valueInit() : "value not closed";
        assert !valueModelInit() : "value model not closed";
        assert !newValueInit() : "new value not closed";
        assert !entrySizeInChunksInit() : "entry size in chunks not closed";
    }

    synchronized void closeFromAnotherThread() {
        if (!used)
            throw new IllegalStateException("Already not used");
        doClose();
    }


    /////////////////////////////////////////////////
    // Map and context locals
    final ArrayList<MapAndContextLocalsReference<K, V>> mapAndContextLocalsCache =
            new ArrayList<>(1);
    MapAndContextLocalsReference<K, V> mapAndContextLocalsReference;

    static class MapAndContextLocals<K, V> {
        K reusableKey;
        V reusableValue;
        MapAndContextLocals(ChronicleMap<K, V> map) {
            // TODO why not using map.newKeyInstance() and newValueInstance()
            if (map.keyClass() == CharSequence.class)
                reusableKey = (K) new StringBuilder();
            if (map.valueClass() == CharSequence.class)
                reusableValue = (V) new StringBuilder();
        }
    }

    static class MapAndContextLocalsReference<K, V>
            extends WeakReference<ChronicleMap<K, V>> {
        MapAndContextLocals<K, V> mapAndContextLocals;
        public MapAndContextLocalsReference(
                ChronicleMap<K, V> map, ReferenceQueue<ChronicleMap> mapAndContextLocalsQueue,
                MapAndContextLocals<K, V> mapAndContextLocals) {
            super(map, mapAndContextLocalsQueue);
            this.mapAndContextLocals = mapAndContextLocals;
        }
    }

    boolean mapAndContextLocalsInit() {
        return mapAndContextLocalsReference != null;
    }

    void initMapAndContextLocals() {
        if (mapAndContextLocalsInit())
            return;
        initMapAndContextLocalsDependencies();
        initMapAndContextLocals0();
    }

    void initMapAndContextLocalsDependencies() {
        checkMapInit();
    }

    void initMapAndContextLocals0() {
        pollLocalsQueue();
        int firstEmptyIndex = -1;
        int cacheSize = mapAndContextLocalsCache.size();
        for (int i = 0; i < cacheSize; i++) {
            MapAndContextLocalsReference<K, V> ref = mapAndContextLocalsCache.get(i);
            ChronicleMap<K, V> referencedMap;
            if (ref == null || (referencedMap = ref.get()) == null) {
                if (firstEmptyIndex < 0)
                    firstEmptyIndex = i;
                if (ref != null)
                    mapAndContextLocalsCache.set(i, null);
                continue;
            }
            if (referencedMap == m) {
                mapAndContextLocalsReference = ref;
                return;
            }
        }
        MapAndContextLocals<K, V> mapAndContextLocals = new MapAndContextLocals<>(m);
        int indexToInsert = firstEmptyIndex < 0 ? cacheSize : firstEmptyIndex;
        MapAndContextLocalsReference<K, V> ref = new MapAndContextLocalsReference<>(m,
                contextCache.mapAndContextLocalsQueue, mapAndContextLocals);
        mapAndContextLocalsReference = ref;
        mapAndContextLocalsCache.add(indexToInsert, ref);
    }

    void pollLocalsQueue() {
        Reference<? extends ChronicleMap> ref;
        while ((ref = contextCache.mapAndContextLocalsQueue.poll()) != null) {
            // null object pointing to potentially large cached instances, for GC
            ((MapAndContextLocalsReference) ref).mapAndContextLocals = null;
        }
    }

    void closeMapAndContextLocals() {
        if (!mapAndContextLocalsInit())
            return;
        closeMapContextAndLocalsDependants();
        closeMapAndContextLocals0();
    }

    void closeMapContextAndLocalsDependants() {
        // TODO determine dependencies
        closeValue();
    }

    void closeMapAndContextLocals0() {
        mapAndContextLocalsReference = null;
    }


    /////////////////////////////////////////////////
    // Key model
    KI keyInterop;

    void initKeyModel() {
        if (keyModelInit())
            return;
        initKeyModelDependencies();
        initKeyModel0();
    }

    boolean keyModelInit() {
        return keyInterop != null;
    }

    void initKeyModelDependencies() {
        checkMapInit();
    }

    void initKeyModel0() {
        keyInterop = m.keyInteropProvider.get(copies, m.originalKeyInterop);
    }

    void closeKeyModel() {
        if (!keyModelInit())
            return;
        closeKeyModelDependants();
        closeKeyModel0();
    }

    void closeKeyModelDependants() {
        closeKey();
    }

    void closeKeyModel0() {
        keyInterop = null;
    }


    /////////////////////////////////////////////////
    // Key reader
    BytesReader<K> keyReader;

    void initKeyReader() {
        if (keyReaderInit())
            return;
        initKeyReaderDependencies();
        initKeyReader0();
    }

    boolean keyReaderInit() {
        return keyReader != null;
    }

    void initKeyReaderDependencies() {
        checkMapInit();
    }

    void initKeyReader0() {
        keyReader = m.keyReaderProvider.get(copies, m.originalKeyReader);
    }

    void closeKeyReader() {
        if (!keyReaderInit())
            return;
        closeKeyReaderDependants();
        closeKeyReader0();
    }

    void closeKeyReaderDependants() {
        closeKey();
    }

    void closeKeyReader0() {
        keyReader = null;
    }

    /////////////////////////////////////////////////
    // Key
    K key;
    MKI metaKeyInterop;
    long keySize;

    void initKey(K key) {
        initKeyDependencies();
        initKey0(key);
    }

    boolean keyInit() {
        return key != null;
    }

    void initKeyDependencies() {
        initKeyModel();
    }

    void initKey0(K key) {
        m.checkKey(key);
        this.key = key;
        metaKeyInterop = m.metaKeyInteropProvider.get(
                copies, m.originalMetaKeyInterop, keyInterop, key);
        keySize = metaKeyInterop.size(keyInterop, key);
    }

    void checkKeyInit() {
        if (!keyInit())
            throw new IllegalStateException("Key should be init");
    }

    void closeKey() {
        if (!keyInit())
            return;
        closeKeyDependants();
        closeKey0();
    }

    void closeKeyDependants() {
        closeKeyHash();
    }

    void closeKey0() {
        key = null;
        metaKeyInterop = null;
    }

    @Override
    public long keySize() {
        checkOnEachPublicOperation();
        checkKeyInit();
        return keySize;
    }

    @NotNull
    @Override
    public K key() {
        if (keyInit())
            return key;
        initKeySearch();
        initMapAndContextLocals();
        mapAndContextLocalsReference.mapAndContextLocals.reusableKey =
                key0(mapAndContextLocalsReference.mapAndContextLocals.reusableKey);
        assert key != null;
        return key;
    }

    /////////////////////////////////////////////////
    // Key hash
    long hash;

    void initKeyHash() {
        if (keyHashInit())
            return;
        initKeyHashDependencies();
        initKeyHash0();
    }

    boolean keyHashInit() {
        return hash != 0;
    }

    void initKeyHashDependencies() {
        checkKeyInit();
        initKeyModel();
    }

    void initKeyHash0() {
        hash = metaKeyInterop.hash(keyInterop, key);
    }

    void closeKeyHash() {
        // don't skip closing if hash = 0 because this is a valid hash also
        closeKeyHashDependants();
        closeKeyHash0();
    }

    void closeKeyHashDependants() {
        closeSegmentIndex();
    }

    void closeKeyHash0() {
        hash = 0L;
    }


    /////////////////////////////////////////////////
    // Segment index
    int segmentIndex = -1;

    void initSegmentIndex() {
        if (segmentIndexInit())
            return;
        initSegmentIndexDependencies();
        initSegmentIndex0();
    }

    boolean segmentIndexInit() {
        return segmentIndex >= 0;
    }

    void initSegmentIndexDependencies() {
        initKeyHash();
    }

    void initSegmentIndex0() {
        segmentIndex = m.hashSplitting.segmentIndex(hash);
    }

    void closeSegmentIndex() {
        if (!segmentIndexInit())
            return;
        closeSegmentIndexDependants();
        closeSegmentIndex0();
    }

    void closeSegmentIndexDependants() {
        closeSegmentHeader();
    }

    void closeSegmentIndex0() {
        segmentIndex = -1;
    }


    /////////////////////////////////////////////////
    // Segment header
    long segmentHeaderAddress;
    SegmentHeader segmentHeader;

    void initSegmentHeader() {
        if (segmentHeaderInit())
            return;
        initSegmentHeaderDependencies();
        initSegmentHeader0();
    }

    boolean segmentHeaderInit() {
        return segmentHeader != null;
    }

    void initSegmentHeaderDependencies() {
        initSegmentIndex();
    }

    void initSegmentHeader0() {
        segmentHeaderAddress = m.ms.address() + m.segmentHeaderOffset(segmentIndex);
        segmentHeader = BigSegmentHeader.INSTANCE;
    }

    void closeSegmentHeader() {
        if (!segmentHeaderInit())
            return;
        closeSegmentHeaderDependants();
        closeSegmentHeader0();
    }

    void closeSegmentHeaderDependants() {
        closeLocks();
        closeSegment();
    }

    void closeSegmentHeader0() {
        segmentHeader = null;
    }

    long size() {
        initSegmentHeader();
        return segmentHeader.size(segmentHeaderAddress);
    }

    void size(long size) {
        segmentHeader.size(segmentHeaderAddress, size);
    }

    long nextPosToSearchFrom() {
        return segmentHeader.nextPosToSearchFrom(segmentHeaderAddress);
    }

    void nextPosToSearchFrom(long nextPosToSearchFrom) {
        segmentHeader.nextPosToSearchFrom(segmentHeaderAddress, nextPosToSearchFrom);
    }


    /////////////////////////////////////////////////
    // Locks
    VanillaContext rootContextOnThisSegment;
    int readLockCount;
    int updateLockCount;
    int writeLockCount;
    int totalReadLockCount;
    int totalUpdateLockCount;
    int totalWriteLockCount;

    final InterProcessLock readLock = new ReadLock();
    final InterProcessLock updateLock = new UpdateLock();
    final InterProcessLock writeLock = new WriteLock();

    void initLocks() {
        if (locksInit())
            return;
        initLocksDependencies();
        initLocks0();
    }

    boolean locksInit() {
        return rootContextOnThisSegment != null;
    }

    void initLocksDependencies() {
        initSegmentHeader();
    }

    void initLocks0() {
        readLockCount = 0;
        updateLockCount = 0;
        writeLockCount = 0;
        for (int i = 0; i < indexInContextCache; i++) {
            VanillaContext parentContext = contextCache.contexts.get(i);
            if (parentContext.segmentHeader != null &&
                    parentContext.segmentHeaderAddress == segmentHeaderAddress) {
                rootContextOnThisSegment = parentContext;
                return;
            }
        }
        rootContextOnThisSegment = this;
        totalReadLockCount = 0;
        totalUpdateLockCount = 0;
        totalWriteLockCount = 0;
    }

    void closeLocks() {
        if (!locksInit())
            return;
        closeLocksDependants();
        closeLocks0();
    }

    void closeLocksDependants() {
        // TODO determine
        closeKeySearch();
    }

    void closeLocks0() {
        // TODO should throw ISE on attempt to close root context when children not closed
        if (rootContextOnThisSegment == this) {
            if (totalWriteLockCount > 0) {
                segmentHeader.writeUnlock(segmentHeaderAddress);
                closeKeySearch();
            } else if (totalUpdateLockCount > 0) {
                segmentHeader.updateUnlock(segmentHeaderAddress);
                closeKeySearch();
            } else if (totalReadLockCount > 0) {
                segmentHeader.readUnlock(segmentHeaderAddress);
                closeKeySearch();
            }
        } else {
            if (writeLockCount > 0 &&
                    rootContextOnThisSegment.totalReadLockCount == writeLockCount) {
                if (shouldUpdateUnlock()) {
                    if (shouldReadUnlock()) {
                        segmentHeader.writeUnlock(segmentHeaderAddress);
                        closeKeySearch();
                    } else {
                        segmentHeader.downgradeWriteToReadLock(segmentHeaderAddress);
                    }
                } else {
                    segmentHeader.downgradeWriteToUpdateLock(segmentHeaderAddress);
                }
            } else if (shouldUpdateUnlock()) {
                if (shouldReadUnlock()) {
                    segmentHeader.updateUnlock(segmentHeaderAddress);
                    closeKeySearch();
                } else {
                    segmentHeader.downgradeUpdateToReadLock(segmentHeaderAddress);
                }
            } else if (shouldReadUnlock()) {
                segmentHeader.readUnlock(segmentHeaderAddress);
                closeKeySearch();
            }
        }
        rootContextOnThisSegment.totalReadLockCount -= readLockCount;
        rootContextOnThisSegment.totalUpdateLockCount -= updateLockCount;
        rootContextOnThisSegment.totalWriteLockCount -= writeLockCount;
        readLockCount = updateLockCount = writeLockCount = 0;
        rootContextOnThisSegment = null;
    }

    boolean isReadLocked() {
        return rootContextOnThisSegment.totalReadLockCount > 0;
    }

    boolean isUpdateLocked() {
        return rootContextOnThisSegment.totalUpdateLockCount > 0;
    }

    boolean isWriteLocked() {
        return rootContextOnThisSegment.totalWriteLockCount > 0;
    }

    void upgradeToWriteLock() {
        if (!isWriteLocked())
            writeLock();
    }

    boolean shouldReadUnlock() {
        return readLockCount > 0 &&
                rootContextOnThisSegment.totalReadLockCount == readLockCount;
    }

    boolean shouldUpdateUnlock() {
        return updateLockCount > 0 &&
                rootContextOnThisSegment.totalUpdateLockCount == updateLockCount;
    }

    void incrementReadCounts() {
        rootContextOnThisSegment.totalReadLockCount++;
        readLockCount++;
    }

    void decrementReadCounts() {
        rootContextOnThisSegment.totalReadLockCount--;
        readLockCount--;
    }

    void incrementUpdateCounts() {
        incrementReadCounts();
        rootContextOnThisSegment.totalUpdateLockCount++;
        updateLockCount++;
    }

    void decrementUpdateCounts() {
        decrementReadCounts();
        rootContextOnThisSegment.totalUpdateLockCount--;
        updateLockCount--;
    }

    void incrementWriteCounts() {
        incrementUpdateCounts();
        rootContextOnThisSegment.totalWriteLockCount++;
        writeLockCount++;
    }

    void decrementWriteCounts() {
        decrementUpdateCounts();
        rootContextOnThisSegment.totalWriteLockCount--;
        writeLockCount--;
    }

    abstract class AbstractLock implements InterProcessLock {
        abstract boolean fastLock();
        abstract void doLock();
        abstract void doLockInterruptibly();
        abstract boolean doTryLock(long time, TimeUnit unit);
        abstract void incrementCounts();

        @Override
        public void lock() {
            checkOnEachPublicOperation();
            if (!fastLock())
                doLock();
            incrementCounts();
        }

        @Override
        public void lockInterruptibly() throws InterruptedException {
            checkOnEachPublicOperation();
            if (!fastLock())
                doLockInterruptibly();
            incrementCounts();
        }

        @Override
        public boolean tryLock(long time, @NotNull TimeUnit unit) throws InterruptedException {
            checkOnEachPublicOperation();
            if (fastLock() || doTryLock(time, unit)) {
                incrementCounts();
                return true;
            } else {
                return false;
            }
        }

        @NotNull
        @Override
        public Condition newCondition() {
            throw new UnsupportedOperationException();
        }
    }

    class ReadLock extends AbstractLock {
        @Override
        boolean fastLock() {
            return isReadLocked();
        }

        @Override
        public boolean isHeldByCurrentThread() {
            return fastLock();
        }

        @Override
        void doLock() {
            segmentHeader.readLock(segmentHeaderAddress);
        }

        @Override
        void doLockInterruptibly() {
            segmentHeader.readLockInterruptibly(segmentHeaderAddress);
        }

        @Override
        public boolean tryLock() {
            checkOnEachPublicOperation();
            if (fastLock() || segmentHeader.tryReadLock(segmentHeaderAddress)) {
                incrementCounts();
                return true;
            } else {
                return false;
            }
        }

        @Override
        boolean doTryLock(long time, TimeUnit unit) {
            return segmentHeader.tryReadLock(segmentHeaderAddress, time, unit);
        }

        @Override
        void incrementCounts() {
            incrementReadCounts();
        }

        @Override
        public void unlock() {
            if (readLockCount == 0)
                throw new IllegalInterProcessLockStateException("Read lock is not held");
            if (rootContextOnThisSegment.totalReadLockCount == 1) {
                segmentHeader.readUnlock(segmentHeaderAddress);
                closeKeySearch();
            }
            decrementReadCounts();
        }
    }

    class UpdateLock extends AbstractLock {
        @Override
        boolean fastLock() {
            if (rootContextOnThisSegment.totalUpdateLockCount == 0) {
                if (rootContextOnThisSegment.totalReadLockCount != 0) {
                    throw new IllegalInterProcessLockStateException("Must not acquire update " +
                            "lock, while read lock is already held by this thread");
                }
                return false;
            } else {
                return true;
            }
        }

        @Override
        public boolean isHeldByCurrentThread() {
            return isUpdateLocked();
        }

        @Override
        void doLock() {
            segmentHeader.updateLock(segmentHeaderAddress);
        }

        @Override
        void doLockInterruptibly() {
            segmentHeader.updateLockInterruptibly(segmentHeaderAddress);
        }

        @Override
        public boolean tryLock() {
            checkOnEachPublicOperation();
            if (doTryLock()) {
                incrementCounts();
                return true;
            } else {
                return false;
            }
        }

        boolean doTryLock() {
            if (rootContextOnThisSegment.totalUpdateLockCount == 0) {
                if (rootContextOnThisSegment.totalReadLockCount > 0) {
                    return segmentHeader.tryUpgradeReadToUpdateLock(segmentHeaderAddress);
                } else {
                    return segmentHeader.tryUpdateLock(segmentHeaderAddress);
                }
            } else {
                return true;
            }
        }

        @Override
        boolean doTryLock(long time, TimeUnit unit) {
            return segmentHeader.tryUpdateLock(segmentHeaderAddress, time, unit);
        }

        @Override
        void incrementCounts() {
            incrementUpdateCounts();
        }

        @Override
        public void unlock() {
            if (updateLockCount == 0)
                throw new IllegalInterProcessLockStateException("Update lock is not held");
            if (rootContextOnThisSegment.totalUpdateLockCount == 1) {
                if (rootContextOnThisSegment.totalReadLockCount == 1) {
                    segmentHeader.updateUnlock(segmentHeaderAddress);
                    closeKeySearch();
                } else {
                    segmentHeader.downgradeUpdateToReadLock(segmentHeaderAddress);
                }
            }
            decrementUpdateCounts();
        }
    }

    class WriteLock extends AbstractLock {
        @Override
        boolean fastLock() {
            if (rootContextOnThisSegment.totalWriteLockCount == 0) {
                if (rootContextOnThisSegment.totalUpdateLockCount == 0 &&
                        rootContextOnThisSegment.totalReadLockCount != 0) {
                    throw new IllegalInterProcessLockStateException("Must not acquire write " +
                            "lock, while read lock is already held by this thread");
                }
                return false;
            } else {
                return true;
            }
        }

        @Override
        public boolean isHeldByCurrentThread() {
            return isWriteLocked();
        }

        @Override
        void doLock() {
            if (rootContextOnThisSegment.totalUpdateLockCount > 0) {
                segmentHeader.upgradeUpdateToWriteLock(segmentHeaderAddress);
            } else {
                segmentHeader.writeLock(segmentHeaderAddress);
            }
        }

        @Override
        void doLockInterruptibly() {
            if (rootContextOnThisSegment.totalUpdateLockCount > 0) {
                segmentHeader.upgradeUpdateToWriteLockInterruptibly(segmentHeaderAddress);
            } else {
                segmentHeader.writeLockInterruptibly(segmentHeaderAddress);
            }
        }

        @Override
        public boolean tryLock() {
            checkOnEachPublicOperation();
            if (doTryLock()) {
                incrementCounts();
                return true;
            } else {
                return false;
            }
        }

        boolean doTryLock() {
            if (rootContextOnThisSegment.totalWriteLockCount == 0) {
                if (rootContextOnThisSegment.totalUpdateLockCount > 0) {
                    return segmentHeader.tryUpgradeUpdateToWriteLock(segmentHeaderAddress);
                } else if (rootContextOnThisSegment.totalReadLockCount > 0) {
                    return segmentHeader.tryUpgradeReadToWriteLock(segmentHeaderAddress);
                } else {
                    return segmentHeader.tryWriteLock(segmentHeaderAddress);
                }
            } else {
                return true;
            }
        }

        @Override
        boolean doTryLock(long time, TimeUnit unit) {
            if (rootContextOnThisSegment.totalUpdateLockCount > 0) {
                return segmentHeader.tryUpgradeUpdateToWriteLock(
                        segmentHeaderAddress, time, unit);
            } else {
                return segmentHeader.tryWriteLock(segmentHeaderAddress, time, unit);
            }
        }

        @Override
        void incrementCounts() {
            incrementWriteCounts();
        }

        @Override
        public void unlock() {
            if (writeLockCount == 0)
                throw new IllegalInterProcessLockStateException("Write lock is not held");
            if (rootContextOnThisSegment.totalWriteLockCount == 1) {
                if (rootContextOnThisSegment.totalUpdateLockCount == 1) {
                    if (rootContextOnThisSegment.totalReadLockCount == 1) {
                        segmentHeader.writeUnlock(segmentHeaderAddress);
                        closeKeySearch();
                    } else {
                        segmentHeader.downgradeWriteToReadLock(segmentHeaderAddress);
                    }
                } else {
                    segmentHeader.downgradeWriteToUpdateLock(segmentHeaderAddress);
                }
            }
            decrementWriteCounts();
        }
    }

    @NotNull
    @Override
    public InterProcessLock readLock() {
        checkOnEachPublicOperation();
        initLocks();
        return readLock;
    }

    @NotNull
    @Override
    public InterProcessLock updateLock() {
        checkOnEachPublicOperation();
        initLocks();
        return updateLock;
    }

    @NotNull
    @Override
    public InterProcessLock writeLock() {
        checkOnEachPublicOperation();
        initLocks();
        return writeLock;
    }

    /////////////////////////////////////////////////
    // Segment
    final HashLookup hashLookup = new HashLookup();
    final MultiStoreBytes freeListBytes = new MultiStoreBytes();
    final SingleThreadedDirectBitSet freeList = new SingleThreadedDirectBitSet();
    long entrySpaceOffset;

    void initSegment() {
        if (segmentInit())
            return;
        initSegmentDependencies();
        initSegment0();
    }

    boolean segmentInit() {
        return entrySpaceOffset != 0;
    }

    void initSegmentDependencies() {
        initSegmentHeader();
    }

    void initSegment0() {
        long hashLookupOffset = m.segmentOffset(segmentIndex);
        hashLookup.reuse(m.ms.address() + hashLookupOffset,
                m.segmentHashLookupCapacity, m.segmentHashLookupEntrySize,
                m.segmentHashLookupKeyBits, m.segmentHashLookupValueBits);
        long freeListOffset = hashLookupOffset + m.segmentHashLookupOuterSize;
        freeListBytes.storePositionAndSize(m.ms, freeListOffset, m.segmentFreeListInnerSize);
        freeList.reuse(freeListBytes);
        entrySpaceOffset = freeListOffset + m.segmentFreeListOuterSize +
                m.segmentEntrySpaceInnerOffset;
    }

    void closeSegment() {
        if (!segmentInit())
            return;
        closeSegmentDependants();
        closeSegment0();
    }

    void closeSegmentDependants() {
        closeHashLookup();
    }

    void closeSegment0() {
        entrySpaceOffset = 0L;
    }

    /////////////////////////////////////////////////
    // Hash lookup
    public void initHashLookup() {
        if (hashLookupInit())
            return;
        initHashLookupDependencies();
        hashLookup.init0(m.hashSplitting.segmentHash(hash));
    }

    boolean hashLookupInit() {
        return hashLookup.isInit();
    }

    void initHashLookupDependencies() {
        initSegment();
    }

    public void closeHashLookup() {
        if (!hashLookupInit())
            return;
        closeHashLookupDependants();
        hashLookup.close0();
    }

    void closeHashLookupDependants() {
        closeKeySearch();
    }

    /////////////////////////////////////////////////
    // Key search, key bytes and entry
    static enum SearchState {
        PRESENT,
        ABSENT,
        DELETED
    }
    SearchState state;
    long pos;
    long keyOffset;
    final MultiStoreBytes entryCache = new MultiStoreBytes();
    MultiStoreBytes entry;

    void initKeySearch() {
        if (keySearchInit())
            return;
        initKeySearchDependencies();
        initKeySearch0();
    }

    boolean keySearchInit() {
        return state != null;
    }

    void initKeySearchDependencies() {
        initHashLookup();
        initLocks();
        if (!isReadLocked())
            readLock().lock();
    }

    void initKeySearch0() {
        hashLookup.initSearch0();
        while ((pos = hashLookup.nextPos()) >= 0L) {
            reuse(pos);
            if (!keyEquals())
                continue;
            hashLookup.found();
            initKeyOffset0();
            keyFound();
            return;
        }
        state = ABSENT;
    }

    void initKeyOffset0() {
        keyOffset = entry.position();
    }

    void keyFound() {
        state = PRESENT;
    }

    void closeKeySearch() {
        if (!keySearchInit())
            return;
        closeKeySearchDependants();
        closeKeySearch0();
    }

    void closeKeySearchDependants() {
        // TODO determine dependencies
        closeValueBytes();
    }

    void closeKeySearch0() {
        state = null;
        pos = -1;
        entry = null;
        hashLookup.closeSearch0();
    }

    @Override
    public boolean containsKey() {
        checkOnEachPublicOperation();
        initKeySearch();
        return state == PRESENT;
    }

    void checkContainsKey() {
        if (!containsKey())
            throw new IllegalStateException("Key is absent");
    }

    @Override
    public Bytes entry() {
        checkContainsKey();
        return entry;
    }

    @Override
    public long keyOffset() {
        checkContainsKey();
        return keyOffset;
    }

    /////////////////////////////////////////////////
    // Value bytes
    long valueSizeOffset;
    long valueOffset;
    long valueSize;

    void initValueBytes() {
        if (valueBytesInit())
            return;
        initValueBytesDependencies();
        initValueBytes0();
    }

    boolean valueBytesInit() {
        return valueSizeOffset != 0;
    }

    void initValueBytesDependencies() {
        initKeySearch();
    }

    void initValueBytes0() {
        if (state == ABSENT)
            throw new IllegalStateException("Key should be present");
        initValueSizeOffset0();
        entry.position(valueSizeOffset);
        valueSize = m.readValueSize(entry);
        m.alignment.alignPositionAddr(entry);
        valueOffset = entry.position();
    }

    void initValueSizeOffset0() {
        valueSizeOffset = keyOffset + keySize;
    }

    void closeValueBytes() {
        if (!valueBytesInit())
            return;
        closeValueBytesDependants();
        closeValueBytes0();
    }

    void closeValueBytesDependants() {
        closeValue();
        closeEntrySizeInChunks();
    }

    void closeValueBytes0() {
        valueSizeOffset = 0L;
    }

    @Override
    public long valueOffset() {
        checkContainsKey();
        initValueBytes();
        return valueOffset;
    }

    @Override
    public long valueSize() {
        checkContainsKey();
        initValueBytes();
        return valueSize;
    }

    /////////////////////////////////////////////////
    // Value reader
    BytesReader<V> valueReader;

    void initValueReader() {
        if (valueReaderInit())
            return;
        initValueReaderDependencies();
        initValueReader0();
    }

    boolean valueReaderInit() {
        return valueReader != null;
    }

    void initValueReaderDependencies() {
        checkMapInit();
    }

    void initValueReader0() {
        valueReader = m.valueReaderProvider.get(copies, m.originalValueReader);
    }

    void closeValueReader() {
        if (!valueReaderInit())
            return;
        closeValueReaderDependants();
        closeValueReader0();
    }

    void closeValueReaderDependants() {
        closeValue();
    }

    void closeValueReader0() {
        valueReader = null;
    }


    /////////////////////////////////////////////////
    // Value
    V value;

    @Override
    public V get() {
        checkOnEachPublicOperation();
        if (valueInit())
            return value;
        initKeySearch();
        if (state != PRESENT)
            return null;
        initValueDependencies();
        initMapAndContextLocals();
        initValue0(mapAndContextLocalsReference.mapAndContextLocals.reusableValue);
        mapAndContextLocalsReference.mapAndContextLocals.reusableValue = value;
        return value;
    }

    boolean valueInit() {
        return value != null;
    }

    void initValueDependencies() {
        initValueBytes();
        initValueReader();
    }

    @Override
    public V getUsing(V usingValue) {
        checkOnEachPublicOperation();
        // no check, because getUsing MUST try to use the given usingValue
//        if (value != null)
//            return value;
        initKeySearch();
        if (state != PRESENT)
            return null;
        initValueDependencies();
        initValue0(usingValue);
        return value;
    }

    void initValue0(V usingValue) {
        entry.position(valueOffset);
        value = valueReader.read(entry, valueSize, usingValue);
    }

    void closeValue() {
        if (!valueInit())
            return;
        closeValueDependants();
        closeValue0();
    }

    void closeValueDependants() {
        // TODO no dependants?
    }

    void closeValue0() {
        closeInstanceValue0();
    }

    void closeInstanceValue0() {
        value = null;
    }


    /////////////////////////////////////////////////
    // Value model
    VI valueInterop;

    void initValueModel() {
        if (valueModelInit())
            return;
        initValueModelDependencies();
        initValueModel0();
    }

    boolean valueModelInit() {
        return valueInterop != null;
    }

    void initValueModelDependencies() {
        checkMapInit();
    }

    void initValueModel0() {
        initInstanceValueModel0();
    }

    void initInstanceValueModel0() {
        valueInterop = m.valueInteropProvider.get(copies, m.originalValueInterop);
    }

    void closeValueModel() {
        if (!valueModelInit())
            return;
        closeValueModelDependants();
        closeValueModel0();
    }

    void closeValueModelDependants() {
        closeNewValue();
    }

    void closeValueModel0() {
        valueInterop = null;
    }

    /////////////////////////////////////////////////
    // New value
    V newValue;
    MVI metaValueInterop;
    long newValueSize;

    void initNewValue(V newValue) {
        initNewValueDependencies();
        initNewValue0(newValue);
    }

    boolean newValueInit() {
        return newValue != null;
    }

    void initNewValueDependencies() {
        initValueModel();
    }

    void initNewValue0(V newValue) {
        initNewInstanceValue0(newValue);
    }

    void initNewInstanceValue0(V newValue) {
        m.checkValue(newValue);
        this.newValue = newValue;
        metaValueInterop = m.metaValueInteropProvider.get(
                copies, m.originalMetaValueInterop, valueInterop, newValue);
        newValueSize = metaValueInterop.size(valueInterop, newValue);
    }

    void closeNewValue() {
        if (!newValueInit())
            return;
        closeNewValueDependants();
        closeNewValue0();
    }

    void closeNewValueDependants() {
        // TODO no dependants?
    }

    void closeNewValue0() {
        metaValueInterop = null;
        newValue = null;
    }

    @Override
    public boolean valueEqualTo(V value) {
        checkOnEachPublicOperation();
        initValueBytes();
        initNewValue(value);
        try {
            if (newValueSize != valueSize)
                return false;
            entry.position(valueOffset);
            return metaValueInterop.startsWith(valueInterop, entry, newValue);
        } finally {
            closeNewValue();
        }
    }


    /////////////////////////////////////////////////
    // Entry size in chunks
    int entrySizeInChunks;

    void initEntrySizeInChunks() {
        if (entrySizeInChunksInit())
            return;
        initEntrySizeInChunksDependencies();
        initEntrySizeInChunks0();
    }

    boolean entrySizeInChunksInit() {
        return entrySizeInChunks != 0;
    }

    void initEntrySizeInChunksDependencies() {
        initValueBytes();
    }

    void initEntrySizeInChunks0() {
        entrySizeInChunks = inChunks(valueOffset + valueSize);
    }

    void closeEntrySizeInChunks() {
        if (!entrySizeInChunksInit())
            return;
        closeEntrySizeInChunksDependants();
        closeEntrySizeInChunks0();
    }

    void closeEntrySizeInChunksDependants() {
        // TODO no dependants?
    }

    void closeEntrySizeInChunks0() {
        entrySizeInChunks = 0;
    }

    void updateLockIfNeeded() {
        if (!isUpdateLocked())
            updateLock().lock();
    }

    public boolean put(V newValue) {
        return doPut(newValue);
    }

    boolean doPut(V newValue) {
        checkOnEachPublicOperation();
        initPutDependencies();
        initNewValue(newValue);
        return put0();
    }

    void initPutDependencies() {
        initLocks();
        updateLockIfNeeded();
        initKeySearch();
        initValueModel();
        initKeyModel();
    }

    boolean put0() {
        switch (state) {
            case PRESENT:
            case DELETED:
                initValueBytes();
                putValue();
                break;
            case ABSENT:
                putEntry();
        }
        state = PRESENT;
        return true;
    }

    void putEntry() {
        long entrySize = entrySize(keySize, newValueSize);
        int allocatedChunks = inChunks(entrySize);
        pos = alloc(allocatedChunks);
        reuse(pos);

        m.keySizeMarshaller.writeSize(entry, keySize);
        initKeyOffset0();
        metaKeyInterop.write(keyInterop, entry, key);
        initValueSizeOffset0();
        writeValueAndPutPos(allocatedChunks);
    }

    void writeValueAndPutPos(int allocatedChunks) {
        writeNewValueSize();
        writeNewValueAndSwitch();

        if (state != PRESENT) {
            // update the size before the store fence
            size(size() + 1L);
        }
        beforePutPos();
        // put + store fence, guarantees if concurrent readers see the new entry
        // in the hashLookup => they will also see written entry bytes
        hashLookup.putVolatile(pos);

        freeExtraAllocatedChunks(allocatedChunks);
    }

    void beforePutPos() {
    }

    void putValue() {
        initEntrySizeInChunks();
        int lesserChunks = -1;
        if (newValueSize != valueSize) {
            long newSizeOfEverythingBeforeValue =
                    valueSizeOffset + m.valueSizeMarshaller.sizeEncodingSize(newValueSize);
            long entryStartAddr = entry.address();
            long newValueAddr = m.alignment.alignAddr(
                    entryStartAddr + newSizeOfEverythingBeforeValue);
            long newEntrySize = newValueAddr + newValueSize - entryStartAddr;
            int newSizeInChunks = inChunks(newEntrySize);
            newValueDoesNotFit:
            if (newSizeInChunks > entrySizeInChunks) {
                if (newSizeInChunks > m.maxChunksPerEntry) {
                    throw new IllegalArgumentException("Value too large: " +
                            "entry takes " + newSizeInChunks + " chunks, " +
                            m.maxChunksPerEntry + " is maximum.");
                }
                if (freeList.allClear(pos + entrySizeInChunks, pos + newSizeInChunks)) {
                    long setFrom = freeListBitsSet() ? pos + entrySizeInChunks : pos;
                    freeList.set(setFrom, pos + newSizeInChunks);
                    break newValueDoesNotFit;
                }
                // RELOCATION
                beforeRelocation();
                if (freeListBitsSet())
                    free(pos, entrySizeInChunks);
                int allocatedChunks =
                        inChunks(innerEntrySize(newSizeOfEverythingBeforeValue, newValueSize));
                pos = alloc(allocatedChunks);
                reuse(pos);
                UNSAFE.copyMemory(entryStartAddr, entry.address(), valueSizeOffset);
                writeValueAndPutPos(allocatedChunks);
                return;
            } else if (newSizeInChunks < entrySizeInChunks) {
                // Freeing extra chunks
                if (freeListBitsSet())
                    freeList.clear(pos + newSizeInChunks, pos + entrySizeInChunks);
                lesserChunks = newSizeInChunks;
                // Do NOT reset nextPosToSearchFrom, because if value
                // once was larger it could easily became larger again,
                // But if these chunks will be taken by that time,
                // this entry will need to be relocated.
            }
            // new size != old size => size is not constant => size is actually written =>
            // to prevent (at least) this execution:
            // 1. concurrent reader thread reads the size
            // 2. this thread updates the size and the value
            // 3. concurrent reader reads the value
            // We MUST upgrade to exclusive lock
            upgradeToWriteLock();
            writeNewValueSize();
        } else {
            entry.position(valueOffset);
            // TODO to turn the following block on, JLANG-46 is required. Also unclear what happens
            // if the value is DataValue generated with 2, 4 or 8 distinct bytes, putting on-heap
            // implementation of such value is also not atomic currently, however there is a way
            // to make is atomic, we should identify such cases and make a single write:
            // state = UNSAFE.getLong(onHeapValueObject, offsetToTheFirstField);
            // bytes.writeLong(state);
//            boolean newValueSizeIsPowerOf2 = ((newValueSize - 1L) & newValueSize) != 0;
//            if (!newValueSizeIsPowerOf2 || newValueSize > 8L) {
//                // if the new value size is 1, 2, 4, or 8, it is written not atomically only if
//                // the user provided own marshaller and writes value byte-by-byte, that is very
//                // unlikely. in this case the user should update acquire write lock before write
//                // updates himself
//                upgradeToWriteLock();
//            }
            upgradeToWriteLock();
        }
        writeNewValueAndSwitch();
        if (state != PRESENT) {
            if (!freeListBitsSet())
                freeList.set(pos, pos + entrySizeInChunks);
            size(size() + 1L);
            beforePutPos();
            hashLookup.putVolatile(pos);
        }
        if (lesserChunks > 0)
            freeExtraAllocatedChunks(lesserChunks);
    }

    boolean freeListBitsSet() {
        return state == PRESENT;
    }

    void beforeRelocation() {
    }

    void writeNewValueSize() {
        entry.position(valueSizeOffset);
        m.valueSizeMarshaller.writeSize(entry, newValueSize);
        valueSize = newValueSize;
        m.alignment.alignPositionAddr(entry);
        valueOffset = entry.position();
    }

    void writeNewValueAndSwitch() {
        entry.position(valueOffset);
        metaValueInterop.write(valueInterop, entry, newValue);
        value = newValue;
        closeNewValue0();
    }

    @Override
    public boolean remove() {
        checkOnEachPublicOperation();
        initRemoveDependencies();
        try {
            return remove0();
        } finally {
            closeRemove();
        }
    }

    boolean remove0() {
        if (containsKey()) {
            initEntrySizeInChunks();
            upgradeToWriteLock();
            hashLookupRemove();
            free(pos, entrySizeInChunks);
            size(size() - 1L);
            state = DELETED;
            return true;
        } else {
            return false;
        }
    }

    boolean forEachEntry;

    void hashLookupRemove() {
        if (!forEachEntry)
            hashLookup.remove();
    }

    void initRemoveDependencies() {
        initSegment();
        initLocks();
        updateLockIfNeeded();
    }

    void closeRemove() {
    }

    void checkMultiMapsAndBitSetsConsistency() {
        class EntryChecker implements HashLookup.EntryConsumer {
            long size = 0;
            @Override
            public void accept(long key, long value) {
                if (freeList.isClear(value))
                    throw new IllegalStateException("Position " + value + " is present in " +
                            "multiMap but available in the free chunk list");
            }
        }
        EntryChecker entryChecker = new EntryChecker();
        hashLookup.forEach(entryChecker);
        if (size() != entryChecker.size) {
            throw new IllegalStateException("Segment inconsistent: " +
                    "size by Segment counter: " + size() +
                    ", size by multiMap records present in bit set: " + entryChecker.size);
        }
    }

    final MultiStoreBytes reuse(MultiStoreBytes entry, long pos) {
        long offsetWithinEntrySpace = pos * m.chunkSize;
        entry.setBytesOffset(m.bytes, entrySpaceOffset + offsetWithinEntrySpace);
        entry.limit(m.segmentEntrySpaceInnerSize - offsetWithinEntrySpace);
        entry.position(m.metaDataBytes);
        return entry;
    }

    final void reuse(long pos) {
        entry = reuse(entryCache, pos);
    }

    final long entrySize(long keySize, long valueSize) {
        long sizeOfEverythingBeforeValue = sizeOfEverythingBeforeValue(keySize, valueSize);
        return innerEntrySize(sizeOfEverythingBeforeValue, valueSize);
    }

    long innerEntrySize(long sizeOfEverythingBeforeValue, long valueSize) {
        if (m.constantlySizedEntry) {
            return m.alignment.alignAddr(sizeOfEverythingBeforeValue + valueSize);
        } else if (m.couldNotDetermineAlignmentBeforeAllocation) {
            return sizeOfEverythingBeforeValue + m.worstAlignment + valueSize;
        } else {
            return m.alignment.alignAddr(sizeOfEverythingBeforeValue) + valueSize;
        }
    }

    long sizeOfEverythingBeforeValue(long keySize, long valueSize) {
        return m.metaDataBytes +
                m.keySizeMarshaller.sizeEncodingSize(keySize) + keySize +
                m.valueSizeMarshaller.sizeEncodingSize(valueSize);
    }

    final int inChunks(long sizeInBytes) {
        // TODO optimize for the case when chunkSize is power of 2, that is default (and often) now
        if (sizeInBytes <= m.chunkSize)
            return 1;
        // int division is MUCH faster than long on Intel CPUs
        sizeInBytes -= 1L;
        if (sizeInBytes <= Integer.MAX_VALUE)
            return (((int) sizeInBytes) / (int) m.chunkSize) + 1;
        return (int) (sizeInBytes / m.chunkSize) + 1;
    }


    /////////////////////////////////////////////////
    // For bytes contexts
    static final Bytes DUMMY_BYTES = new ByteBufferBytes(ByteBuffer.allocate(0));
    private final MultiStoreBytes keyCopy = new MultiStoreBytes();
    private final MultiStoreBytes valueCopy = new MultiStoreBytes();
    TcpReplicator.TcpSocketChannelEntryWriter output;

    void initBytesKeyModel0() {
        keyInterop = (KI) BytesBytesInterop.INSTANCE;
    }

    void initBytesKey0(Bytes key) {
        keySize = m.keySizeMarshaller.readSize(key);
        key.limit(key.position() + keySize);
        initBytesKey00(key);
        metaKeyInterop = (MKI) DelegatingMetaBytesInterop.instance();
    }

    void initBytesKey00(Bytes key) {
        keyCopy.setBytesOffset(key, key.position());
        keyCopy.limit(keySize);
        this.key = (K) keyCopy;
    }

    void initBytesValueModel0() {
        valueInterop = (VI) BytesBytesInterop.INSTANCE;
    }

    void initNewBytesValue0(Bytes entry) {
        metaValueInterop = (MVI) DelegatingMetaBytesInterop.instance();
        entry.position(entry.limit());
        entry.limit(entry.capacity());
        newValueSize = m.valueSizeMarshaller.readSize(entry);
        entry.limit(entry.position() + newValueSize);
        initNewBytesValue00(entry);
    }

    void initNewBytesValue00(Bytes value) {
        valueCopy.setBytesOffset(value, value.position());
        valueCopy.limit(newValueSize);
        newValue = (V) valueCopy;
    }

    void closeBytesValue0() {
        closeInstanceValue0();
        output = null;
    }

    Bytes getBytes() {
        return getBytesUsing(null);
    }

    Bytes getBytesUsing(Bytes usingValue) {
        initKeySearch();
        if (state != PRESENT) {
            if (output != null) {
                output.ensureBufferSize(1L);
                output.in().writeBoolean(true);
            }
            return null;
        }
        if (output != null) {
            initValueBytes();
            long totalSize = 1L + m.valueSizeMarshaller.sizeEncodingSize(valueSize) + valueSize;
            output.ensureBufferSize(totalSize);
            output.in().writeBoolean(false);
            m.valueSizeMarshaller.writeSize(output.in(), valueSize);
            output.in().write(entry, valueOffset, valueSize);
        }
        return DUMMY_BYTES;
    }
    

    final void freeExtraAllocatedChunks(int allocatedChunks) {
        int actuallyUsedChunks;
        // fast path
        if (!m.constantlySizedEntry && m.couldNotDetermineAlignmentBeforeAllocation &&
                (actuallyUsedChunks = inChunks(entry.position())) < allocatedChunks)  {
            free(pos + actuallyUsedChunks, allocatedChunks - actuallyUsedChunks);
            entrySizeInChunks = actuallyUsedChunks;
        } else {
            entrySizeInChunks = allocatedChunks;
        }
    }

    //TODO refactor/optimize
    final long alloc(int chunks) {
        if (chunks > m.maxChunksPerEntry)
            throw new IllegalArgumentException("Entry is too large: requires " + chunks +
                    " entry size chucks, " + m.maxChunksPerEntry + " is maximum.");
        long ret = freeList.setNextNContinuousClearBits(nextPosToSearchFrom(), chunks);
        if (ret == DirectBitSet.NOT_FOUND || ret + chunks > m.actualChunksPerSegment) {
            if (ret != DirectBitSet.NOT_FOUND &&
                    ret + chunks > m.actualChunksPerSegment && ret < m.actualChunksPerSegment)
                freeList.clear(ret, m.actualChunksPerSegment);
            ret = freeList.setNextNContinuousClearBits(0L, chunks);
            if (ret == DirectBitSet.NOT_FOUND || ret + chunks > m.actualChunksPerSegment) {
                if (ret != DirectBitSet.NOT_FOUND &&
                        ret + chunks > m.actualChunksPerSegment &&
                        ret < m.actualChunksPerSegment)
                    freeList.clear(ret, m.actualChunksPerSegment);
                if (chunks == 1) {
                    throw new IllegalStateException(
                            "Segment is full, no free entries found");
                } else {
                    throw new IllegalStateException(
                            "Segment is full or has no ranges of " + chunks
                                    + " continuous free chunks"
                    );
                }
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

    void updateNextPosToSearchFrom(long allocated, int chunks) {
        long nextPosToSearchFrom = allocated + chunks;
        if (nextPosToSearchFrom >= m.actualChunksPerSegment)
            nextPosToSearchFrom = 0L;
        nextPosToSearchFrom(nextPosToSearchFrom);
    }

    void free(long fromPos, int chunks) {
        freeList.clear(fromPos, fromPos + chunks);
        if (fromPos < nextPosToSearchFrom())
            nextPosToSearchFrom(fromPos);
    }

    boolean keyEquals() {
        return keySize == m.keySizeMarshaller.readSize(entry) &&
                metaKeyInterop.startsWith(keyInterop, entry, key);
    }

    void clear() {
        writeLock();
        initSegment();
        hashLookup.clear();
        freeList.clear();
        nextPosToSearchFrom(0L);
        size(0L);
    }


    /////////////////////////////////////////////////
    // Iteration

    void initKeyFromPos() {
        reuse(pos);
        keySize = m.keySizeMarshaller.readSize(entry);
        initKeyOffset0();
        keyFound();
        initValueBytes();
    }

    K immutableKey() {
        return key0(null);
    }

    K key0(K usingKey) {
        initKeyReader();
        entry.position(keyOffset);
        key = keyReader.read(entry, keySize, usingKey);
        return key;
    }


    /////////////////////////////////////////////////
    // Acquire context

    boolean acquirePut(V newValue) {
        throw new UnsupportedOperationException("Acquire context doesn't support explicit put. " +
                "Use simple lazy context (map.context()) instead.");
    }

    boolean acquireRemove() {
        throw new UnsupportedOperationException("Acquire context doesn't support remove. " +
                "Use simple lazy context (map.context()) instead.");
    }

    void acquireClose() {
        checkOnEachPublicOperation();
        doPut(value);
        doClose();
    }
}
