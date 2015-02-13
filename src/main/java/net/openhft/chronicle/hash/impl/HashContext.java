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

package net.openhft.chronicle.hash.impl;

import net.openhft.chronicle.hash.ChronicleHash;
import net.openhft.chronicle.hash.KeyContext;
import net.openhft.chronicle.hash.hashing.LongHashFunction;
import net.openhft.chronicle.hash.impl.hashlookup.HashLookup;
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

import static net.openhft.chronicle.hash.impl.HashContext.SearchState.ABSENT;
import static net.openhft.chronicle.hash.impl.HashContext.SearchState.DELETED;
import static net.openhft.chronicle.hash.impl.HashContext.SearchState.PRESENT;

public abstract class HashContext<K, KI, MKI extends MetaBytesInterop<K, ? super KI>>
        implements KeyContext<K> {

    
    /////////////////////////////////////////////////
    // Inner state & lifecycle
    static final ThreadLocal<HashContext> threadLocalContextCache =
            new ThreadLocal<>();

    public static <T extends HashContext> T get(ContextFactory<T> contextFactory) {
        HashContext cache = threadLocalContextCache.get();
        if (cache != null)
            return (T) cache.getContext(contextFactory);
        T rootContext = contextFactory.createRootContext();
        threadLocalContextCache.set(rootContext);
        return rootContext;
    }

    final ArrayList<HashContext> contexts;
    final ReferenceQueue<ChronicleHash> hashAndContextLocalsQueue;
    final Thread owner;
    public final ThreadLocalCopies copies;
    final HashContext<K, KI, MKI> contextCache;
    final int indexInContextCache;
    boolean used;

    public HashContext() {
        contexts = new ArrayList<>(1);
        contextCache = this;
        indexInContextCache = 0;
        contexts.add(this);
        hashAndContextLocalsQueue = new ReferenceQueue<>();
        owner = Thread.currentThread();
        copies = new ThreadLocalCopies();
        used = true;
    }

    public HashContext(HashContext contextCache, int indexInContextCache) {
        contexts = null;
        hashAndContextLocalsQueue = null;
        owner = Thread.currentThread();
        copies = new ThreadLocalCopies();
        this.contextCache = contextCache;
        this.indexInContextCache = indexInContextCache;
    }

    <T extends HashContext> T getContext(ContextFactory<T> contextFactory) {
        for (HashContext context : contexts) {
            if (context.getClass() == contextFactory.contextClass() &&
                    !context.used) {
                context.used = true;
                return (T) context;
            }
        }
        int maxNestedContexts = 1 << 16;
        if (contexts.size() > maxNestedContexts) {
            throw new IllegalStateException("More than " + maxNestedContexts +
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
    
    public boolean used() {
        return used;
    }

    public void checkOnEachPublicOperation() {
        if (owner != Thread.currentThread()) {
            throw new ConcurrentModificationException(
                    "Context shouldn't be accessed from multiple threads");
        }
        if (!used)
            throw new IllegalStateException("Context shouldn't be accessed after close()");
    }

    @Override
    public void close() {
        checkOnEachPublicOperation();
        doClose();
    }

    public void doClose() {
        closeHash();
        used = false;
        totalCheckClosed();
    }

    public void totalCheckClosed() {
        assert !hashInit() : "map not closed";
        assert !hashAndContextLocalsInit() : "map&context locals not closed";
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
        assert !entrySizeInChunksInit() : "entry size in chunks not closed";
    }
    

    /////////////////////////////////////////////////
    // Hash
    public VanillaChronicleHash<K, KI, MKI, ?> h;

    public boolean hashInit() {
        return h != null;
    }

    public void initHash(VanillaChronicleHash hash) {
        initHashDependencies();
        initHash0(hash);
    }

    public void initHashDependencies() {
        // no dependencies
    }

    void initHash0(VanillaChronicleHash<K, KI, MKI, ?> hash) {
        h = hash;
    }

    public void checkHashInit() {
        if (!hashInit())
            throw new IllegalStateException("Hash should be init");
    }

    public void closeHash() {
        if (!hashInit())
            return;
        closeHashDependants();
        closeHash0();
    }

    public void closeHashDependants() {
        // TODO update dependants
        closeHashAndContextLocals();
        closeSegmentHeader();
        closeKeyModel();
        closeKey();
        closeKeyReader();
        closeSegmentIndex();
        closeLocks();
    }

    void closeHash0() {
        h = null;
    }

    
    /////////////////////////////////////////////////
    // Hash and context locals
    final ArrayList<HashAndContextLocalsReference<K>> hashAndContextLocalsCache =
            new ArrayList<>(1);
    HashAndContextLocalsReference<K> hashAndContextLocalsReference;

    public static class HashAndContextLocals<K> {
        K reusableKey;
        public HashAndContextLocals(ChronicleHash<K, ?> hash) {
            // TODO why not using hash.newKeyInstance()
            if (hash.keyClass() == CharSequence.class)
                reusableKey = (K) new StringBuilder();
        }
    }

    static class HashAndContextLocalsReference<K>
            extends WeakReference<ChronicleHash<K, ?>> {
        HashAndContextLocals<K> hashAndContextLocals;
        public HashAndContextLocalsReference(
                ChronicleHash<K, ?> hash, ReferenceQueue<ChronicleHash> hashAndContextLocalsQueue,
                HashAndContextLocals<K> hashAndContextLocals) {
            super(hash, hashAndContextLocalsQueue);
            this.hashAndContextLocals = hashAndContextLocals;
        }
    }

    public boolean hashAndContextLocalsInit() {
        return hashAndContextLocalsReference != null;
    }

    public void initHashAndContextLocals() {
        if (hashAndContextLocalsInit())
            return;
        initHashAndContextLocalsDependencies();
        initHashAndContextLocals0();
    }

    public void initHashAndContextLocalsDependencies() {
        checkHashInit();
    }

    void initHashAndContextLocals0() {
        pollLocalsQueue();
        int firstEmptyIndex = -1;
        int cacheSize = hashAndContextLocalsCache.size();
        for (int i = 0; i < cacheSize; i++) {
            HashAndContextLocalsReference<K> ref = hashAndContextLocalsCache.get(i);
            ChronicleHash<K, ?> referencedHash;
            if (ref == null || (referencedHash = ref.get()) == null) {
                if (firstEmptyIndex < 0)
                    firstEmptyIndex = i;
                if (ref != null)
                    hashAndContextLocalsCache.set(i, null);
                continue;
            }
            if (referencedHash == h) {
                hashAndContextLocalsReference = ref;
                return;
            }
        }
        HashAndContextLocals<K> hashAndContextLocals = newHashAndContextLocals();
        int indexToInsert = firstEmptyIndex < 0 ? cacheSize : firstEmptyIndex;
        HashAndContextLocalsReference<K> ref = new HashAndContextLocalsReference<>(h,
                contextCache.hashAndContextLocalsQueue, hashAndContextLocals);
        hashAndContextLocalsReference = ref;
        hashAndContextLocalsCache.add(indexToInsert, ref);
    }
    
    public HashAndContextLocals<K> newHashAndContextLocals() {
        return new HashAndContextLocals<>(h);
    }
    
    public HashAndContextLocals<K> hashAndContextLocals() {
        return hashAndContextLocalsReference.hashAndContextLocals;
    }

    void pollLocalsQueue() {
        Reference<? extends ChronicleHash> ref;
        while ((ref = contextCache.hashAndContextLocalsQueue.poll()) != null) {
            // null object pointing to potentially large cached instances, for GC
            ((HashAndContextLocalsReference) ref).hashAndContextLocals = null;
        }
    }

    public void closeHashAndContextLocals() {
        if (!hashAndContextLocalsInit())
            return;
        closeHashContextAndLocalsDependants();
        closeHashAndContextLocals0();
    }

    public void closeHashContextAndLocalsDependants() {
        // TODO determine dependencies
    }

    void closeHashAndContextLocals0() {
        hashAndContextLocalsReference = null;
    }


    /////////////////////////////////////////////////
    // Key model
    KI keyInterop;

    public void initKeyModel() {
        if (keyModelInit())
            return;
        initKeyModelDependencies();
        initKeyModel0();
    }

    public boolean keyModelInit() {
        return keyInterop != null;
    }

    public void initKeyModelDependencies() {
        checkHashInit();
    }

    public void initKeyModel0() {
        keyInterop = h.keyInteropProvider.get(copies, h.originalKeyInterop);
    }

    public void closeKeyModel() {
        if (!keyModelInit())
            return;
        closeKeyModelDependants();
        closeKeyModel0();
    }

    public void closeKeyModelDependants() {
        closeKey();
    }

    void closeKeyModel0() {
        keyInterop = null;
    }


    /////////////////////////////////////////////////
    // Key reader
    BytesReader<K> keyReader;

    public void initKeyReader() {
        if (keyReaderInit())
            return;
        initKeyReaderDependencies();
        initKeyReader0();
    }

    public boolean keyReaderInit() {
        return keyReader != null;
    }

    public void initKeyReaderDependencies() {
        checkHashInit();
    }

    void initKeyReader0() {
        keyReader = h.keyReaderProvider.get(copies, h.originalKeyReader);
    }

    public void closeKeyReader() {
        if (!keyReaderInit())
            return;
        closeKeyReaderDependants();
        closeKeyReader0();
    }

    public void closeKeyReaderDependants() {
        closeKey();
    }

    void closeKeyReader0() {
        keyReader = null;
    }

    
    /////////////////////////////////////////////////
    // Key
    K key;
    public MKI metaKeyInterop;
    public long keySize = -1;

    public void initKey(K key) {
        initKeyDependencies();
        initKey0(key);
    }

    public boolean keyInit() {
        return keySize >= 0;
    }

    public void initKeyDependencies() {
        initKeyModel();
    }

    public void initKey0(K key) {
        h.checkKey(key);
        MKI mki = h.metaKeyInteropProvider.get(
                copies, h.originalMetaKeyInterop, keyInterop, key);
        keySize = mki.size(keyInterop, key);
        metaKeyInterop = mki;
        this.key = key;
    }

    public void checkKeyInit() {
        if (!keyInit())
            throw new IllegalStateException("Key should be init");
    }

    public void closeKey() {
        if (!keyInit())
            return;
        closeKeyDependants();
        closeKey0();
    }

    public void closeKeyDependants() {
        closeKeyHash();
    }

    public void closeKey0() {
        keySize = -1;
        metaKeyInterop = null;
        key = null;
    }

    @Override
    public long keySize() {
        checkOnEachPublicOperation();
        checkKeyInit();
        return keySize0();
    }
    
    public long keySize0() {
        return keySize;
    }

    @NotNull
    @Override
    public K key() {
        if (key != null)
            return key;
        initKeySearch();
        initHashAndContextLocals();
        hashAndContextLocals().reusableKey = key0(hashAndContextLocals().reusableKey);
        assert key != null;
        return key;
    }


    /////////////////////////////////////////////////
    // Key hash
    long hash;

    public void initKeyHash() {
        if (keyHashInit())
            return;
        initKeyHashDependencies();
        initKeyHash0();
    }

    public boolean keyHashInit() {
        return hash != 0;
    }

    public void initKeyHashDependencies() {
        checkKeyInit();
        initKeyModel();
    }

    void initKeyHash0() {
        hash = metaKeyInterop.hash(keyInterop, LongHashFunction.city_1_1(), key);
    }

    public void closeKeyHash() {
        // don't skip closing if hash = 0 because this is a valid hash also
        closeKeyHashDependants();
        closeKeyHash0();
    }

    public void closeKeyHashDependants() {
        closeSegmentIndex();
    }

    void closeKeyHash0() {
        hash = 0L;
    }


    /////////////////////////////////////////////////
    // Segment index
    public int segmentIndex = -1;

    public void initSegmentIndex() {
        if (segmentIndexInit())
            return;
        initSegmentIndexDependencies();
        initSegmentIndex0();
    }

    public boolean segmentIndexInit() {
        return segmentIndex >= 0;
    }

    public void initSegmentIndexDependencies() {
        initKeyHash();
    }

    void initSegmentIndex0() {
        segmentIndex = h.hashSplitting.segmentIndex(hash);
    }

    public void closeSegmentIndex() {
        if (!segmentIndexInit())
            return;
        closeSegmentIndexDependants();
        closeSegmentIndex0();
    }

    public void closeSegmentIndexDependants() {
        closeSegmentHeader();
    }

    void closeSegmentIndex0() {
        segmentIndex = -1;
    }


    /////////////////////////////////////////////////
    // Segment header
    long segmentHeaderAddress;
    public SegmentHeader segmentHeader;

    public void initSegmentHeader() {
        if (segmentHeaderInit())
            return;
        initSegmentHeaderDependencies();
        initSegmentHeader0();
    }

    public boolean segmentHeaderInit() {
        return segmentHeader != null;
    }

    public void initSegmentHeaderDependencies() {
        initSegmentIndex();
    }

    void initSegmentHeader0() {
        segmentHeaderAddress = h.ms.address() + h.segmentHeaderOffset(segmentIndex);
        segmentHeader = BigSegmentHeader.INSTANCE;
    }

    public void closeSegmentHeader() {
        if (!segmentHeaderInit())
            return;
        closeSegmentHeaderDependants();
        closeSegmentHeader0();
    }

    public void closeSegmentHeaderDependants() {
        closeLocks();
        closeSegment();
    }

    void closeSegmentHeader0() {
        segmentHeader = null;
    }

    public long entries() {
        initSegmentHeader();
        return segmentHeader.size(segmentHeaderAddress);
    }

    public void entries(long size) {
        segmentHeader.size(segmentHeaderAddress, size);
    }

    public long nextPosToSearchFrom() {
        return segmentHeader.nextPosToSearchFrom(segmentHeaderAddress);
    }

    public void nextPosToSearchFrom(long nextPosToSearchFrom) {
        segmentHeader.nextPosToSearchFrom(segmentHeaderAddress, nextPosToSearchFrom);
    }

    public long deleted() {
        initSegmentHeader();
        return segmentHeader.deleted(segmentHeaderAddress);
    }

    public void deleted(long deleted) {
        segmentHeader.deleted(segmentHeaderAddress, deleted);
    }

    public long size() {
        return entries() - deleted();
    }


    /////////////////////////////////////////////////
    // Locks
    HashContext rootContextOnThisSegment;
    int readLockCount;
    int updateLockCount;
    public int writeLockCount;
    int totalReadLockCount;
    int totalUpdateLockCount;
    int totalWriteLockCount;

    final InterProcessLock readLock = new ReadLock();
    final InterProcessLock updateLock = new UpdateLock();
    final InterProcessLock writeLock = new WriteLock();

    public void initLocks() {
        if (locksInit())
            return;
        initLocksDependencies();
        initLocks0();
    }

    public boolean locksInit() {
        return rootContextOnThisSegment != null;
    }

    public void initLocksDependencies() {
        initSegmentHeader();
    }

    void initLocks0() {
        readLockCount = 0;
        updateLockCount = 0;
        writeLockCount = 0;
        for (int i = 0; i < indexInContextCache; i++) {
            HashContext parentContext = contextCache.contexts.get(i);
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

    public void closeLocks() {
        if (!locksInit())
            return;
        closeLocksDependants();
        closeLocks0();
    }

    public void closeLocksDependants() {
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

    public boolean isReadLocked() {
        return rootContextOnThisSegment.totalReadLockCount > 0;
    }

    public boolean isUpdateLocked() {
        return rootContextOnThisSegment.totalUpdateLockCount > 0;
    }

    public boolean isWriteLocked() {
        return rootContextOnThisSegment.totalWriteLockCount > 0;
    }

    public void upgradeToWriteLock() {
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
    public final HashLookup hashLookup = new HashLookup();
    final MultiStoreBytes freeListBytes = new MultiStoreBytes();
    public final SingleThreadedDirectBitSet freeList = new SingleThreadedDirectBitSet();
    long entrySpaceOffset;

    public void initSegment() {
        if (segmentInit())
            return;
        initSegmentDependencies();
        initSegment0();
    }

    public boolean segmentInit() {
        return entrySpaceOffset != 0;
    }

    public void initSegmentDependencies() {
        initSegmentHeader();
    }

    void initSegment0() {
        long hashLookupOffset = h.segmentOffset(segmentIndex);
        hashLookup.reuse(h.ms.address() + hashLookupOffset,
                h.segmentHashLookupCapacity, h.segmentHashLookupEntrySize,
                h.segmentHashLookupKeyBits, h.segmentHashLookupValueBits);
        long freeListOffset = hashLookupOffset + h.segmentHashLookupOuterSize;
        freeListBytes.storePositionAndSize(h.ms, freeListOffset, h.segmentFreeListInnerSize);
        freeList.reuse(freeListBytes);
        entrySpaceOffset = freeListOffset + h.segmentFreeListOuterSize +
                h.segmentEntrySpaceInnerOffset;
    }

    public void closeSegment() {
        if (!segmentInit())
            return;
        closeSegmentDependants();
        closeSegment0();
    }

    public void closeSegmentDependants() {
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
        hashLookup.init0(h.hashSplitting.segmentHash(hash));
    }

    public boolean hashLookupInit() {
        return hashLookup.isInit();
    }

    public void initHashLookupDependencies() {
        initSegment();
    }

    public void closeHashLookup() {
        if (!hashLookupInit())
            return;
        closeHashLookupDependants();
        hashLookup.close0();
    }

    public void closeHashLookupDependants() {
        closeKeySearch();
    }

    
    /////////////////////////////////////////////////
    // Entry
    final MultiStoreBytes entryCache = new MultiStoreBytes();
    public MultiStoreBytes entry;
    
    final MultiStoreBytes reuse(MultiStoreBytes entry, long pos) {
        long offsetWithinEntrySpace = pos * h.chunkSize;
        entry.setBytesOffset(h.bytes, entrySpaceOffset + offsetWithinEntrySpace);
        entry.limit(h.segmentEntrySpaceInnerSize - offsetWithinEntrySpace);
        return entry;
    }

    public final void reuse(long pos) {
        entry = reuse(entryCache, pos);
    }


    /////////////////////////////////////////////////
    // Key search, key bytes and entry
    public static enum SearchState {
        PRESENT,
        DELETED,
        ABSENT
    }
    
    SearchState searchState;
    public long pos;
    long keyOffset;

    public void initKeySearch() {
        if (keySearchInit())
            return;
        initKeySearchDependencies();
        initKeySearch0();
    }

    public boolean keySearchInit() {
        return searchState != null;
    }

    public void initKeySearchDependencies() {
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
        searchState = ABSENT;
    }

    boolean keyEquals() {
        return keySize == h.keySizeMarshaller.readSize(entry) &&
                metaKeyInterop.startsWith(keyInterop, entry, key);
    }

    public void initKeyOffset0() {
        keyOffset = entry.position();
    }

    public void keyFound() {
        searchState = PRESENT;
    }

    public void closeKeySearch() {
        if (!keySearchInit())
            return;
        closeKeySearchDependants();
        closeKeySearch0();
    }

    public void closeKeySearchDependants() {
    }

    void closeKeySearch0() {
        searchState = null;
        pos = -1;
        entry = null;
        hashLookup.closeSearch0();
    }

    @Override
    public boolean containsKey() {
        checkOnEachPublicOperation();
        initKeySearch();
        return containsKey0();
    }

    public boolean containsKey0() {
        return searchStatePresent();
    }

    public SearchState searchState0() {
        return searchState;
    }
    
    public boolean searchStatePresent() {
        return searchState == PRESENT;
    }

    public void checkContainsKey() {
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
        return keyOffset0();
    }
    
    public long keyOffset0() {
        return keyOffset;
    }


    /////////////////////////////////////////////////
    // Entry size in chunks
    public int entrySizeInChunks;

    public void initEntrySizeInChunks() {
        if (entrySizeInChunksInit())
            return;
        initEntrySizeInChunksDependencies();
        initEntrySizeInChunks0();
    }

    public boolean entrySizeInChunksInit() {
        return entrySizeInChunks != 0;
    }

    public void initEntrySizeInChunksDependencies() {
    }

    public abstract void initEntrySizeInChunks0();

    public void closeEntrySizeInChunks() {
        if (!entrySizeInChunksInit())
            return;
        closeEntrySizeInChunksDependants();
        closeEntrySizeInChunks0();
    }

    public void closeEntrySizeInChunksDependants() {
        // TODO no dependants?
    }

    void closeEntrySizeInChunks0() {
        entrySizeInChunks = 0;
    }


    /////////////////////////////////////////////////
    // Write operations

    public void updateLockIfNeeded() {
        if (!isUpdateLocked())
            updateLock().lock();
    }

    public int allocateEntry(long entrySize) {
        int allocatedChunks = h.inChunks(entrySize);
        pos = alloc(allocatedChunks);
        reuse(pos);
        return allocatedChunks;
    }

    public int allocateEntryAndWriteKey(long payloadSize) {
        int allocatedChunks = allocateEntry(keySize + payloadSize);

        h.keySizeMarshaller.writeSize(entry, keySize);
        initKeyOffset0();
        metaKeyInterop.write(keyInterop, entry, key);

        if (!searchStatePresent()) {
            // update the size before the store fence
            entries(entries() + 1L);
        }

        return allocatedChunks;
    }

    public void commitEntryAllocation() {
        searchState = PRESENT;
        // put + store fence, guarantees if concurrent readers see the new entry
        // in the hashLookup => they will also see written entry bytes
        hashLookup.putVolatile(pos);
    }

    //TODO refactor/optimize
    public final long alloc(int chunks) {
        if (chunks > h.maxChunksPerEntry)
            throw new IllegalArgumentException("Entry is too large: requires " + chunks +
                    " entry size chucks, " + h.maxChunksPerEntry + " is maximum.");
        long ret = freeList.setNextNContinuousClearBits(nextPosToSearchFrom(), chunks);
        if (ret == DirectBitSet.NOT_FOUND || ret + chunks > h.actualChunksPerSegment) {
            if (ret != DirectBitSet.NOT_FOUND &&
                    ret + chunks > h.actualChunksPerSegment && ret < h.actualChunksPerSegment)
                freeList.clear(ret, h.actualChunksPerSegment);
            ret = freeList.setNextNContinuousClearBits(0L, chunks);
            if (ret == DirectBitSet.NOT_FOUND || ret + chunks > h.actualChunksPerSegment) {
                if (ret != DirectBitSet.NOT_FOUND &&
                        ret + chunks > h.actualChunksPerSegment &&
                        ret < h.actualChunksPerSegment)
                    freeList.clear(ret, h.actualChunksPerSegment);
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

    public void free(long fromPos, int chunks) {
        freeList.clear(fromPos, fromPos + chunks);
        if (fromPos < nextPosToSearchFrom())
            nextPosToSearchFrom(fromPos);
    }

    void updateNextPosToSearchFrom(long allocated, int chunks) {
        long nextPosToSearchFrom = allocated + chunks;
        if (nextPosToSearchFrom >= h.actualChunksPerSegment)
            nextPosToSearchFrom = 0L;
        nextPosToSearchFrom(nextPosToSearchFrom);
    }


    /////////////////////////////////////////////////
    // Remove
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

    public boolean remove0() {
        if (containsKey()) {
            initEntrySizeInChunks();
            upgradeToWriteLock();
            hashLookupRemove();
            free(pos, entrySizeInChunks);
            entries(entries() - 1L);
            searchState = DELETED;
            return true;
        } else {
            return false;
        }
    }

    public boolean forEachEntry;

    public void hashLookupRemove() {
        if (!forEachEntry)
            hashLookup.remove();
    }

    public void initRemoveDependencies() {
        initSegment();
        initLocks();
        updateLockIfNeeded();
    }

    public void closeRemove() {
    }


    public void clear() {
        writeLock();
        initSegment();
        hashLookup.clear();
        freeList.clear();
        nextPosToSearchFrom(0L);
        entries(0L);
    }


    /////////////////////////////////////////////////
    // For bytes contexts
    public static final Bytes DUMMY_BYTES = new ByteBufferBytes(ByteBuffer.allocate(0));
    private final MultiStoreBytes keyCopy = new MultiStoreBytes();

    public void initBytesKeyModel0() {
        keyInterop = (KI) BytesBytesInterop.INSTANCE;
    }

    public void initBytesKey0(Bytes key) {
        keySize = h.keySizeMarshaller.readSize(key);
        key.limit(key.position() + keySize);
        initBytesKey00(key);
        metaKeyInterop = (MKI) DelegatingMetaBytesInterop.instance();
    }

    public void initBytesKey00(Bytes key) {
        keyCopy.setBytesOffset(key, key.position());
        keyCopy.limit(keySize);
        this.key = (K) keyCopy;
    }


    /////////////////////////////////////////////////
    // Iteration
    public void initKeyFromPos() {
        reuse(pos);
        keySize = h.keySizeMarshaller.readSize(entry);
        initKeyOffset0();
        keyFound();
    }

    public K immutableKey() {
        return key0(null);
    }

    K key0(K usingKey) {
        initKeyReader();
        entry.position(keyOffset);
        key = keyReader.read(entry, keySize, usingKey);
        return key;
    }
}
