/*
 * Copyright 2012-2018 Chronicle Map Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.map;

import net.openhft.chronicle.algo.hashing.LongHashFunction;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.bytes.PointerBytesStore;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.hash.ChronicleHashClosedException;
import net.openhft.chronicle.hash.ChronicleHashCorruption;
import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.impl.*;
import net.openhft.chronicle.hash.impl.stage.entry.LocksInterface;
import net.openhft.chronicle.hash.impl.stage.hash.ChainingInterface;
import net.openhft.chronicle.hash.impl.util.Objects;
import net.openhft.chronicle.hash.impl.util.Throwables;
import net.openhft.chronicle.hash.serialization.DataAccess;
import net.openhft.chronicle.hash.serialization.SizeMarshaller;
import net.openhft.chronicle.hash.serialization.SizedReader;
import net.openhft.chronicle.hash.serialization.impl.SerializationBuilder;
import net.openhft.chronicle.map.impl.*;
import net.openhft.chronicle.map.impl.ret.InstanceReturnValue;
import net.openhft.chronicle.set.ChronicleSet;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

import static net.openhft.chronicle.map.ChronicleMapBuilder.greatestCommonDivisor;

public class VanillaChronicleMap<K, V, R>
        extends VanillaChronicleHash<K, MapEntry<K, V>, MapSegmentContext<K, V, ?>,
        ExternalMapQueryContext<K, V, ?>>
        implements AbstractChronicleMap<K, V> {

    public SizeMarshaller valueSizeMarshaller;
    public SizedReader<V> valueReader;
    public DataAccess<V> valueDataAccess;
    public boolean constantlySizedEntry;
    /////////////////////////////////////////////////
    // Memory management and dependent fields
    public int alignment;
    public int worstAlignment;
    public transient boolean couldNotDetermineAlignmentBeforeAllocation;
    /**
     * @see net.openhft.chronicle.set.SetFromMap
     */
    public transient ChronicleSet<K> chronicleSet;
    public transient MapEntryOperations<K, V, R> entryOperations;
    public transient MapMethods<K, V, R> methods;
    public transient DefaultValueProvider<K, V> defaultValueProvider;
    /////////////////////////////////////////////////
    // Value Data model
    Type valueClass;
    /////////////////////////////////////////////////
    // Behavior
    transient boolean putReturnsNull;
    transient boolean removeReturnsNull;
    transient Set<Entry<K, V>> entrySet;
    transient ThreadLocal<ContextHolder> cxt;
    /////////////////////////////////////////////////
    private transient String name;
    /**
     * identityString is initialized lazily in {@link #toIdentityString()} rather than in
     * {@link #initOwnTransients()} because it depends on {@link #file()} which is set after
     * initOwnTransients().
     */
    private transient String identityString;
    private transient boolean defaultEntryOperationsAndMethods;

    public VanillaChronicleMap(ChronicleMapBuilder<K, V> builder) throws IOException {
        super(builder);
        SerializationBuilder<V> valueBuilder = builder.valueBuilder;
        valueClass = valueBuilder.tClass;
        valueSizeMarshaller = valueBuilder.sizeMarshaller();
        valueReader = valueBuilder.reader();
        valueDataAccess = valueBuilder.dataAccess();

        constantlySizedEntry = builder.constantlySizedEntries();

        // Concurrency (number of segments), memory management and dependent fields
        alignment = builder.valueAlignment();
        worstAlignment = builder.worstAlignment();

        initTransientsFromBuilder(builder);
        initTransients();
    }

    public static long alignAddr(long addr, long alignment) {
        return (addr + alignment - 1) & ~(alignment - 1L);
    }

    @Override
    protected void readMarshallableFields(@NotNull WireIn wireIn) {
        super.readMarshallableFields(wireIn);

        valueClass = wireIn.read(() -> "valueClass").lenientTypeLiteral();
        valueSizeMarshaller = wireIn.read(() -> "valueSizeMarshaller").object(SizeMarshaller.class);
        valueReader = wireIn.read(() -> "valueReader").object(SizedReader.class);
        valueDataAccess = wireIn.read(() -> "valueDataAccess").object(DataAccess.class);

        constantlySizedEntry = wireIn.read(() -> "constantlySizedEntry").bool();

        alignment = wireIn.read(() -> "alignment").int32();
        worstAlignment = wireIn.read(() -> "worstAlignment").int32();
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wireOut) {
        super.writeMarshallable(wireOut);

        wireOut.write(() -> "valueClass").typeLiteral(valueClass);
        wireOut.write(() -> "valueSizeMarshaller").object(valueSizeMarshaller);
        wireOut.write(() -> "valueReader").object(valueReader);
        wireOut.write(() -> "valueDataAccess").object(valueDataAccess);

        wireOut.write(() -> "constantlySizedEntry").bool(constantlySizedEntry);

        wireOut.write(() -> "alignment").int32(alignment);
        wireOut.write(() -> "worstAlignment").int32(worstAlignment);
    }

    void initTransientsFromBuilder(ChronicleMapBuilder<K, V> builder) {
        name = builder.name();
        putReturnsNull = builder.putReturnsNull();
        removeReturnsNull = builder.removeReturnsNull();

        entryOperations = (MapEntryOperations<K, V, R>) builder.entryOperations;
        methods = (MapMethods<K, V, R>) builder.methods;
        defaultEntryOperationsAndMethods = entryOperations == DefaultSpi.mapEntryOperations() &&
                methods == DefaultSpi.mapMethods();
        defaultValueProvider = builder.defaultValueProvider;
    }

    @Override
    public void initTransients() {
        super.initTransients();
        initOwnTransients();
    }

    public void recover(
            ChronicleHashResources resources, ChronicleHashCorruption.Listener corruptionListener,
            ChronicleHashCorruptionImpl corruption) throws IOException {
        basicRecover(resources, corruptionListener, corruption);
        try (IterationContext<K, V, ?> iterationContext = iterationContext()) {
            iterationContext.recoverSegments(corruptionListener, corruption);
        }
    }

    private void initOwnTransients() {
        couldNotDetermineAlignmentBeforeAllocation =
                greatestCommonDivisor((int) chunkSize, alignment) != alignment;
        cxt = new ThreadLocal<>();
    }

    @Override
    protected void cleanupOnClose() {
        super.cleanupOnClose();
        // Make GC life easier
        valueReader = null;
        valueDataAccess = null;
        cxt = null;
    }

    public final V checkValue(Object value) {
        Class<V> valueClass = valueClass();
        if (!valueClass.isInstance(value)) {
            throw new ClassCastException(toIdentityString() + ": Value must be a " +
                    valueClass.getName() + " but was a " + value.getClass());
        }
        return (V) value;
    }

    @Override
    public final long longSize() {
        long result = 0L;
        try (IterationContext<K, V, ?> c = iterationContext()) {
            for (int segmentIndex = 0; segmentIndex < segments(); segmentIndex++) {
                c.initSegmentIndex(segmentIndex);
                result += c.size();
            }
        }
        return result;
    }

    @Override
    public Class<K> keyClass() {
        return (Class<K>) keyClass;
    }

    @Override
    public Type keyType() {
        return keyClass;
    }

    @Override
    public Class<V> valueClass() {
        return (Class<V>) valueClass;
    }

    @Override
    public Type valueType() {
        return valueClass;
    }

    @NotNull
    @Override
    public final Closeable acquireContext(K key, V usingValue) {
        QueryContextInterface<K, V, R> q = queryContext(key);
        // TODO optimize to update lock in certain cases
        try {
            q.writeLock().lock();
            checkAcquiredUsing(acquireUsingBody(q, usingValue), usingValue);
            return q.acquireHandle();
        } catch (Throwable throwable) {
            try {
                q.close();
            } catch (Throwable t) {
                throwable.addSuppressed(t);
            }
            throw throwable;
        }
    }

    private void checkAcquiredUsing(V acquired, V using) {
        if (acquired != using) {
            throw new IllegalArgumentException(toIdentityString() +
                    ": acquire*() MUST reuse the given value. Given value " + using +
                    " cannot be reused to read " + acquired);
        }
    }

    @NotNull
    @Override
    public final Set<Entry<K, V>> entrySet() {
        return (entrySet != null) ? entrySet : (entrySet = newEntrySet());
    }

    @Override
    public int hashCode() {
        return mapHashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return mapEquals(obj);
    }

    @Override
    public String toString() {
        return mapToString();
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public String toIdentityString() {
        if (identityString == null)
            identityString = makeIdentityString();
        return identityString;
    }

    private String makeIdentityString() {
        if (chronicleSet != null)
            return chronicleSet.toIdentityString();
        return "ChronicleMap{" +
                "name=" + name() +
                ", file=" + file() +
                ", identityHashCode=" + System.identityHashCode(this) +
                "}";
    }

    @Override
    public void clear() {
        forEachEntry(c -> c.context().remove(c));
    }

    public final long readValueSize(Bytes entry) {
        long valueSize = valueSizeMarshaller.readSize(entry);
        alignReadPosition(entry);
        return valueSize;
    }

    public void alignReadPosition(Bytes entry) {
        long positionAddr = entry.addressForRead(entry.readPosition());
        long skip = alignAddr(positionAddr, alignment) - positionAddr;
        if (skip > 0)
            entry.readSkip(skip);
    }

    final ChainingInterface q() {
        ThreadLocal<ContextHolder> cxt = this.cxt;
        if (cxt == null)
            throw new ChronicleHashClosedException(this);
        ContextHolder contextHolder = cxt.get();
        if (contextHolder == null) {
            ChainingInterface queryContext = newQueryContext();
            try {
                contextHolder = new ContextHolder(queryContext);
                addContext(contextHolder);
                cxt.set(contextHolder);
                return queryContext;
            } catch (Throwable throwable) {
                try {
                    ((AutoCloseable) queryContext).close();
                } catch (Throwable t) {
                    throwable.addSuppressed(t);
                }
                throw throwable;
            }
        }
        ChainingInterface queryContext = contextHolder.get();
        if (queryContext != null) {
            return queryContext;
        } else {
            throw new ChronicleHashClosedException(this);
        }
    }

    ChainingInterface newQueryContext() {
        return new CompiledMapQueryContext<>(this);
    }

    public QueryContextInterface<K, V, R> mapContext() {
        //noinspection unchecked
        return q().getContext(CompiledMapQueryContext.class,
                // lambda is used instead of constructor reference because currently stage-compiler
                // has issues with parsing method/constructor refs.
                // TODO replace with constructor ref when stage-compiler is improved
                CompiledMapQueryContext::new, this);
    }

    final ChainingInterface i() {
        ThreadLocal<ContextHolder> cxt = this.cxt;
        if (cxt == null)
            throw new ChronicleHashClosedException(this);
        ContextHolder contextHolder = cxt.get();
        if (contextHolder == null) {
            ChainingInterface iterationContext = newIterationContext();
            try {
                contextHolder = new ContextHolder(iterationContext);
                addContext(contextHolder);
                cxt.set(contextHolder);
                return iterationContext;
            } catch (Throwable throwable) {
                try {
                    ((AutoCloseable) iterationContext).close();
                } catch (Throwable t) {
                    throwable.addSuppressed(t);
                }
                throw throwable;
            }
        }
        ChainingInterface iterationContext = contextHolder.get();
        if (iterationContext != null) {
            return iterationContext;
        } else {
            throw new ChronicleHashClosedException(this);
        }
    }

    ChainingInterface newIterationContext() {
        return new CompiledMapIterationContext<>(this);
    }

    public IterationContext<K, V, R> iterationContext() {
        //noinspection unchecked
        return i().getContext(
                CompiledMapIterationContext.class,
                // lambda is used instead of constructor reference because currently stage-compiler
                // has issues with parsing method/constructor refs.
                // TODO replace with constructor ref when stage-compiler is improved
                CompiledMapIterationContext::new, this);
    }

    @Override
    @NotNull
    public QueryContextInterface<K, V, R> queryContext(Object key) {
        checkKey(key);
        QueryContextInterface<K, V, R> c = mapContext();
        try {
            c.initInputKey(c.inputKeyDataAccess().getData((K) key));
            return c;
        } catch (Throwable throwable) {
            try {
                c.close();
            } catch (Throwable t) {
                throwable.addSuppressed(t);
            }
            throw throwable;
        }
    }

    @Override
    @NotNull
    public QueryContextInterface<K, V, R> queryContext(Data<K> key) {
        QueryContextInterface<K, V, R> c = mapContext();
        try {
            c.initInputKey(key);
            return c;
        } catch (Throwable throwable) {
            try {
                c.close();
            } catch (Throwable t) {
                throwable.addSuppressed(t);
            }
            throw throwable;
        }
    }

    @NotNull
    @Override
    public ExternalMapQueryContext<K, V, ?> queryContext(
            BytesStore keyBytes, long offset, long size) {
        Objects.requireNonNull(keyBytes);
        QueryContextInterface<K, V, R> c = mapContext();
        try {
            c.initInputKey(c.getInputKeyBytesAsData(keyBytes, offset, size));
            return c;
        } catch (Throwable throwable) {
            try {
                c.close();
            } catch (Throwable t) {
                throwable.addSuppressed(t);
            }
            throw throwable;
        }
    }

    @Override
    public MapSegmentContext<K, V, ?> segmentContext(int segmentIndex) {
        IterationContext<K, V, ?> c = iterationContext();
        try {
            c.initSegmentIndex(segmentIndex);
            return c;
        } catch (Throwable throwable) {
            try {
                c.close();
            } catch (Throwable t) {
                throwable.addSuppressed(t);
            }
            throw throwable;
        }
    }

    @Override
    public V get(Object key) {
        return defaultEntryOperationsAndMethods ? optimizedGet(key, null) : defaultGet(key);
    }

    final V defaultGet(Object key) {
        try (QueryContextInterface<K, V, R> q = queryContext(key)) {
            methods.get(q, q.defaultReturnValue());
            return q.defaultReturnValue().returnValue();
        }
    }

    private V optimizedGet(Object key, V using) {
        checkKey(key);
        CompiledMapQueryContext<K, V, R> c = (CompiledMapQueryContext<K, V, R>) mapContext();
        boolean needReadUnlock = false;
        Throwable primaryExc = null;
        long segmentHeaderAddress = 0;
        try {
            Data<K> inputKey = c.inputKeyDataAccess().getData((K) key);
            long inputKeySize = inputKey.size();

            long keyHash = inputKey.hash(LongHashFunction.xx_r39());
            HashSplitting hs = this.hashSplitting;
            int segmentIndex = hs.segmentIndex(keyHash);
            segmentHeaderAddress = segmentHeaderAddress(segmentIndex);
            CompactOffHeapLinearHashTable hl = this.hashLookup;
            long searchKey = hl.maskUnsetKey(hs.segmentHash(keyHash));
            long searchStartPos = hl.hlPos(searchKey);
            boolean needReadLock = true;
            initLocks:
            {
                int indexOfThisContext = c.indexInContextChain;
                for (int i = 0, size = c.contextChain.size(); i < size; i++) {
                    if (i == indexOfThisContext)
                        continue;
                    LocksInterface locks = ((LocksInterface) (c.contextChain.get(i)));
                    if (locks.segmentHeaderInit() &&
                            locks.segmentHeaderAddress() == segmentHeaderAddress &&
                            locks.locksInit()) {
                        LocksInterface root = locks.rootContextLockedOnThisSegment();
                        if (root.totalReadLockCount() > 0 || root.totalUpdateLockCount() > 0 ||
                                root.totalWriteLockCount() > 0) {
                            needReadLock = false;
                            break initLocks;
                        }
                    }
                }
            }
            if (needReadLock) {
                BigSegmentHeader.INSTANCE.readLock(segmentHeaderAddress);
                needReadUnlock = true;
            }
            return tieredValue(c, segmentHeaderAddress, segmentIndex, searchKey, searchStartPos,
                    inputKeySize, inputKey, using);
        } catch (Throwable t) {
            primaryExc = t;
            throw t;
        } finally {
            if (primaryExc != null) {
                try {
                    getClose(c, segmentHeaderAddress, needReadUnlock);
                } catch (Throwable suppressedExc) {
                    primaryExc.addSuppressed(suppressedExc);
                }
            } else {
                getClose(c, segmentHeaderAddress, needReadUnlock);
            }
        }
    }

    private void getClose(CompiledMapQueryContext<K, V, R> c, long segmentHeaderAddress,
                          boolean needReadUnlock) {
        Throwable thrown = null;
        try {
            if (needReadUnlock)
                BigSegmentHeader.INSTANCE.readUnlock(segmentHeaderAddress);
        } catch (Throwable t) {
            thrown = t;
        }
        try {
            c.doCloseUsed();
        } catch (Throwable t) {
            thrown = Throwables.returnOrSuppress(thrown, t);
        }
        try {
            c.doCloseInputValueDataAccess();
        } catch (Throwable t) {
            thrown = Throwables.returnOrSuppress(thrown, t);
        }
        if (thrown != null)
            throw Jvm.rethrow(thrown);
    }

    private V tieredValue(CompiledMapQueryContext<K, V, R> q,
                          long segmentHeaderAddress, int segmentIndex,
                          long searchKey, long searchStartPos,
                          long inputKeySize, Data<K> inputKey, V using) {
        int tier = 0;
        long tierBaseAddr = segmentBaseAddr(segmentIndex);
        while (true) {
            V value = searchValue(q, searchKey, searchStartPos, tierBaseAddr,
                    inputKeySize, inputKey, using);
            if (value != null)
                return value;
            long nextTierIndex;
            if (tier == 0) {
                nextTierIndex = BigSegmentHeader.INSTANCE.nextTierIndex(segmentHeaderAddress);
            } else {
                nextTierIndex = TierCountersArea.nextTierIndex(
                        tierBaseAddr + tierHashLookupOuterSize);
            }
            if (nextTierIndex == 0)
                return null;
            tier++;
            tierBaseAddr = tierIndexToBaseAddr(nextTierIndex);

        }
    }

    private V searchValue(CompiledMapQueryContext<K, V, R> q,
                          long searchKey, long searchStartPos, long tierBaseAddr,
                          long inputKeySize, Data<K> inputKey, V using) {
        CompactOffHeapLinearHashTable hl = this.hashLookup;

        PointerBytesStore segmentBytesStore = q.segmentBS;
        segmentBytesStore.set(tierBaseAddr, tierSize);
        Bytes bs = q.segmentBytes;
        bs.clear();
        long freeListOffset = tierHashLookupOuterSize + TIER_COUNTERS_AREA_SIZE;
        long entrySpaceOffset = freeListOffset + tierFreeListOuterSize + tierEntrySpaceInnerOffset;

        long hlPos = searchStartPos;
        searchLoop:
        while (true) {
            long entryPos;
            nextPos:
            {
                while (true) {
                    long entry = hl.readEntryVolatile(tierBaseAddr, hlPos);
                    if (hl.empty(entry)) {
                        break searchLoop;
                    }
                    hlPos = hl.step(hlPos);
                    if (hlPos == searchStartPos) {
                        throw new IllegalStateException(
                                toIdentityString() + ": HashLookup overflow should never occur");
                    }

                    if ((hl.key(entry)) == searchKey) {
                        entryPos = hl.value(entry);
                        break nextPos;
                    }
                }
            }

            long keySizeOffset = entrySpaceOffset + (entryPos * chunkSize);
            bs.readLimit(bs.capacity());
            bs.readPosition(keySizeOffset);
            long keySize = keySizeMarshaller.readSize(bs);
            long keyOffset = bs.readPosition();
            if (!((inputKeySize == keySize) &&
                    (inputKey.equivalent(segmentBytesStore, keyOffset)))) {
                continue;
            }
            long valueSizeOffset = keyOffset + keySize;
            bs.readPosition(valueSizeOffset);
            long valueSize = readValueSize(bs);
            return q.valueReader.read(bs, valueSize, using);
        }
        return null;
    }

    @Override
    public V getUsing(K key, V usingValue) {
        return defaultEntryOperationsAndMethods ? optimizedGet(key, usingValue) :
                defaultGetUsing(key, usingValue);
    }

    final V defaultGetUsing(K key, V usingValue) {
        try (QueryContextInterface<K, V, R> q = queryContext(key)) {
            q.usingReturnValue().initUsingReturnValue(usingValue);
            methods.get(q, q.usingReturnValue());
            return q.usingReturnValue().returnValue();
        }
    }

    @Override
    public V acquireUsing(K key, V usingValue) {
        try (QueryContextInterface<K, V, R> q = queryContext(key)) {
            return acquireUsingBody(q, usingValue);
        }
    }

    private V acquireUsingBody(QueryContextInterface<K, V, R> q, V usingValue) {
        q.usingReturnValue().initUsingReturnValue(usingValue);
        methods.acquireUsing(q, q.usingReturnValue());
        return q.usingReturnValue().returnValue();
    }

    @Override
    public V putIfAbsent(K key, V value) {
        checkValue(value);
        try (QueryContextInterface<K, V, R> q = queryContext(key)) {
            methods.putIfAbsent(q, q.inputValueDataAccess().getData(value), q.defaultReturnValue());
            return q.defaultReturnValue().returnValue();
        }
    }

    @Override
    public boolean remove(Object key, Object value) {
        if (value == null)
            return false; // ConcurrentHashMap compatibility
        V v = checkValue(value);
        try (QueryContextInterface<K, V, R> q = queryContext(key)) {
            return methods.remove(q, q.inputValueDataAccess().getData(v));
        }
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        checkValue(oldValue);
        checkValue(newValue);
        try (QueryContextInterface<K, V, R> q = queryContext(key)) {
            return methods.replace(
                    q, q.inputValueDataAccess().getData(oldValue), q.wrapValueAsData(newValue));
        }
    }

    @Override
    public V replace(K key, V value) {
        checkValue(value);
        try (QueryContextInterface<K, V, R> q = queryContext(key)) {
            methods.replace(q, q.inputValueDataAccess().getData(value), q.defaultReturnValue());
            return q.defaultReturnValue().returnValue();
        }
    }

    @Override
    public boolean containsKey(Object key) {
        try (QueryContextInterface<K, V, R> q = queryContext(key)) {
            return methods.containsKey(q);
        }
    }

    @Override
    public V put(K key, V value) {
        checkValue(value);
        try (QueryContextInterface<K, V, R> q = queryContext(key)) {
            Data<V> valueData = q.inputValueDataAccess().getData(value);
            InstanceReturnValue<V> returnValue =
                    putReturnsNull ? NullReturnValue.get() : q.defaultReturnValue();
            methods.put(q, valueData, returnValue);
            return returnValue.returnValue();
        }
    }

    @Override
    public V remove(Object key) {
        try (QueryContextInterface<K, V, R> q = queryContext(key)) {
            InstanceReturnValue<V> returnValue =
                    removeReturnsNull ? NullReturnValue.get() : q.defaultReturnValue();
            methods.remove(q, returnValue);
            return returnValue.returnValue();
        }
    }

    @Override
    public V merge(K key, V value,
                   BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
        try (QueryContextInterface<K, V, R> q = queryContext(key)) {
            methods.merge(q, q.inputValueDataAccess().getData(value), remappingFunction,
                    q.defaultReturnValue());
            return q.defaultReturnValue().returnValue();
        }
    }

    @Override
    public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
        try (QueryContextInterface<K, V, R> q = queryContext(key)) {
            methods.compute(q, remappingFunction, q.defaultReturnValue());
            return q.defaultReturnValue().returnValue();
        }
    }

    @Override
    public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
        try (QueryContextInterface<K, V, R> q = queryContext(key)) {
            methods.computeIfAbsent(q, mappingFunction, q.defaultReturnValue());
            return q.defaultReturnValue().returnValue();
        }
    }

    @Override
    public V computeIfPresent(K key,
                              BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
        try (QueryContextInterface<K, V, R> q = queryContext(key)) {
            methods.computeIfPresent(q, remappingFunction, q.defaultReturnValue());
            return q.defaultReturnValue().returnValue();
        }
    }

    public void verifyTierCountersAreaData() {
        for (int i = 0; i < actualSegments; i++) {
            try (MapSegmentContext<K, V, ?> c = segmentContext(i)) {
                Method verifyTierCountersAreaData =
                        c.getClass().getMethod("verifyTierCountersAreaData");
                verifyTierCountersAreaData.invoke(c);
            } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
                throw new AssertionError(e);
            }
        }
    }
}
