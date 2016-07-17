/*
 *      Copyright (C) 2015  higherfrequencytrading.com
 *
 *      This program is free software: you can redistribute it and/or modify
 *      it under the terms of the GNU Lesser General Public License as published by
 *      the Free Software Foundation, either version 3 of the License.
 *
 *      This program is distributed in the hope that it will be useful,
 *      but WITHOUT ANY WARRANTY; without even the implied warranty of
 *      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *      GNU Lesser General Public License for more details.
 *
 *      You should have received a copy of the GNU Lesser General Public License
 *      along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.map;

import net.openhft.chronicle.algo.hashing.LongHashFunction;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.bytes.PointerBytesStore;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.impl.*;
import net.openhft.chronicle.hash.impl.stage.entry.LocksInterface;
import net.openhft.chronicle.hash.impl.stage.hash.ChainingInterface;
import net.openhft.chronicle.hash.impl.util.Objects;
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
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

import static net.openhft.chronicle.map.ChronicleMapBuilder.greatestCommonDivisor;

public class VanillaChronicleMap<K, V, R>
        extends VanillaChronicleHash<K, MapEntry<K, V>, MapSegmentContext<K, V, ?>,
        ExternalMapQueryContext<K, V, ?>>
        implements AbstractChronicleMap<K, V> {

    /////////////////////////////////////////////////
    // Value Data model
    Class<V> valueClass;
    public SizeMarshaller valueSizeMarshaller;
    public SizedReader<V> valueReader;
    public DataAccess<V> valueDataAccess;

    public boolean constantlySizedEntry;

    /////////////////////////////////////////////////
    // Memory management and dependent fields
    public int alignment;
    public int worstAlignment;

    public transient boolean couldNotDetermineAlignmentBeforeAllocation;

    /////////////////////////////////////////////////
    // Behavior
    transient boolean putReturnsNull;
    transient boolean removeReturnsNull;

    transient Set<Entry<K, V>> entrySet;

    /** @see net.openhft.chronicle.set.SetFromMap */
    public transient ChronicleSet<K> chronicleSet;
    
    public transient MapEntryOperations<K, V, R> entryOperations;
    public transient MapMethods<K, V, R> methods;
    private transient boolean defaultEntryOperationsAndMethods;
    public transient DefaultValueProvider<K, V> defaultValueProvider;
    
    transient ThreadLocal<ChainingInterface> cxt;

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

    @Override
    protected void readMarshallableFields(@NotNull WireIn wireIn) {
        super.readMarshallableFields(wireIn);

        valueClass = wireIn.read(() -> "valueClass").typeLiteral();
        valueSizeMarshaller = wireIn.read(() -> "valueSizeMarshaller").typedMarshallable();
        valueReader = wireIn.read(() -> "valueReader").typedMarshallable();
        valueDataAccess = wireIn.read(() -> "valueDataAccess").typedMarshallable();

        constantlySizedEntry = wireIn.read(() -> "constantlySizedEntry").bool();

        alignment = wireIn.read(() -> "alignment").int32();
        worstAlignment = wireIn.read(() -> "worstAlignment").int32();
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wireOut) {
        super.writeMarshallable(wireOut);

        wireOut.write(() -> "valueClass").typeLiteral(valueClass);
        wireOut.write(() -> "valueSizeMarshaller").typedMarshallable(valueSizeMarshaller);
        wireOut.write(() -> "valueReader").typedMarshallable(valueReader);
        wireOut.write(() -> "valueDataAccess").typedMarshallable(valueDataAccess);

        wireOut.write(() -> "constantlySizedEntry").bool(constantlySizedEntry);

        wireOut.write(() -> "alignment").int32(alignment);
        wireOut.write(() -> "worstAlignment").int32(worstAlignment);
    }

    void initTransientsFromBuilder(ChronicleMapBuilder<K, V> builder) {
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

    public void recover() throws IOException {
        basicRecover();
        iterationContext().recoverSegments();
    }

    private void initOwnTransients() {
        couldNotDetermineAlignmentBeforeAllocation =
                greatestCommonDivisor((int) chunkSize, alignment) != alignment;
        cxt = new ThreadLocal<>();
    }

    public final V checkValue(Object value) {
        if (!valueClass.isInstance(value)) {
            throw new ClassCastException("Value must be a " + valueClass.getName() +
                    " but was a " + value.getClass());
        }
        return (V) value;
    }

    @Override
    public Class<K> keyClass() {
        return keyClass;
    }

    @Override
    public Class<V> valueClass() {
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
        } catch (Throwable e) {
            try {
                q.close();
            } catch (Throwable suppressed) {
                e.addSuppressed(suppressed);
            }
            throw e;
        }
    }

    private static <V> void checkAcquiredUsing(V acquired, V using) {
        if (acquired != using) {
            throw new IllegalArgumentException("acquire*() MUST reuse the given " +
                    "value. Given value " + using + " cannot be reused to read " + acquired);
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
    public void clear() {
        forEachEntry(c -> c.context().remove(c));
    }

    public final long readValueSize(Bytes entry) {
        long valueSize = valueSizeMarshaller.readSize(entry);
        alignReadPosition(entry);
        return valueSize;
    }

    public void alignReadPosition(Bytes entry) {
        long positionAddr = entry.address(entry.readPosition());
        long skip = alignAddr(positionAddr, alignment) - positionAddr;
        if (skip > 0)
            entry.readSkip(skip);
    }

    public static long alignAddr(long addr, long alignment) {
        return (addr + alignment - 1) & ~(alignment - 1L);
    }

    private ChainingInterface q() {
        ChainingInterface queryContext;
        queryContext = cxt.get();
        if (queryContext == null) {
            queryContext = new CompiledMapQueryContext<>(VanillaChronicleMap.this);
            addContext(queryContext);
            cxt.set(queryContext);
        }
        return queryContext;
    }

    public QueryContextInterface<K, V, R> mapContext() {
        return q().getContext(CompiledMapQueryContext.class,
                ci -> new CompiledMapQueryContext<>(ci, this));
    }

    private ChainingInterface i() {
        ChainingInterface iterContext;
        iterContext = cxt.get();
        if (iterContext == null) {
            iterContext = new CompiledMapIterationContext<>(VanillaChronicleMap.this);
            addContext(iterContext);
            cxt.set(iterContext);
        }
        return iterContext;
    }

    public IterationContext<K, V, ?> iterationContext() {
        return i().getContext(CompiledMapIterationContext.class,
                ci -> new CompiledMapIterationContext<>(ci, this));
    }

    @Override
    @NotNull
    public QueryContextInterface<K, V, R> queryContext(Object key) {
        checkKey(key);
        QueryContextInterface<K, V, R> q = mapContext();
        q.initInputKey(q.inputKeyDataAccess().getData((K) key));
        return q;
    }

    @Override
    @NotNull
    public QueryContextInterface<K, V, R> queryContext(Data<K> key) {
        QueryContextInterface<K, V, R> q = mapContext();
        q.initInputKey(key);
        return q;
    }

    @NotNull
    @Override
    public ExternalMapQueryContext<K, V, ?> queryContext(
            BytesStore keyBytes, long offset, long size) {
        Objects.requireNonNull(keyBytes);
        QueryContextInterface<K, V, R> q = mapContext();
        q.initInputKey(q.getInputKeyBytesAsData(keyBytes, offset, size));
        return q;
    }

    @Override
    public MapSegmentContext<K, V, ?> segmentContext(int segmentIndex) {
        IterationContext<K, V, ?> c = iterationContext();
        c.initSegmentIndex(segmentIndex);
        return c;
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
        CompiledMapQueryContext<K, V, R> q = (CompiledMapQueryContext<K, V, R>) mapContext();
        Data<K> inputKey = q.inputKeyDataAccess().getData((K) key);
        try {
            Throwable primaryExc = null;
            long inputKeySize = inputKey.size();

            long keyHash = inputKey.hash(LongHashFunction.xx_r39());
            HashSplitting hs = this.hashSplitting;
            int segmentIndex = hs.segmentIndex(keyHash);
            long segmentHeaderAddress = segmentHeaderAddress(segmentIndex);
            boolean needReadUnlock = false;
            try {
                CompactOffHeapLinearHashTable hl = this.hashLookup;
                long searchKey = hl.maskUnsetKey(hs.segmentHash(keyHash));
                long searchStartPos = hl.hlPos(searchKey);
                boolean needReadLock = true;
                initLocks:
                {
                    int indexOfThisContext = q.indexInContextChain;
                    for (int i = 0, size = q.contextChain.size(); i < size; i++) {
                        if (i == indexOfThisContext)
                            continue;
                        LocksInterface c = ((LocksInterface) (q.contextChain.get(i)));
                        if (c.segmentHeaderInit() &&
                                c.segmentHeaderAddress() == segmentHeaderAddress &&
                                c.locksInit()) {
                            LocksInterface root = c.rootContextLockedOnThisSegment();
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
                return tieredValue(q, segmentHeaderAddress, segmentIndex, searchKey, searchStartPos,
                        inputKeySize, inputKey, using);
            } catch (Throwable t) {
                primaryExc = t;
                throw t;
            } finally {
                if (primaryExc != null) {
                    try {
                        getClose(q, segmentHeaderAddress, needReadUnlock);
                    } catch (Throwable suppressedExc) {
                        primaryExc.addSuppressed(suppressedExc);
                    }
                } else {
                    getClose(q, segmentHeaderAddress, needReadUnlock);
                }
            }
        } finally {
            q.doCloseInputKeyDataAccess();
        }
    }

    private void getClose(CompiledMapQueryContext<K, V, R> q, long segmentHeaderAddress,
                          boolean needReadUnlock) {
        if (needReadUnlock)
            BigSegmentHeader.INSTANCE.readUnlock(segmentHeaderAddress);
        q.doCloseUsed();
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
            nextPos: {
                while (true) {
                    long entry = hl.readEntryVolatile(tierBaseAddr, hlPos);
                    if (hl.empty(entry)) {
                        break searchLoop;
                    }
                    hlPos = hl.step(hlPos);
                    if (hlPos == searchStartPos)
                        throw new IllegalStateException("HashLookup overflow should never occur");

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
