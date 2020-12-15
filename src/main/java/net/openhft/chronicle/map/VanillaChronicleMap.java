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

import net.openhft.chronicle.algo.bitset.ReusableBitSet;
import net.openhft.chronicle.algo.hashing.LongHashFunction;
import net.openhft.chronicle.analytics.Analytics;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.bytes.PointerBytesStore;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.analytics.AnalyticsFacade;
import net.openhft.chronicle.core.announcer.Announcer;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.hash.ChronicleHashClosedException;
import net.openhft.chronicle.hash.ChronicleHashCorruption;
import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.impl.*;
import net.openhft.chronicle.hash.impl.stage.entry.LocksInterface;
import net.openhft.chronicle.hash.impl.stage.hash.ChainingInterface;
import net.openhft.chronicle.hash.impl.util.Throwables;
import net.openhft.chronicle.hash.serialization.DataAccess;
import net.openhft.chronicle.hash.serialization.SizeMarshaller;
import net.openhft.chronicle.hash.serialization.SizedReader;
import net.openhft.chronicle.hash.serialization.impl.SerializationBuilder;
import net.openhft.chronicle.map.impl.*;
import net.openhft.chronicle.map.impl.ret.InstanceReturnValue;
import net.openhft.chronicle.map.internal.AnalyticsHolder;
import net.openhft.chronicle.set.ChronicleSet;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static net.openhft.chronicle.map.ChronicleMapBuilder.greatestCommonDivisor;

@SuppressWarnings("JavadocReference")
public class VanillaChronicleMap<K, V, R>
        extends VanillaChronicleHash<K, MapEntry<K, V>, MapSegmentContext<K, V, ?>, ExternalMapQueryContext<K, V, ?>>
        implements AbstractChronicleMap<K, V> {

    private double maxBloatFactor;

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
     * identityString is initialized lazily in {@link #toIdentityString()} rather than in {@link #initOwnTransients()} because it depends on {@link
     * #file()} which is set after initOwnTransients().
     */
    private transient String identityString;
    private transient boolean defaultEntryOperationsAndMethods;

    public VanillaChronicleMap(@NotNull final ChronicleMapBuilder<K, V> builder) {
        super(builder);
        SerializationBuilder<V> valueBuilder = builder.valueBuilder;
        valueClass = valueBuilder.tClass;
        valueSizeMarshaller = valueBuilder.sizeMarshaller();
        valueReader = valueBuilder.reader();
        valueDataAccess = valueBuilder.dataAccess();
        maxBloatFactor = builder.maxBloatFactor();

        constantlySizedEntry = builder.constantlySizedEntries();

        // Concurrency (number of segments), memory management and dependent fields
        alignment = builder.valueAlignment();
        worstAlignment = builder.worstAlignment();

        initTransientsFromBuilder(builder);
        initTransients();

        final Map<String, String> additionalEventParameters = AnalyticsFacade.standardAdditionalProperties();
        additionalEventParameters.put("key_type", keyClass.getTypeName());
        additionalEventParameters.put("value_type", valueClass.getTypeName());
        try {
            additionalEventParameters.put("entries", Long.toString(builder.entries()));
        } catch (RuntimeException ignored) {
            // The ChronicleMapBuilder::entries may throw an Exception. If so, just ignore this parameter
        }
        Announcer.announce("net.openhft", "chronicle-map",
                AnalyticsFacade.isEnabled()
                        ? singletonMap("Analytics", "Chronicle Map reports usage statistics. Learn more or turn off: https://github.com/OpenHFT/Chronicle-Map/blob/master/DISCLAIMER.adoc")
                        : emptyMap());
        AnalyticsHolder.instance().sendEvent("started", additionalEventParameters);
    }

    public static long alignAddr(final long addr, final long alignment) {
        return (addr + alignment - 1) & ~(alignment - 1L);
    }

    @Override
    protected void readMarshallableFields(@NotNull final WireIn wireIn) {
        super.readMarshallableFields(wireIn);

        valueClass = wireIn.read(() -> "valueClass").lenientTypeLiteral();
        valueSizeMarshaller = wireIn.read(() -> "valueSizeMarshaller").object(SizeMarshaller.class);
        valueReader = wireIn.read(() -> "valueReader").object(SizedReader.class);
        valueDataAccess = wireIn.read(() -> "valueDataAccess").object(DataAccess.class);

        constantlySizedEntry = wireIn.read(() -> "constantlySizedEntry").bool();

        alignment = wireIn.read(() -> "alignment").int32();
        worstAlignment = wireIn.read(() -> "worstAlignment").int32();
        maxBloatFactor = wireIn.read(() -> "maxBloatFactor").float64();
    }

    @Override
    public void writeMarshallable(@NotNull final WireOut wireOut) {

        super.writeMarshallable(wireOut);

        wireOut.write(() -> "valueClass").typeLiteral(valueClass);
        wireOut.write(() -> "valueSizeMarshaller").object(valueSizeMarshaller);
        wireOut.write(() -> "valueReader").object(valueReader);
        wireOut.write(() -> "valueDataAccess").object(valueDataAccess);

        wireOut.write(() -> "constantlySizedEntry").bool(constantlySizedEntry);

        wireOut.write(() -> "alignment").int32(alignment);
        wireOut.write(() -> "worstAlignment").int32(worstAlignment);
        wireOut.write(() -> "maxBloatFactor").float64(maxBloatFactor);
    }

    void initTransientsFromBuilder(@NotNull final ChronicleMapBuilder<K, V> builder) {
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
        throwExceptionIfClosed();

        super.initTransients();
        initOwnTransients();
    }

    public void recover(final ChronicleHashResources resources,
                        final ChronicleHashCorruption.Listener corruptionListener,
                        final ChronicleHashCorruptionImpl corruption) throws IOException {
        throwExceptionIfClosed();

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

    public final V checkValue(final Object value) {
        final Class<V> valueClass = valueClass();
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
        throwExceptionIfClosed();

        return (Class<K>) keyClass;
    }

    @Override
    public Type keyType() {
        throwExceptionIfClosed();

        return keyClass;
    }

    @Override
    public Class<V> valueClass() {
        throwExceptionIfClosed();

        return (Class<V>) valueClass;
    }

    @Override
    public Type valueType() {
        throwExceptionIfClosed();

        return valueClass;
    }

    private long tiersUsed() {
        return globalMutableState().getExtraTiersInUse();
    }

    private long maxTiers() {
        return (long) (maxBloatFactor * actualSegments);
    }

    @Override
    public int remainingAutoResizes() {
        throwExceptionIfClosed();

        return (int) (maxTiers() - tiersUsed());
    }

    @Override
    public short percentageFreeSpace() {
        throwExceptionIfClosed();

        double totalUsed = 0;
        double totalSize = 0;

        try (IterationContext<K, V, ?> c = iterationContext()) {
            for (int segmentIndex = 0; segmentIndex < segments(); segmentIndex++) {
                c.initSegmentIndex(segmentIndex);

                if (!(c instanceof CompiledMapIterationContext))
                    continue;
                final CompiledMapIterationContext c1 = (CompiledMapIterationContext) c;

                c1.goToFirstTier();
                final ReusableBitSet freeList = c1.freeList();
                totalUsed += freeList.cardinality();
                totalSize += freeList.logicalSize();

                while (c1.hasNextTier()) {
                    c1.nextTier();
                    final ReusableBitSet freeList0 = c1.freeList();
                    totalUsed += freeList0.cardinality();
                    totalSize += freeList.logicalSize();
                }
            }
        }

        return (short) (100 - (int) (100 * totalUsed / totalSize));
    }

    @NotNull
    @Override
    public final MapClosable acquireContext(@NotNull final K key, @NotNull final V usingValue) {
        final QueryContextInterface<K, V, R> q = queryContext(key);
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

    private void checkAcquiredUsing(final V acquired, final V using) {
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
        throwExceptionIfClosed();

        return mapHashCode();
    }

    @Override
    public boolean equals(final Object obj) {
        throwExceptionIfClosed();

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

    @NotNull
    @Override
    public String toIdentityString() {
        throwExceptionIfClosed();

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
        throwExceptionIfClosed();

        forEachEntry(c -> c.context().remove(c));
    }

    public final long readValueSize(final Bytes<?> entry) {
        final long valueSize = valueSizeMarshaller.readSize(entry);
        alignReadPosition(entry);
        return valueSize;
    }

    public void alignReadPosition(Bytes<?> entry) {
        throwExceptionIfClosed();

        final long positionAddr = entry.addressForRead(entry.readPosition());
        final long skip = alignAddr(positionAddr, alignment) - positionAddr;
        if (skip > 0)
            entry.readSkip(skip);
    }

    final ChainingInterface q() {
        final ThreadLocal<ContextHolder> cxt = this.cxt;
        if (cxt == null)
            throw new ChronicleHashClosedException(this);
        ContextHolder contextHolder = cxt.get();
        if (contextHolder == null) {
            final ChainingInterface queryContext = newQueryContext();
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
        final ChainingInterface queryContext = contextHolder.get();
        if (queryContext != null) {
            return queryContext;
        } else {
            throw new ChronicleHashClosedException(this);
        }
    }

    ChainingInterface newQueryContext() {
        CompiledMapQueryContext<Object, Object, Object> context = new CompiledMapQueryContext<>(this);
        IOTools.unmonitor(context.segmentBytes);
        return context;
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
        final ThreadLocal<ContextHolder> cxt = this.cxt;
        if (cxt == null)
            throw new ChronicleHashClosedException(this);
        ContextHolder contextHolder = cxt.get();
        if (contextHolder == null) {
            final ChainingInterface iterationContext = newIterationContext();
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
        final ChainingInterface iterationContext = contextHolder.get();
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
    public QueryContextInterface<K, V, R> queryContext(final Object key) {
        checkKey(key);
        final QueryContextInterface<K, V, R> c = mapContext();
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
    public QueryContextInterface<K, V, R> queryContext(final Data<K> key) {
        final QueryContextInterface<K, V, R> c = mapContext();
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
    public ExternalMapQueryContext<K, V, ?> queryContext(@NotNull final BytesStore keyBytes,
                                                         final long offset,
                                                         final long size) {
        final QueryContextInterface<K, V, R> c = mapContext();
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
    public MapSegmentContext<K, V, ?> segmentContext(final int segmentIndex) {
        final IterationContext<K, V, ?> c = iterationContext();
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
        throwExceptionIfClosed();

        return defaultEntryOperationsAndMethods ? optimizedGet(key, null) : defaultGet(key);
    }

    final V defaultGet(final Object key) {
        try (QueryContextInterface<K, V, R> q = queryContext(key)) {
            methods.get(q, q.defaultReturnValue());
            return q.defaultReturnValue().returnValue();
        }
    }

    private V optimizedGet(final Object key, final V using) {
        checkKey(key);
        final CompiledMapQueryContext<K, V, R> c = (CompiledMapQueryContext<K, V, R>) mapContext();
        boolean needReadUnlock = false;
        Throwable primaryExc = null;
        long segmentHeaderAddress = 0;
        try {
            final Data<K> inputKey = c.inputKeyDataAccess().getData((K) key);
            final long inputKeySize = inputKey.size();

            final long keyHash = inputKey.hash(LongHashFunction.xx_r39());
            final HashSplitting hs = this.hashSplitting;
            final int segmentIndex = hs.segmentIndex(keyHash);
            segmentHeaderAddress = segmentHeaderAddress(segmentIndex);
            final CompactOffHeapLinearHashTable hl = this.hashLookup;
            final long searchKey = hl.maskUnsetKey(hs.segmentHash(keyHash));
            final long searchStartPos = hl.hlPos(searchKey);
            boolean needReadLock = true;
            initLocks:
            {
                final int indexOfThisContext = c.indexInContextChain;
                for (int i = 0, size = c.contextChain.size(); i < size; i++) {
                    if (i == indexOfThisContext)
                        continue;
                    final LocksInterface locks = ((LocksInterface) (c.contextChain.get(i)));
                    if (locks.segmentHeaderInit() &&
                            locks.segmentHeaderAddress() == segmentHeaderAddress &&
                            locks.locksInit()) {
                        final LocksInterface root = locks.rootContextLockedOnThisSegment();
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

    private void getClose(@NotNull final CompiledMapQueryContext<K, V, R> c,
                          final long segmentHeaderAddress,
                          final boolean needReadUnlock) {
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

    private V tieredValue(final CompiledMapQueryContext<K, V, R> q,
                          final long segmentHeaderAddress,
                          final int segmentIndex,
                          final long searchKey,
                          final long searchStartPos,
                          final long inputKeySize, Data<K> inputKey,
                          final V using) {
        int tier = 0;
        long tierBaseAddr = segmentBaseAddr(segmentIndex);
        while (true) {
            final V value = searchValue(q, searchKey, searchStartPos, tierBaseAddr,
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

    private V searchValue(final CompiledMapQueryContext<K, V, R> q,
                          final long searchKey,
                          final long searchStartPos,
                          final long tierBaseAddr,
                          final long inputKeySize, Data<K> inputKey, V using) {
        final CompactOffHeapLinearHashTable hl = this.hashLookup;

        final PointerBytesStore segmentBytesStore = q.segmentBS;
        segmentBytesStore.set(tierBaseAddr, tierSize);
        final Bytes<?> bs = q.segmentBytes;
        bs.clear();
        final long freeListOffset = tierHashLookupOuterSize + TIER_COUNTERS_AREA_SIZE;
        final long entrySpaceOffset = freeListOffset + tierFreeListOuterSize + tierEntrySpaceInnerOffset;

        long hlPos = searchStartPos;
        searchLoop:
        while (true) {
            long entryPos;
            nextPos:
            {
                while (true) {
                    final long entry = hl.readEntryVolatile(tierBaseAddr, hlPos);
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

            final long keySizeOffset = entrySpaceOffset + (entryPos * chunkSize);
            bs.readLimitToCapacity();
            bs.readPosition(keySizeOffset);
            final long keySize = keySizeMarshaller.readSize(bs);
            final long keyOffset = bs.readPosition();
            if (!((inputKeySize == keySize) &&
                    (inputKey.equivalent(segmentBytesStore, keyOffset)))) {
                continue;
            }
            final long valueSizeOffset = keyOffset + keySize;
            bs.readPosition(valueSizeOffset);
            final long valueSize = readValueSize(bs);
            return q.valueReader.read(bs, valueSize, using);
        }
        return null;
    }

    @Override
    public V getUsing(final K key, final V usingValue) {
        throwExceptionIfClosed();

        return defaultEntryOperationsAndMethods
                ? optimizedGet(key, usingValue)
                : defaultGetUsing(key, usingValue);
    }

    final V defaultGetUsing(final K key, final V usingValue) {
        try (QueryContextInterface<K, V, R> q = queryContext(key)) {
            q.usingReturnValue().initUsingReturnValue(usingValue);
            methods.get(q, q.usingReturnValue());
            return q.usingReturnValue().returnValue();
        }
    }

    @Override
    public V acquireUsing(@NotNull final K key, final V usingValue) {
        throwExceptionIfClosed();

        try (QueryContextInterface<K, V, R> q = queryContext(key)) {
            return acquireUsingBody(q, usingValue);
        }
    }

    private V acquireUsingBody(@NotNull final QueryContextInterface<K, V, R> q, final V usingValue) {
        q.usingReturnValue().initUsingReturnValue(usingValue);
        methods.acquireUsing(q, q.usingReturnValue());
        return q.usingReturnValue().returnValue();
    }

    @Override
    public V putIfAbsent(@NotNull final K key, final V value) {
        throwExceptionIfClosed();

        checkValue(value);
        try (QueryContextInterface<K, V, R> q = queryContext(key)) {
            methods.putIfAbsent(q, q.inputValueDataAccess().getData(value), q.defaultReturnValue());
            return q.defaultReturnValue().returnValue();
        }
    }

    @Override
    public boolean remove(@NotNull final Object key, final Object value) {
        throwExceptionIfClosed();

        if (value == null)
            return false; // ConcurrentHashMap compatibility
        final V v = checkValue(value);
        try (QueryContextInterface<K, V, R> q = queryContext(key)) {
            return methods.remove(q, q.inputValueDataAccess().getData(v));
        }
    }

    @Override
    public boolean replace(@NotNull final K key,
                           @NotNull final V oldValue,
                           @NotNull final V newValue) {
        throwExceptionIfClosed();

        checkValue(oldValue);
        checkValue(newValue);
        try (QueryContextInterface<K, V, R> q = queryContext(key)) {
            return methods.replace(
                    q, q.inputValueDataAccess().getData(oldValue), q.wrapValueAsData(newValue));
        }
    }

    @Override
    public V replace(@NotNull final K key, @NotNull final V value) {
        throwExceptionIfClosed();

        checkValue(value);
        try (QueryContextInterface<K, V, R> q = queryContext(key)) {
            methods.replace(q, q.inputValueDataAccess().getData(value), q.defaultReturnValue());
            return q.defaultReturnValue().returnValue();
        }
    }

    @Override
    public boolean containsKey(final Object key) {
        throwExceptionIfClosed();

        try (QueryContextInterface<K, V, R> q = queryContext(key)) {
            return methods.containsKey(q);
        }
    }

    @Override
    public V put(final K key, final V value) {
        throwExceptionIfClosed();

        checkValue(value);
        try (QueryContextInterface<K, V, R> q = queryContext(key)) {
            final Data<V> valueData = q.inputValueDataAccess().getData(value);
            final InstanceReturnValue<V> returnValue = putReturnsNull
                    ? NullReturnValue.get()
                    : q.defaultReturnValue();
            methods.put(q, valueData, returnValue);
            return returnValue.returnValue();
        }
    }

    @Override
    public V remove(final Object key) {
        throwExceptionIfClosed();

        try (QueryContextInterface<K, V, R> q = queryContext(key)) {
            final InstanceReturnValue<V> returnValue = removeReturnsNull
                    ? NullReturnValue.get()
                    : q.defaultReturnValue();
            methods.remove(q, returnValue);
            return returnValue.returnValue();
        }
    }

    @Override
    public V merge(final K key,
                   final V value,
                   final BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
        throwExceptionIfClosed();

        try (QueryContextInterface<K, V, R> q = queryContext(key)) {
            methods.merge(q, q.inputValueDataAccess().getData(value), remappingFunction,
                    q.defaultReturnValue());
            return q.defaultReturnValue().returnValue();
        }
    }

    @Override
    public V compute(final K key, final BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
        throwExceptionIfClosed();

        try (QueryContextInterface<K, V, R> q = queryContext(key)) {
            methods.compute(q, remappingFunction, q.defaultReturnValue());
            return q.defaultReturnValue().returnValue();
        }
    }

    @Override
    public V computeIfAbsent(final K key, final Function<? super K, ? extends V> mappingFunction) {
        throwExceptionIfClosed();

        try (QueryContextInterface<K, V, R> q = queryContext(key)) {
            methods.computeIfAbsent(q, mappingFunction, q.defaultReturnValue());
            return q.defaultReturnValue().returnValue();
        }
    }

    @Override
    public V computeIfPresent(final K key,
                              final BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
        throwExceptionIfClosed();

        try (QueryContextInterface<K, V, R> q = queryContext(key)) {
            methods.computeIfPresent(q, remappingFunction, q.defaultReturnValue());
            return q.defaultReturnValue().returnValue();
        }
    }

    public void verifyTierCountersAreaData() {
        throwExceptionIfClosed();

        for (int i = 0; i < actualSegments; i++) {
            try (MapSegmentContext<K, V, ?> c = segmentContext(i)) {
                final Method verifyTierCountersAreaData =
                        c.getClass().getMethod("verifyTierCountersAreaData");
                verifyTierCountersAreaData.invoke(c);
            } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
                throw new AssertionError(e);
            }
        }
    }
}