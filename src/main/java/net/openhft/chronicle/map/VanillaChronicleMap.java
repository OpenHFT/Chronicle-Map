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

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.IORuntimeException;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.impl.VanillaChronicleHash;
import net.openhft.chronicle.hash.impl.stage.hash.ChainingInterface;
import net.openhft.chronicle.hash.serialization.DataAccess;
import net.openhft.chronicle.hash.serialization.SizeMarshaller;
import net.openhft.chronicle.hash.serialization.SizedReader;
import net.openhft.chronicle.hash.serialization.impl.SerializationBuilder;
import net.openhft.chronicle.map.impl.*;
import net.openhft.chronicle.map.impl.CompiledMapIterationContext;
import net.openhft.chronicle.map.impl.CompiledMapQueryContext;
import net.openhft.chronicle.map.impl.ret.InstanceReturnValue;
import net.openhft.chronicle.values.Values;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

import static net.openhft.chronicle.map.ChronicleMapBuilder.greatestCommonDivisor;

public class VanillaChronicleMap<K, V, R>
        extends VanillaChronicleHash<K, MapEntry<K, V>, MapSegmentContext<K, V, ?>,
        ExternalMapQueryContext<K, V, ?>>
        implements AbstractChronicleMap<K, V> {

    private static final long serialVersionUID = 4L;

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
    
    public transient MapEntryOperations<K, V, R> entryOperations;
    public transient MapMethods<K, V, R> methods;
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
    protected void readMarshallableFields(@NotNull WireIn wireIn) throws IORuntimeException {
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

        this.entryOperations = (MapEntryOperations<K, V, R>) builder.entryOperations;
        this.methods = (MapMethods<K, V, R>) builder.methods;
        this.defaultValueProvider = builder.defaultValueProvider;
    }

    @Override
    public void initTransients() {
        super.initTransients();
        initOwnTransients();
    }

    private void initOwnTransients() {
        couldNotDetermineAlignmentBeforeAllocation =
                greatestCommonDivisor((int) chunkSize, alignment) != alignment;
        cxt = new ThreadLocal<>();
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        initOwnTransients();
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

    static <T> T newInstance(Class<T> aClass, boolean isKey) {
        try {
            // TODO optimize -- save heap class / direct class in transient fields and call
            // newInstance() on them directly
            return Values.newHeapInstance(aClass);

        } catch (Exception e) {
            if (e.getCause() instanceof IllegalStateException)
                throw e;

            if (aClass.isInterface())
                throw new IllegalStateException("It not possible to create a instance from " +
                        "interface=" + aClass.getSimpleName() + " we recommend you create an " +
                        "instance in the usual way.", e);

            try {
                return (T) aClass.newInstance();
            } catch (Exception e1) {
                throw new IllegalStateException("It has not been possible to create a instance " +
                        "of class=" + aClass.getSimpleName() +
                        ", Note : its more efficient if your chronicle map is configured with " +
                        "interface key " +
                        "and value types rather than classes, as this method is able to use " +
                        "interfaces to generate off heap proxies that point straight at your " +
                        "data. " +
                        "In this case you have used a class and chronicle is unable to create an " +
                        "instance of this class has it does not have a default constructor. " +
                        "If your class is mutable, we " +
                        "recommend you create and instance of your class=" +
                        aClass.getSimpleName() +
                        " in the usual way, rather than using this method.", e);
            }
        }
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

    void alignReadPosition(Bytes entry) {
        long positionAddr = entry.address(entry.readPosition());
        long skip = alignAddr(positionAddr, alignment) - positionAddr;
        if (skip > 0)
            entry.readSkip(skip);
    }

    public static long alignAddr(long addr, int alignment) {
        return (addr + alignment - 1) & ~(alignment - 1);
    }

    private ChainingInterface q() {
        ChainingInterface queryContext;
        queryContext = cxt.get();
        if (queryContext == null) {
            queryContext = new CompiledMapQueryContext<>(VanillaChronicleMap.this);
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

    @Override
    public MapSegmentContext<K, V, ?> segmentContext(int segmentIndex) {
        IterationContext<K, V, ?> c = iterationContext();
        c.initSegmentIndex(segmentIndex);
        return c;
    }

    @Override
    public V get(Object key) {
        try (QueryContextInterface<K, V, R> q = queryContext(key)) {
            methods.get(q, q.defaultReturnValue());
            return q.defaultReturnValue().returnValue();
        }
    }

    @Override
    public V getUsing(K key, V usingValue) {
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
}
