/*
 * Copyright 2014 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
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

import net.openhft.chronicle.hash.KeyContext;
import net.openhft.chronicle.hash.impl.ContextFactory;
import net.openhft.chronicle.hash.impl.HashContext;
import net.openhft.chronicle.hash.impl.VanillaChronicleHash;
import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.chronicle.hash.serialization.SizeMarshaller;
import net.openhft.chronicle.hash.serialization.internal.*;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.model.DataValueClasses;
import net.openhft.lang.threadlocal.Provider;
import net.openhft.lang.threadlocal.ThreadLocalCopies;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Map;
import java.util.Set;

import static net.openhft.chronicle.map.ChronicleMapBuilder.greatestCommonDivisor;

class VanillaChronicleMap<K, KI, MKI extends MetaBytesInterop<K, ? super KI>,
        V, VI, MVI extends MetaBytesInterop<V, ? super VI>>
        extends VanillaChronicleHash<K, KI, MKI, MapKeyContext<K, V>>
        implements AbstractChronicleMap<K, V>  {

    private static final Logger LOG = LoggerFactory.getLogger(VanillaChronicleMap.class);

    private static final long serialVersionUID = 3L;

    /////////////////////////////////////////////////
    // Value Data model
    final Class<V> vClass;
    final Class nativeValueClass;
    final SizeMarshaller valueSizeMarshaller;
    final BytesReader<V> originalValueReader;
    final VI originalValueInterop;
    final MVI originalMetaValueInterop;
    final MetaProvider<V, VI, MVI> metaValueInteropProvider;

    final DefaultValueProvider<K, V> defaultValueProvider;

    final boolean constantlySizedEntry;

    transient Provider<BytesReader<V>> valueReaderProvider;
    transient Provider<VI> valueInteropProvider;

    /////////////////////////////////////////////////
    // Event listener and meta data
    final int metaDataBytes;

    /////////////////////////////////////////////////
    // Behavior
    final boolean putReturnsNull;
    final boolean removeReturnsNull;

    /////////////////////////////////////////////////
    // Memory management and dependent fields
    final Alignment alignment;
    final int worstAlignment;
    final boolean couldNotDetermineAlignmentBeforeAllocation;

    transient Set<Map.Entry<K, V>> entrySet;

    public VanillaChronicleMap(ChronicleMapBuilder<K, V> builder, boolean replicated)
            throws IOException {
        super(builder, replicated);
        SerializationBuilder<V> valueBuilder = builder.valueBuilder;
        vClass = valueBuilder.eClass;
        if (vClass.getName().endsWith("$$Native")) {
            nativeValueClass = vClass;

        } else if (vClass.isInterface()) {
            Class nativeValueClass = null;
            try {
                nativeValueClass = DataValueClasses.directClassFor(vClass);
            } catch (Exception e) {
                // fall through
            }
            this.nativeValueClass = nativeValueClass;

        } else {
            nativeValueClass = null;
        }
        valueSizeMarshaller = valueBuilder.sizeMarshaller();
        originalValueReader = valueBuilder.reader();
        originalValueInterop = (VI) valueBuilder.interop();
        originalMetaValueInterop = (MVI) valueBuilder.metaInterop();
        metaValueInteropProvider = (MetaProvider) valueBuilder.metaInteropProvider();

        defaultValueProvider = builder.defaultValueProvider();

        constantlySizedEntry = builder.constantlySizedEntries();

        // Event listener and meta data
        metaDataBytes = builder.metaDataBytes();

        // Behavior
        putReturnsNull = builder.putReturnsNull();
        removeReturnsNull = builder.removeReturnsNull();

        // Concurrency (number of segments), memory management and dependent fields
        alignment = builder.valueAlignment();
        worstAlignment = builder.worstAlignment(replicated);
        int alignment = this.alignment.alignment();
        couldNotDetermineAlignmentBeforeAllocation =
                greatestCommonDivisor((int) chunkSize, alignment) != alignment;

        initTransients();
    }

    @Override
    public void initTransients() {
        super.initTransients();
        ownInitTransients();
    }

    private void ownInitTransients() {
        valueReaderProvider = Provider.of((Class) originalValueReader.getClass());
        valueInteropProvider = Provider.of((Class) originalValueInterop.getClass());

        if (defaultValueProvider instanceof ConstantValueProvider) {
            ConstantValueProvider<K, V> constantValueProvider =
                    (ConstantValueProvider<K, V>) defaultValueProvider;
            if (constantValueProvider.wasDeserialized()) {
                ThreadLocalCopies copies = valueReaderProvider.getCopies(null);
                BytesReader<V> valueReader = valueReaderProvider.get(copies, originalValueReader);
                constantValueProvider.initTransients(valueReader);
            }
        }
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        ownInitTransients();
    }

    @Override
    public final void checkValue(Object value) {
        if (!vClass.isInstance(value)) {
            throw new ClassCastException("Value must be a " + vClass.getName() +
                    " but was a " + value.getClass());
        }
    }

    @Override
    public VanillaContext<K, KI, MKI, V, VI, MVI> mapContext() {
        VanillaContext<K, KI, MKI, V, VI, MVI> context = rawContext();
        context.initHash(this);
        return context;
    }

    VanillaContext bytesMapContext() {
        VanillaContext context = rawBytesContext();
        context.initHash(this);
        return context;
    }

    @Override
    public VanillaContext<K, KI, MKI, V, VI, MVI> context(K key) {
        VanillaContext<K, KI, MKI, V, VI, MVI> context = mapContext();
        context.initKey(key);
        return context;
    }

    @Override
    public void putDefaultValue(VanillaContext context) {
        context.doPut(defaultValue(context));
    }

    @Override
    public int actualSegments() {
        return actualSegments;
    }

    VanillaContext<K, KI, MKI, V, VI, MVI> rawContext() {
        return VanillaContext.get(VanillaContext.VanillaChronicleMapContextFactory.INSTANCE);
    }

    VanillaContext rawBytesContext() {
        return HashContext.get(BytesContextFactory.INSTANCE);
    }

    V defaultValue(KeyContext keyContext) {
        if (defaultValueProvider != null)
            return defaultValueProvider.get(keyContext);
        throw new IllegalStateException("To call acquire*() methods, " +
                "you should specify either default value or default value provider " +
                "during map building");
    }

    @Override
    public V prevValueOnPut(MapKeyContext<K, V> context) {
        return putReturnsNull ? null : AbstractChronicleMap.super.prevValueOnPut(context);
    }

    @Override
    public V prevValueOnRemove(MapKeyContext<K, V> context) {
        return removeReturnsNull ? null : AbstractChronicleMap.super.prevValueOnRemove(context);
    }

    @Override
    public V newValueInstance() {
        if (vClass == CharSequence.class || vClass == StringBuilder.class)
            return (V) new StringBuilder();
        return newInstance(vClass, false);
    }

    @Override
    public K newKeyInstance() {
        return newInstance(kClass, true);
    }

    @Override
    public Class<K> keyClass() {
        return kClass;
    }

    @Override
    public Class<V> valueClass() {
        return vClass;
    }

    static <T> T newInstance(Class<T> aClass, boolean isKey) {
        try {
            // TODO optimize -- save heap class / direct class in transient fields and call
            // newInstance() on them directly
            return isKey ? DataValueClasses.newInstance(aClass) :
                    DataValueClasses.newDirectInstance(aClass);
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
                        "interfaces to generate off heap proxies that point straight at your data. " +
                        "In this case you have used a class and chronicle is unable to create an " +
                        "instance of this class has it does not have a default constructor. " +
                        "If your class is mutable, we " +
                        "recommend you create and instance of your class=" + aClass.getSimpleName() +
                        " in the usual way, rather than using this method.", e);
            }
        }
    }

    @NotNull
    @Override
    public final MapKeyContext<K, V> acquireContext(K key, V usingValue) {
        VanillaContext<K, KI, MKI, V, VI, MVI> c = acquireContext(key);
        // TODO optimize to update lock in certain cases
        c.writeLock().lock();
        try {
            if (!c.containsKey()) {
                c.doPut(defaultValue(c));
            }
            V value = c.getUsing(usingValue);
            if (value != usingValue) {
                throw new IllegalArgumentException("acquireContext MUST reuse the given " +
                        "value. Given value" + usingValue + " cannot be reused to read " + value);
            }
            return c;
        } catch (Throwable e) {
            try {
                c.closeHash();
            } catch (Throwable suppressed) {
                e.addSuppressed(suppressed);
            }
            throw e;
        }
    }

    VanillaContext<K, KI, MKI, V, VI, MVI> acquireContext(K key) {
        AcquireContext<K, KI, MKI, V, VI, MVI> c =
                VanillaContext.get(AcquireContextFactory.INSTANCE);
        c.initHash(this);
        c.initKey(key);
        return c;
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

    static class AcquireContext<K, KI, MKI extends MetaBytesInterop<K, ? super KI>,
            V, VI, MVI extends MetaBytesInterop<V, ? super VI>>
            extends VanillaContext<K, KI, MKI, V, VI, MVI> {
        AcquireContext() {
            super();
        }

        AcquireContext(HashContext contextCache, int indexInContextCache) {
            super(contextCache, indexInContextCache);
        }

        @Override
        public boolean put(V newValue) {
            return acquirePut(newValue);
        }

        @Override
        public boolean remove() {
            return acquireRemove();
        }

        @Override
        public void close() {
            acquireClose();
        }
    }

    enum AcquireContextFactory implements ContextFactory<AcquireContext> {
        INSTANCE;

        @Override
        public AcquireContext createContext(HashContext root, int indexInContextCache) {
            return new AcquireContext(root, indexInContextCache);
        }

        @Override
        public AcquireContext createRootContext() {
            return new AcquireContext();
        }

        @Override
        public Class<AcquireContext> contextClass() {
            return AcquireContext.class;
        }
    }

    @Override
    public void clear() {
        VanillaContext<K, KI, MKI, V, VI, MVI> context = mapContext();
        for (int i = 0; i < actualSegments; i++) {
            context.segmentIndex = i;
            try {
                context.clear();
            } finally {
                context.closeSegmentIndex();
            }
        }
    }

    final long readValueSize(Bytes entry) {
        long valueSize = valueSizeMarshaller.readSize(entry);
        alignment.alignPositionAddr(entry);
        return valueSize;
    }

    static class BytesContext
            extends VanillaContext<Bytes, BytesBytesInterop,
            DelegatingMetaBytesInterop<Bytes, BytesBytesInterop>,
            Bytes, BytesBytesInterop,
            DelegatingMetaBytesInterop<Bytes, BytesBytesInterop>> {
        BytesContext() {
            super();
        }

        BytesContext(HashContext contextCache, int indexInContextCache) {
            super(contextCache, indexInContextCache);
        }

        @Override
        public void initKeyModel0() {
            initBytesKeyModel0();
        }

        @Override
        public void initKey0(Bytes key) {
            initBytesKey0(key);
        }

        @Override
        void initValueModel0() {
            initBytesValueModel0();
        }

        @Override
        void initNewValue0(Bytes newValue) {
            initNewBytesValue0(newValue);
        }

        @Override
        void closeValue0() {
            closeBytesValue0();
        }

        @Override
        public Bytes get() {
            return getBytes();
        }

        @Override
        public Bytes getUsing(Bytes usingValue) {
            return getBytesUsing(usingValue);
        }
    }

    enum BytesContextFactory implements ContextFactory<BytesContext> {
        INSTANCE;

        @Override
        public BytesContext createContext(HashContext root, int indexInContextCache) {
            return new BytesContext(root, indexInContextCache);
        }

        @Override
        public BytesContext createRootContext() {
            return new BytesContext();
        }

        @Override
        public Class<BytesContext> contextClass() {
            return BytesContext.class;
        }
    }
}
