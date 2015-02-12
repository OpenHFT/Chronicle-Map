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

import net.openhft.chronicle.hash.impl.ContextFactory;
import net.openhft.chronicle.hash.impl.HashContext;
import net.openhft.chronicle.hash.impl.hashlookup.EntryConsumer;
import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.chronicle.hash.serialization.internal.BytesBytesInterop;
import net.openhft.chronicle.hash.serialization.internal.DelegatingMetaBytesInterop;
import net.openhft.chronicle.hash.serialization.internal.MetaBytesInterop;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.MultiStoreBytes;

import static net.openhft.lang.io.NativeBytes.UNSAFE;

class VanillaContext<K, KI, MKI extends MetaBytesInterop<K, ? super KI>,
        V, VI, MVI extends MetaBytesInterop<V, ? super VI>>
        extends HashContext<K, KI, MKI>
        implements MapKeyContext<K, V> {

    
    /////////////////////////////////////////////////
    // Inner state & lifecycle
    
    enum VanillaChronicleMapContextFactory implements ContextFactory<VanillaContext> {
        INSTANCE;

        @Override
        public VanillaContext createContext(HashContext root, int indexInContextCache) {
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
    
    VanillaContext() {}
    
    VanillaContext(HashContext contextCache, int indexInContextCache) {
        super(contextCache, indexInContextCache);
    }

    @Override
    public void totalCheckClosed() {
        super.totalCheckClosed();
        assert !valueBytesInit() : "value bytes not closed";
        assert !valueReaderInit() : "value reader not closed";
        assert !valueInit() : "value not closed";
        assert !valueModelInit() : "value model not closed";
        assert !newValueInit() : "new value not closed";
    }


    /////////////////////////////////////////////////
    // Map
    
    public VanillaChronicleMap<K, KI, MKI, V, VI, MVI> m() {
        return (VanillaChronicleMap<K, KI, MKI, V, VI, MVI>) h;
    }
    
    @Override
    public void closeHashDependants() {
        super.closeHashDependants();
        closeValueModel();
        closeValue();
        closeValueReader();
    }
    

    /////////////////////////////////////////////////
    // Map and context locals

    static class MapAndContextLocals<K, V> extends HashAndContextLocals<K> {
        V reusableValue;
        MapAndContextLocals(ChronicleMap<K, V> map) {
            super(map);
            // TODO why not using newValueInstance()
            if (map.valueClass() == CharSequence.class)
                reusableValue = (V) new StringBuilder();
        }
    }

    @Override
    public HashAndContextLocals<K> newHashAndContextLocals() {
        return new MapAndContextLocals<>(m());
    }

    public MapAndContextLocals<K, V> mapAndContextLocals() {
        return (MapAndContextLocals<K, V>) hashAndContextLocals();
    }

    @Override
    public void closeHashContextAndLocalsDependants() {
        super.closeHashContextAndLocalsDependants();
        closeValue();
    }


    /////////////////////////////////////////////////
    // Key search
    @Override
    public void closeKeySearchDependants() {
        super.closeKeySearchDependants();
        closeValueBytes();
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
        if (!searchStatePresent())
            throw new IllegalStateException("Key should be present");
        initValueSizeOffset0();
        entry.position(valueSizeOffset);
        valueSize = m().readValueSize(entry);
        m().alignment.alignPositionAddr(entry);
        valueOffset = entry.position();
    }

    void initValueSizeOffset0() {
        valueSizeOffset = keyOffset0() + keySize0();
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
        checkHashInit();
    }

    void initValueReader0() {
        valueReader = m().valueReaderProvider.get(copies, m().originalValueReader);
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
        if (!containsKey())
            return null;
        initValueDependencies();
        initHashAndContextLocals();
        initValue0(mapAndContextLocals().reusableValue);
        mapAndContextLocals().reusableValue = value;
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
        if (!containsKey())
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
        checkHashInit();
    }

    void initValueModel0() {
        initInstanceValueModel0();
    }

    void initInstanceValueModel0() {
        valueInterop = m().valueInteropProvider.get(copies, m().originalValueInterop);
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
        m().checkValue(newValue);
        this.newValue = newValue;
        metaValueInterop = m().metaValueInteropProvider.get(
                copies, m().originalMetaValueInterop, valueInterop, newValue);
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
    @Override
    public void initEntrySizeInChunksDependencies() {
        super.initEntrySizeInChunksDependencies();
        initValueBytes();
    }

    @Override
    public void initEntrySizeInChunks0() {
        entrySizeInChunks = m().inChunks(valueOffset + valueSize);
    }


    /////////////////////////////////////////////////
    // Put
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
        if (searchState0() == SearchState.ABSENT) {
            putEntry();
        } else {
            initValueBytes();
            putValue();
        }
        return true;
    }

    void putEntry() {
        long entrySize = entrySize(keySize0(), newValueSize);
        int allocatedChunks = allocateEntryAndWriteKey(entrySize - keySize0());
        initValueSizeOffset0();
        writeValueAndPutPos(allocatedChunks);
    }

    void writeValueAndPutPos(int allocatedChunks) {
        writeNewValueSize();
        writeNewValueAndSwitch();
        commitEntryAllocation();
        freeExtraAllocatedChunks(allocatedChunks);
    }

    // TODO extract ChronicleHash abstraction reallocateEntry()
    void putValue() {
        initEntrySizeInChunks();
        int lesserChunks = -1;
        if (newValueSize != valueSize) {
            long newSizeOfEverythingBeforeValue =
                    valueSizeOffset + m().valueSizeMarshaller.sizeEncodingSize(newValueSize);
            long entryStartAddr = entry.address();
            long newValueAddr = m().alignment.alignAddr(
                    entryStartAddr + newSizeOfEverythingBeforeValue);
            long newEntrySize = newValueAddr + newValueSize - entryStartAddr;
            int newSizeInChunks = m().inChunks(newEntrySize);
            newValueDoesNotFit:
            if (newSizeInChunks > entrySizeInChunks) {
                if (newSizeInChunks > m().maxChunksPerEntry) {
                    throw new IllegalArgumentException("Value too large: " +
                            "entry takes " + newSizeInChunks + " chunks, " +
                            m().maxChunksPerEntry + " is maximum.");
                }
                if (freeList.allClear(pos + entrySizeInChunks, pos + newSizeInChunks)) {
                    long setFrom = searchStatePresent() ? pos + entrySizeInChunks : pos;
                    freeList.set(setFrom, pos + newSizeInChunks);
                    break newValueDoesNotFit;
                }
                // RELOCATION
                beforeRelocation();
                if (searchStatePresent())
                    free(pos, entrySizeInChunks);
                int allocatedChunks =
                        m().inChunks(innerEntrySize(newSizeOfEverythingBeforeValue, newValueSize));
                pos = alloc(allocatedChunks);
                reuse(pos);
                UNSAFE.copyMemory(entryStartAddr, entry.address(), valueSizeOffset);
                writeValueAndPutPos(allocatedChunks);
                return;
            } else if (newSizeInChunks < entrySizeInChunks) {
                // Freeing extra chunks
                if (searchStatePresent())
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
        if (!searchStatePresent()) {
            freeList.set(pos, pos + entrySizeInChunks);
            entries(entries() + 1L);
            commitEntryAllocation();
        }
        if (lesserChunks > 0)
            freeExtraAllocatedChunks(lesserChunks);
    }

    void beforeRelocation() {
    }

    void writeNewValueSize() {
        entry.position(valueSizeOffset);
        m().valueSizeMarshaller.writeSize(entry, newValueSize);
        valueSize = newValueSize;
        m().alignment.alignPositionAddr(entry);
        valueOffset = entry.position();
    }

    void writeNewValueAndSwitch() {
        entry.position(valueOffset);
        metaValueInterop.write(valueInterop, entry, newValue);
        value = newValue;
        closeNewValue0();
    }


    /////////////////////////////////////////////////
    // Entry size
    final long entrySize(long keySize, long valueSize) {
        long sizeOfEverythingBeforeValue = sizeOfEverythingBeforeValue(keySize, valueSize);
        return innerEntrySize(sizeOfEverythingBeforeValue, valueSize);
    }

    long innerEntrySize(long sizeOfEverythingBeforeValue, long valueSize) {
        if (m().constantlySizedEntry) {
            return m().alignment.alignAddr(sizeOfEverythingBeforeValue + valueSize);
        } else if (m().couldNotDetermineAlignmentBeforeAllocation) {
            return sizeOfEverythingBeforeValue + m().worstAlignment + valueSize;
        } else {
            return m().alignment.alignAddr(sizeOfEverythingBeforeValue) + valueSize;
        }
    }

    long sizeOfEverythingBeforeValue(long keySize, long valueSize) {
        return m().metaDataBytes +
                m().keySizeMarshaller.sizeEncodingSize(keySize) + keySize +
                m().valueSizeMarshaller.sizeEncodingSize(valueSize);
    }


    /////////////////////////////////////////////////
    // For bytes contexts


    private final MultiStoreBytes valueCopy = new MultiStoreBytes();
    TcpReplicator.TcpSocketChannelEntryWriter output;

    void initBytesValueModel0() {
        valueInterop = (VI) BytesBytesInterop.INSTANCE;
    }

    void initNewBytesValue0(Bytes entry) {
        metaValueInterop = (MVI) DelegatingMetaBytesInterop.instance();
        entry.position(entry.limit());
        entry.limit(entry.capacity());
        newValueSize = m().valueSizeMarshaller.readSize(entry);
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
        if (!containsKey()) {
            if (output != null) {
                output.ensureBufferSize(1L);
                output.in().writeBoolean(true);
            }
            return null;
        }
        if (output != null) {
            initValueBytes();
            long totalSize = 1L + m().valueSizeMarshaller.sizeEncodingSize(valueSize) + valueSize;
            output.ensureBufferSize(totalSize);
            output.in().writeBoolean(false);
            m().valueSizeMarshaller.writeSize(output.in(), valueSize);
            output.in().write(entry, valueOffset, valueSize);
        }
        return DUMMY_BYTES;
    }

    final void freeExtraAllocatedChunks(int allocatedChunks) {
        int actuallyUsedChunks;
        // fast path
        if (!m().constantlySizedEntry && m().couldNotDetermineAlignmentBeforeAllocation &&
                (actuallyUsedChunks = m().inChunks(valueOffset + valueSize)) < allocatedChunks)  {
            free(pos + actuallyUsedChunks, allocatedChunks - actuallyUsedChunks);
            entrySizeInChunks = actuallyUsedChunks;
        } else {
            entrySizeInChunks = allocatedChunks;
        }
    }


    /////////////////////////////////////////////////
    // Iteration

    @Override
    public void initKeyFromPos() {
        super.initKeyFromPos();
        initValueBytes();
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
