package net.openhft.chronicle.map.impl.stage.iter;

import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.impl.stage.entry.SegmentStages;
import net.openhft.chronicle.hash.impl.stage.hash.CheckOnEachPublicOperation;
import net.openhft.chronicle.hash.locks.InterProcessLock;
import net.openhft.chronicle.map.MapKeyContext;
import net.openhft.chronicle.map.impl.stage.entry.MapEntryStages;
import net.openhft.chronicle.map.impl.stage.map.MapEntryOperationsDelegation;
import net.openhft.lang.io.Bytes;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;
import org.jetbrains.annotations.NotNull;

@Staged
public class DeprecatedMapKeyContextOnIteration<K, V> implements MapKeyContext<K, V> {
    
    @StageRef CheckOnEachPublicOperation checkOnEachPublicOperation;
    @StageRef SegmentStages s;
    @StageRef MapSegmentIteration<K, V> it;
    @StageRef MapEntryOperationsDelegation<K, V, ?> ops;
    @StageRef MapEntryStages<K, V> e;
    
    @Override
    public long valueOffset() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return e.valueOffset;
    }

    @Override
    public long valueSize() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return e.valueSize;
    }

    @Override
    public boolean valueEqualTo(V value) {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return Data.bytesEquivalent(e.entryValue, it.context().wrapValueAsData(value));
    }

    @Override
    public V get() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return it.value().get();
    }

    @Override
    public V getUsing(V usingValue) {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return it.value().getUsing(usingValue);
    }

    @Override
    public boolean put(V newValue) {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        ops.replaceValue(it, it.context().wrapValueAsData(newValue));
        return true;
    }

    @NotNull
    @Override
    public K key() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return it.key().get();
    }

    @NotNull
    @Override
    public Bytes entry() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return e.entryBytes;
    }

    @Override
    public long keyOffset() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return e.keyOffset;
    }

    @Override
    public long keySize() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return e.keySize;
    }

    @Override
    public boolean containsKey() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return true;
    }

    @Override
    public boolean remove() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        ops.remove(it);
        return true;
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException("close() is not supported during iteration");
    }

    @NotNull
    @Override
    public InterProcessLock readLock() {
        throw unsupportedLocks();
    }

    @NotNull
    @Override
    public InterProcessLock updateLock() {
        throw unsupportedLocks();
    }

    @NotNull
    @Override
    public InterProcessLock writeLock() {
        throw unsupportedLocks();
    }

    @NotNull
    private UnsupportedOperationException unsupportedLocks() {
        return new UnsupportedOperationException(
                "Lock operations are not supported (and not needed!) during iteration");
    }
}
