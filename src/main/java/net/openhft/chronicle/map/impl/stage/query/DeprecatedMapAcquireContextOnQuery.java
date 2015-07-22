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

package net.openhft.chronicle.map.impl.stage.query;

import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.impl.stage.hash.CheckOnEachPublicOperation;
import net.openhft.chronicle.hash.locks.InterProcessLock;
import net.openhft.chronicle.map.MapEntry;
import net.openhft.chronicle.map.MapKeyContext;
import net.openhft.chronicle.map.impl.stage.entry.MapEntryStages;
import net.openhft.chronicle.map.impl.stage.ret.UsingReturnValue;
import net.openhft.lang.io.Bytes;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;
import org.jetbrains.annotations.NotNull;

@Staged
public class DeprecatedMapAcquireContextOnQuery<K, V> implements MapKeyContext<K, V> {
    
    @StageRef CheckOnEachPublicOperation checkOnEachPublicOperation;
    @StageRef MapEntryStages<K, V> e;
    @StageRef MapQuery<K, V, ?> q;
    @StageRef UsingReturnValue<V> usingReturn;
    
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
        return Data.bytesEquivalent(e.entryValue, q.wrapValueAsData(value));
    }

    @Override
    public V get() {
        assert containsKey();
        return usingReturn.returnValue();
    }

    @Override
    public V getUsing(V usingValue) {
        return containsKey() ? q.value().getUsing(usingValue) : null;
    }

    @Override
    public boolean put(V newValue) {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        updateLock().lock();
        MapEntry<K, V> entry = q.entry();
        if (entry != null) {
            q.replaceValue(entry, q.wrapValueAsData(newValue));
        } else {
            q.insert(q.absentEntry(), q.wrapValueAsData(newValue));
        }
        return true;
    }

    @NotNull
    @Override
    public K key() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return q.queriedKey().get();
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
        return q.entry() != null;
    }

    @Override
    public boolean remove() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        updateLock().lock();
        MapEntry<K, V> entry = q.entry();
        if (entry != null) {
            q.remove(entry);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void close() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        put(usingReturn.returnValue());
        q.close();
    }

    @NotNull
    @Override
    public InterProcessLock readLock() {
        return q.readLock();
    }

    @NotNull
    @Override
    public InterProcessLock updateLock() {
        return q.updateLock();
    }

    @NotNull
    @Override
    public InterProcessLock writeLock() {
        return q.writeLock();
    }
}
