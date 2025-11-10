//
// Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
//

package net.openhft.chronicle.map.impl.stage.query;

import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.impl.stage.hash.CheckOnEachPublicOperation;
import net.openhft.chronicle.hash.impl.stage.query.KeySearch;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;
import org.jetbrains.annotations.NotNull;

@Staged
public class ReplicatedMapAbsentDelegating<K, V> implements Absent<K, V> {

    @StageRef
    CheckOnEachPublicOperation checkOnEachPublicOperation;
    @StageRef
    KeySearch<K> ks;
    @StageRef
    ReplicatedMapAbsent<K, V> delegate;

    @NotNull
    @Override
    public MapQuery<K, V, ?> context() {
        return delegate.context();
    }

    @Override
    public void doInsert() {
        delegate.doInsert();
    }

    @Override
    public void doInsert(Data<V> value) {
        delegate.doInsert(value);
    }

    @NotNull
    @Override
    public Data<V> defaultValue() {
        return delegate.defaultValue();
    }

    @NotNull
    @Override
    public Data<K> absentKey() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return ks.inputKey;
    }
}
