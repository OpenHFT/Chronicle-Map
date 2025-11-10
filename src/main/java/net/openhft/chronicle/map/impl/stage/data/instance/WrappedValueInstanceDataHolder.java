//
// Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
//

package net.openhft.chronicle.map.impl.stage.data.instance;

import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.serialization.DataAccess;
import net.openhft.chronicle.map.impl.VanillaChronicleMapHolder;
import net.openhft.sg.Stage;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

@Staged
public class WrappedValueInstanceDataHolder<V> {

    public Data<V> wrappedData = null;
    @StageRef
    VanillaChronicleMapHolder<?, V, ?> mh;
    private final DataAccess<V> wrappedValueDataAccess = mh.m().valueDataAccess.copy();
    private WrappedValueInstanceDataHolder<V> next;
    private V value;

    boolean nextInit() {
        return true;
    }

    void closeNext() {
        // do nothing
    }

    @Stage("Next")
    public WrappedValueInstanceDataHolder<V> getUnusedWrappedValueHolder() {
        if (!valueInit())
            return this;
        if (next == null)
            next = new WrappedValueInstanceDataHolder<>();
        return next.getUnusedWrappedValueHolder();
    }

    public boolean valueInit() {
        return value != null;
    }

    public void initValue(V value) {
        mh.m().checkValue(value);
        this.value = value;
    }

    public void closeValue() {
        value = null;
        if (next != null)
            next.closeValue();
    }

    private void initWrappedData() {
        wrappedData = wrappedValueDataAccess.getData(value);
    }

    private void closeWrappedData() {
        wrappedData = null;
        wrappedValueDataAccess.uninit();
    }
}
