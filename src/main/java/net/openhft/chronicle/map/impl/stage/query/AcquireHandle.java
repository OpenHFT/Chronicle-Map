/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map.impl.stage.query;

import net.openhft.chronicle.hash.impl.stage.hash.CheckOnEachPublicOperation;
import net.openhft.chronicle.map.MapClosable;
import net.openhft.chronicle.map.impl.stage.ret.UsingReturnValue;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

/**
 * Stage that writes back the acquired value when the query context is closed.
 *
 * @param <K> key type
 * @param <V> value type
 */
@Staged
public class AcquireHandle<K, V> implements MapClosable {

    /**
     * Default constructor used by staged code generation.
     */
    public AcquireHandle() {
    }

    @StageRef
    CheckOnEachPublicOperation checkOnEachPublicOperation;
    @StageRef
    MapQuery<K, V, ?> q;
    @StageRef
    UsingReturnValue<V> usingReturn;

    @Override
    public void close() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        q.replaceValue(q.entry(), q.wrapValueAsData(usingReturn.returnValue()));
        q.close();
    }
}
