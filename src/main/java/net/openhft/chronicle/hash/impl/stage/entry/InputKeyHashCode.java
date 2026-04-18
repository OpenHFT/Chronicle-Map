/*
 * Copyright 2013-2026 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash.impl.stage.entry;

import net.openhft.chronicle.algo.hashing.LongHashFunction;
import net.openhft.chronicle.hash.impl.stage.query.KeySearch;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

@Staged
@SuppressWarnings({"rawtypes", "unchecked"})
public class InputKeyHashCode implements KeyHashCode {

    @StageRef
    public KeySearch ks;

    public long keyHash = 0;

    void initKeyHash() {
        keyHash = ks.inputKey.hash(LongHashFunction.xx_r39());
    }

    @Override
    public long keyHashCode() {
        return keyHash;
    }
}
