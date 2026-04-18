/*
 * Copyright 2013-2026 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map.impl.stage.map;

import net.openhft.chronicle.hash.serialization.SizedReader;
import net.openhft.chronicle.map.impl.VanillaChronicleMapHolder;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

import static net.openhft.chronicle.hash.serialization.StatefulCopyable.copyIfNeeded;

@Staged
public class ValueBytesInterop<V> {

    @StageRef
    VanillaChronicleMapHolder<?, V, ?> mh;

    public final SizedReader<V> valueReader = copyIfNeeded(mh.m().valueReader);

}
