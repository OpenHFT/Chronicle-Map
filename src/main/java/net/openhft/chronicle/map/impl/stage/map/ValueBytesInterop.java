/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map.impl.stage.map;

import net.openhft.chronicle.hash.serialization.SizedReader;
import net.openhft.chronicle.map.impl.VanillaChronicleMapHolder;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

import static net.openhft.chronicle.hash.serialization.StatefulCopyable.copyIfNeeded;

/**
 * Helper stage that exposes a per-context {@link SizedReader} for values.
 * <p>
 * Some {@code SizedReader} implementations keep internal state and therefore
 * cannot be shared safely across threads. This class takes the map's
 * configured value reader and {@linkplain net.openhft.chronicle.hash.serialization.StatefulCopyable#copyIfNeeded(Object) copies it} when
 * necessary so that staged code can decode values from bytes without
 * additional allocation or locking.
 */
@Staged
public class ValueBytesInterop<V> {

    @StageRef
    VanillaChronicleMapHolder<?, V, ?> mh;

    public final SizedReader<V> valueReader = copyIfNeeded(mh.m().valueReader);

}
