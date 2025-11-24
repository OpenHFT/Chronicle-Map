/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash.impl.stage.hash;

import net.openhft.chronicle.hash.impl.VanillaChronicleHashHolder;
import net.openhft.chronicle.hash.serialization.SizedReader;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

import static net.openhft.chronicle.hash.serialization.StatefulCopyable.copyIfNeeded;

/**
 * Stage that exposes a per-context {@link SizedReader} for keys.
 *
 * <p>The reader is copied from the map's configured key reader using
 * {@link net.openhft.chronicle.hash.serialization.StatefulCopyable} so that staged
 * queries can safely decode keys without sharing mutable state across contexts.
 */
@Staged
public class KeyBytesInterop<K> {

    @StageRef
    VanillaChronicleHashHolder<K> hh;

    public final SizedReader<K> keyReader = copyIfNeeded(hh.h().keyReader);
}
