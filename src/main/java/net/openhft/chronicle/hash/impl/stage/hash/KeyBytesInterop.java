/*
 * Copyright 2013-2026 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash.impl.stage.hash;

import net.openhft.chronicle.hash.impl.VanillaChronicleHashHolder;
import net.openhft.chronicle.hash.serialization.SizedReader;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

import static net.openhft.chronicle.hash.serialization.StatefulCopyable.copyIfNeeded;

@Staged
public class KeyBytesInterop<K> {

    @StageRef
    VanillaChronicleHashHolder<K> hh;

    public final SizedReader<K> keyReader = copyIfNeeded(hh.h().keyReader);
}
