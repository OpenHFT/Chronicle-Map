/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash.impl.stage.hash;

import net.openhft.chronicle.map.VanillaChronicleMap;

import java.util.List;
import java.util.function.BiFunction;

/**
 * Base abstraction for staged context chains used by Chronicle Map.
 *
 * <p>Concrete implementations provide access to the per-thread list of contexts,
 * track whether a context is currently in use, and support creation or reuse of
 * additional chained contexts for the same {@link VanillaChronicleMap}.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public abstract class ChainingInterface extends ThreadLocalState {

    public abstract List<ChainingInterface> getContextChain();

    public abstract void initUsed(boolean used, VanillaChronicleMap map);

    public abstract boolean usedInit();

    public abstract <T extends ChainingInterface> T getContext(
            Class<? extends T> contextClass, BiFunction<ChainingInterface, VanillaChronicleMap, T> createChaining,
            VanillaChronicleMap map);
}
