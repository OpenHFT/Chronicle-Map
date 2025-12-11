/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash.impl.stage.hash;

import net.openhft.chronicle.map.VanillaChronicleMap;

import java.util.List;
import java.util.function.BiFunction;

/**
 * Abstraction for chaining Chronicle Map contexts within a thread.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public abstract class ChainingInterface extends ThreadLocalState {

    /**
     * Returns the ordered list of contexts for this thread.
     */
    public abstract List<ChainingInterface> getContextChain();

    /**
     * Initializes usage state for the context.
     *
     * @param used flag indicating the context is now used
     * @param map  owning map
     */
    public abstract void initUsed(boolean used, VanillaChronicleMap map);

    /**
     * Returns whether the context has been marked as used.
     *
     * @return true if used
     */
    public abstract boolean usedInit();

    /**
     * Retrieves or creates a context of the given type.
     *
     * @param contextClass   desired context type
     * @param createChaining factory to create when absent
     * @param map            owning map
     * @param <T>            context type
     * @return existing or newly created context
     */
    public abstract <T extends ChainingInterface> T getContext(
            Class<? extends T> contextClass, BiFunction<ChainingInterface, VanillaChronicleMap, T> createChaining,
            VanillaChronicleMap map);
}
