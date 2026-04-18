/*
 * Copyright 2013-2026 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash.impl.stage.hash;

import net.openhft.chronicle.map.VanillaChronicleMap;

import java.util.List;
import java.util.function.BiFunction;

@SuppressWarnings({"rawtypes", "unchecked"})
public abstract class ChainingInterface extends ThreadLocalState {

    public abstract List<ChainingInterface> getContextChain();

    public abstract void initUsed(boolean used, VanillaChronicleMap map);

    public abstract boolean usedInit();

    public abstract <T extends ChainingInterface> T getContext(
            Class<? extends T> contextClass, BiFunction<ChainingInterface, VanillaChronicleMap, T> createChaining,
            VanillaChronicleMap map);
}
