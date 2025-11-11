/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map.impl.stage.map;

import net.openhft.chronicle.hash.ChronicleHash;
import net.openhft.chronicle.hash.impl.VanillaChronicleHash;
import net.openhft.chronicle.hash.impl.stage.hash.Chaining;
import net.openhft.chronicle.hash.impl.stage.hash.ChainingInterface;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.VanillaChronicleMap;
import net.openhft.chronicle.map.impl.VanillaChronicleMapHolder;
import net.openhft.chronicle.set.ChronicleSet;
import net.openhft.sg.Stage;
import net.openhft.sg.Staged;

@Staged
@SuppressWarnings({"rawtypes", "unchecked"})
public abstract class VanillaChronicleMapHolderImpl<K, V, R>
        extends Chaining
        implements VanillaChronicleMapHolder<K, V, R> {

    @Stage("Map")
    private VanillaChronicleMap<K, V, R> m = null;

    public VanillaChronicleMapHolderImpl(VanillaChronicleMap map) {
        super(map);
    }

    public VanillaChronicleMapHolderImpl(
            ChainingInterface rootContextInThisThread, VanillaChronicleMap map) {
        super(rootContextInThisThread, map);
    }

    @Override
    public void initMap(VanillaChronicleMap map) {
        // alternative to this "unsafe" casting approach is proper generalization
        // of Chaining/ChainingInterface, but this causes issues with current version
        // of stage-compiler.
        // TODO generalize Chaining with <M extends VanillaCM> when stage-compiler is improved.
        //noinspection unchecked
        m = map;
    }

    @Override
    public VanillaChronicleMap<K, V, R> m() {
        return m;
    }

    @Override
    public VanillaChronicleHash<K, ?, ?, ?> h() {
        return m;
    }

    @Override
    public ChronicleMap<K, V> map() {
        return m;
    }

    @Override
    public ChronicleSet<K> set() {
        return m.chronicleSet;
    }

    public ChronicleHash<K, ?, ?, ?> hash() {
        return set() != null ? set() : map();
    }
}
