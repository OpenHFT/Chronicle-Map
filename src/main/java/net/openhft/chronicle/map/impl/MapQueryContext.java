/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map.impl;

import net.openhft.chronicle.hash.impl.stage.data.bytes.EntryKeyBytesData;
import net.openhft.chronicle.hash.impl.stage.data.bytes.InputKeyBytesData;
import net.openhft.chronicle.hash.impl.stage.entry.*;
import net.openhft.chronicle.hash.impl.stage.hash.KeyBytesInterop;
import net.openhft.chronicle.hash.impl.stage.hash.OwnerThreadHolder;
import net.openhft.chronicle.hash.impl.stage.query.*;
import net.openhft.chronicle.map.impl.stage.data.DummyValueZeroData;
import net.openhft.chronicle.map.impl.stage.data.bytes.EntryValueBytesData;
import net.openhft.chronicle.map.impl.stage.data.bytes.WrappedValueBytesData;
import net.openhft.chronicle.map.impl.stage.data.instance.WrappedValueInstanceDataHolder;
import net.openhft.chronicle.map.impl.stage.entry.MapEntryStages;
import net.openhft.chronicle.map.impl.stage.map.*;
import net.openhft.chronicle.map.impl.stage.query.AcquireHandle;
import net.openhft.chronicle.map.impl.stage.query.MapAbsent;
import net.openhft.chronicle.map.impl.stage.query.MapQuery;
import net.openhft.chronicle.map.impl.stage.query.QueryCheckOnEachPublicOperation;
import net.openhft.chronicle.map.impl.stage.ret.DefaultReturnValue;
import net.openhft.chronicle.map.impl.stage.ret.UsingReturnValue;
import net.openhft.sg.Context;
import net.openhft.sg.Staged;

/**
 * Staged context definition used to compile Chronicle-Map query pipelines.
 *
 * <p>The top-level and nested stages declared in {@link Context} describe how keys are
 * read, looked up and resolved into entries and return values. Like {@link MapIterationContext},
 * this class is a declarative template consumed by the staging framework rather than a
 * container for executable logic.
 */
@Staged
@Context(topLevel = {
        CompilationAnchor.class,
        OwnerThreadHolder.class,

        VanillaChronicleMapHolderImpl.class,

        KeyBytesInterop.class,
        QuerySegmentStages.class,
        KeySearch.class,
        InputKeyHashCode.class,
        QueryHashLookupSearch.class,
        HashLookupPos.class,

        QueryCheckOnEachPublicOperation.class,
        SearchAllocatedChunks.class,

        MapEntryStages.class,
        MapEntryOperationsDelegation.class,
        WrappedValueInstanceDataHolderAccess.class,
        WrappedValueBytesDataAccess.class,
        MapQuery.class,
        MapAbsent.class,
        DefaultValue.class,
        ValueBytesInterop.class,

        QueryAlloc.class,
}, nested = {
        ReadLock.class,
        UpdateLock.class,
        WriteLock.class,

        EntryKeyBytesData.class,
        EntryValueBytesData.class,

        InputKeyBytesData.class,

        WrappedValueInstanceDataHolder.class,
        WrappedValueBytesData.class,
        AcquireHandle.class,
        DefaultReturnValue.class,
        UsingReturnValue.class,

        HashEntryChecksumStrategy.class,

        DummyValueZeroData.class,
})
public class MapQueryContext {
}
