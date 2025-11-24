/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash.impl.stage.query;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.impl.stage.entry.HashEntryStages;
import net.openhft.chronicle.hash.impl.stage.entry.HashLookupSearch;
import net.openhft.chronicle.hash.impl.stage.entry.SegmentStages;
import net.openhft.chronicle.map.impl.VanillaChronicleMapHolder;
import net.openhft.sg.Stage;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

import static net.openhft.chronicle.hash.impl.stage.query.KeySearch.SearchState.ABSENT;
import static net.openhft.chronicle.hash.impl.stage.query.KeySearch.SearchState.PRESENT;

/**
 * Performs key-based search within a single hash segment tier.
 *
 * <p>The stage walks the hash-lookup table using {@link HashLookupSearch}, comparing candidate
 * keys against an input {@link Data} instance held off-heap. When a matching entry is found it
 * initialises {@link HashEntryStages} with the located position and records whether the search
 * state is {@link SearchState#PRESENT} or {@link SearchState#ABSENT}.
 */
@Staged
@SuppressWarnings({"rawtypes", "unchecked"})
public abstract class KeySearch<K> {

    @StageRef
    public SegmentStages s;
    @StageRef
    public HashLookupSearch hashLookupSearch;
    @StageRef
    public HashEntryStages<K> entry;
    public Data<K> inputKey = null;
    @Stage("KeySearch")
    protected SearchState searchState = null;
    @StageRef
    VanillaChronicleMapHolder<K, ?, ?> mh;

    public abstract boolean inputKeyInit();

    public void initInputKey(Data<K> inputKey) {
        this.inputKey = inputKey;
    }

    public abstract boolean keySearchInit();

    @Stage("KeySearch")
    public void setSearchState(SearchState newSearchState) {
        this.searchState = newSearchState;
    }

    public void initKeySearch() {
        for (long pos; (pos = hashLookupSearch.nextPos()) >= 0L; ) {
            // otherwise we are inside iteration relocation.
            // During iteration, key search occurs when doReplaceValue() exhausts space in
            // the current segment, and insertion into the tiered segment requires to locate
            // an empty slot in the hashLookup.
            if (inputKeyInit()) {
                long keySizeOffset = s.entrySpaceOffset + pos * mh.m().chunkSize;
                Bytes segmentBytes = s.segmentBytesForRead();
                segmentBytes.readPosition(keySizeOffset);
                long keySize = mh.h().keySizeMarshaller.readSize(segmentBytes);
                long keyOffset = segmentBytes.readPosition();
                if (!keyEquals(keySize, keyOffset))
                    continue;
                hashLookupSearch.found();
                entry.readFoundEntry(pos, keySizeOffset, keySize, keyOffset);
                searchState = PRESENT;
                return;
            }
        }
        searchState = SearchState.ABSENT;
    }

    boolean keyEquals(long keySize, long keyOffset) {
        return inputKey.size() == keySize && inputKey.equivalent(s.segmentBS, keyOffset);
    }

    public boolean searchStatePresent() {
        return searchState == PRESENT;
    }

    public boolean searchStateAbsent() {
        return searchState == ABSENT;
    }

    public enum SearchState {
        PRESENT,
        ABSENT
    }
}
