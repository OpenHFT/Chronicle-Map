/*
 * Copyright 2012-2018 Chronicle Map Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

@Staged
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
    VanillaChronicleMapHolder<?, ?, ?> mh;

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
