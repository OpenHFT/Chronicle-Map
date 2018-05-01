/*
 *      Copyright (C) 2012, 2016  higherfrequencytrading.com
 *      Copyright (C) 2016 Roman Leventov
 *
 *      This program is free software: you can redistribute it and/or modify
 *      it under the terms of the GNU Lesser General Public License as published by
 *      the Free Software Foundation, either version 3 of the License.
 *
 *      This program is distributed in the hope that it will be useful,
 *      but WITHOUT ANY WARRANTY; without even the implied warranty of
 *      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *      GNU Lesser General Public License for more details.
 *
 *      You should have received a copy of the GNU Lesser General Public License
 *      along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
