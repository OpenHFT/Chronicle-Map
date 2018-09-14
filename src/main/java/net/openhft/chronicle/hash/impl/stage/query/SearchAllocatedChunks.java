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

import net.openhft.chronicle.hash.impl.stage.entry.AllocatedChunks;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

@Staged
public class SearchAllocatedChunks extends AllocatedChunks {

    @StageRef
    KeySearch<?> ks;

    /**
     * @return {@code true} if tier has changed
     */
    public boolean initEntryAndKey(long entrySize) {
        initAllocatedChunks(hh.h().inChunks(entrySize));
        int tierBeforeAllocation = s.tier;
        long pos = alloc.alloc(allocatedChunks, -1, 0);
        entry.writeNewEntry(pos, ks.inputKey);
        return s.tier != tierBeforeAllocation;
    }
}
