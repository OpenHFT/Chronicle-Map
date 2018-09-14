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

package net.openhft.chronicle.map;

import net.openhft.chronicle.hash.VanillaGlobalMutableState;
import net.openhft.chronicle.values.*;

interface ReplicatedGlobalMutableState extends VanillaGlobalMutableState {

    static void main(String[] args) {
        System.setProperty("chronicle.values.dumpCode", "true");
        Values.nativeClassFor(ReplicatedGlobalMutableState.class);
    }

    @Group(6)
    int getCurrentCleanupSegmentIndex();

    void setCurrentCleanupSegmentIndex(
            @Range(min = 0, max = Integer.MAX_VALUE) int currentCleanupSegmentIndex);

    @Group(7)
    @Align(offset = 1)
    int getModificationIteratorsCount();

    int addModificationIteratorsCount(@Range(min = 0, max = 128) int addition);

    @Group(8)
    // Align to prohibit array filling the highest bit of the previous 2-byte block, that
    // complicates generated code for all fields, defined in this interface.
    @Align(offset = 2)
    @Array(length = 128)
    boolean getModificationIteratorInitAt(int index);

    void setModificationIteratorInitAt(int index, boolean init);
}
