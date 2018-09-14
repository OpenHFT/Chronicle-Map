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

package net.openhft.chronicle.hash.impl.stage.iter;

import net.openhft.chronicle.algo.hashing.LongHashFunction;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.hash.impl.VanillaChronicleHashHolder;
import net.openhft.chronicle.hash.impl.stage.entry.HashEntryStages;
import net.openhft.chronicle.hash.impl.stage.entry.KeyHashCode;
import net.openhft.chronicle.hash.impl.stage.entry.SegmentStages;
import net.openhft.chronicle.hash.serialization.impl.IntegerDataAccess;
import net.openhft.chronicle.hash.serialization.impl.WrongXxHash;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

@Staged
public class IterationKeyHashCode implements KeyHashCode {

    @StageRef
    VanillaChronicleHashHolder<?> hh;
    @StageRef
    SegmentStages s;
    @StageRef
    HashEntryStages<?> e;

    long keyHash = 0;

    void initKeyHash() {
        long addr = s.tierBaseAddr + e.keyOffset;
        long len = e.keySize;
        if (len == 4 && hh.h().keyDataAccess instanceof IntegerDataAccess) {
            keyHash = WrongXxHash.hashInt(OS.memory().readInt(addr));
        } else {
            keyHash = LongHashFunction.xx_r39().hashMemory(addr, len);
        }
    }

    @Override
    public long keyHashCode() {
        return keyHash;
    }
}
