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

package net.openhft.chronicle.hash.impl.stage.entry;

import net.openhft.chronicle.algo.hashing.LongHashFunction;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

import static net.openhft.chronicle.hash.impl.stage.entry.ChecksumHashing.hash8To16Bytes;

@Staged
public class HashEntryChecksumStrategy implements ChecksumStrategy {

    @StageRef
    SegmentStages s;
    @StageRef
    HashEntryStages<?> e;
    @StageRef
    KeyHashCode h;

    @Override
    public void computeAndStoreChecksum() {
        int checksum = computeChecksum();
        s.segmentBS.writeInt(e.entryEnd(), checksum);
    }

    @Override
    public int computeChecksum() {
        long keyHashCode = h.keyHashCode();

        long keyEnd = e.keyEnd();
        long len = e.entryEnd() - keyEnd;

        long checksum;
        if (len > 0) {
            long addr = s.tierBaseAddr + keyEnd;
            long payloadChecksum = LongHashFunction.xx_r39().hashMemory(addr, len);
            checksum = hash8To16Bytes(e.keySize, keyHashCode, payloadChecksum);
        } else {
            // non replicated ChronicleSet has no payload
            checksum = keyHashCode;
        }
        return (int) ((checksum >>> 32) ^ checksum);
    }

    @Override
    public boolean innerCheckSum() {
        int oldChecksum = storedChecksum();
        int checksum = computeChecksum();
        return oldChecksum == checksum;
    }

    @Override
    public int storedChecksum() {
        return s.segmentBS.readInt(e.entryEnd());
    }

    @Override
    public long extraEntryBytes() {
        return CHECKSUM_STORED_BYTES;
    }
}
