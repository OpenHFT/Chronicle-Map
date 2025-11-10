//
// Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
//

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
