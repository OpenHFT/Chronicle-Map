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
