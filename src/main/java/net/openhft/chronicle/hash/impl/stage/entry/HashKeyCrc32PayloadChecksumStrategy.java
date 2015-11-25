/*
 *      Copyright (C) 2015  higherfrequencytrading.com
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

import net.openhft.chronicle.hash.impl.stage.hash.CheckOnEachPublicOperation;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

import static net.openhft.chronicle.hash.impl.stage.entry.ChecksumHashing.hash8To16Bytes;

@Staged
public class HashKeyCrc32PayloadChecksumStrategy implements ChecksumStrategy {

    @StageRef CheckOnEachPublicOperation checkOnEachPublicOperation;
    @StageRef SegmentStages s;
    @StageRef HashEntryStages<?> e;
    @StageRef KeyHashCode h;

    @Override
    public void computeAndStoreChecksum() {
        int crc = computeChecksum();
        s.segmentBS.writeInt(e.entryEnd(), crc);
    }

    private int computeChecksum() {
        long keyHashCode = h.keyHashCode();

        long keyEnd = e.keyEnd();
        long len = e.entryEnd() - keyEnd;

        long checksum;
        if (len > 0) {
            long addr = s.tierBaseAddr + keyEnd;
            int payloadCrc = Crc32.compute(addr, len);
            checksum = hash8To16Bytes(e.keySize, keyHashCode, payloadCrc);
        } else {
            // non replicated ChronicleSet has no payload
            checksum = hash8To16Bytes(e.keySize, keyHashCode, keyHashCode);
        }
        return (int) ((checksum >>> 32) ^ checksum);
    }

    @Override
    public void updateChecksum() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        computeAndStoreChecksum();
    }

    @Override
    public boolean innerCheckSum() {
        int oldChecksum = s.segmentBS.readInt(e.entryEnd());
        int crc = computeChecksum();
        return oldChecksum == crc;
    }

    @Override
    public boolean checkSum() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return innerCheckSum();
    }

    @Override
    public long extraEntryBytes() {
        return CHECKSUM_STORED_BYTES;
    }
}
