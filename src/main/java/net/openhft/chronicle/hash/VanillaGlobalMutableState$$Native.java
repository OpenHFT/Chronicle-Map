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

package net.openhft.chronicle.hash;

import net.openhft.chronicle.bytes.*;
import net.openhft.chronicle.values.Copyable;

/**
 * This class is a dumped output of {@link VanillaGlobalMutableState#main(String[])}. Chronicle
 * Values still have problems with class loading in OSGi/container/module environments. Dumping the
 * class is a practical workaround for Chronicle Map users who don't need Value-interface keys or
 * values, until Chronicle Values learn how to generate classes in compile time.
 *
 * @deprecated don't use this class directly, it might be removed in the future versions. Use
 * {@link net.openhft.chronicle.values.Values#nativeClassFor(Class)
 * Values.nativeClassFor(VanillaGlobalMutableState.class)}
 */
@Deprecated
public class VanillaGlobalMutableState$$Native implements VanillaGlobalMutableState, Copyable<VanillaGlobalMutableState>, BytesMarshallable, Byteable {
    private BytesStore bs;

    private long offset;

    @Override
    public int getAllocatedExtraTierBulks() {
        return (bs.readInt(offset + 0)) & ((1 << 24) - 1);
    }

    @Override
    public void setAllocatedExtraTierBulks(int _allocatedExtraTierBulks) {
        if (_allocatedExtraTierBulks < 0 || _allocatedExtraTierBulks > 16777215) {
            throw new IllegalArgumentException("_allocatedExtraTierBulks should be in [0, 16777215] range, " + _allocatedExtraTierBulks + " is given");
        }
        bs.writeInt(offset + 0, ((bs.readInt(offset + 0)) & 0xFF000000) | (_allocatedExtraTierBulks));
    }

    @Override
    public long getFirstFreeTierIndex() {
        return (bs.readLong(offset + 3)) & ((1L << 40) - 1);
    }

    @Override
    public void setFirstFreeTierIndex(long _firstFreeTierIndex) {
        if (_firstFreeTierIndex < 0 || _firstFreeTierIndex > 1099511627775L) {
            throw new IllegalArgumentException("_firstFreeTierIndex should be in [0, 1099511627775] range, " + _firstFreeTierIndex + " is given");
        }
        bs.writeLong(offset + 3, ((bs.readLong(offset + 3)) & 0xFFFFFF0000000000L) | (_firstFreeTierIndex));
    }

    @Override
    public long getExtraTiersInUse() {
        return (bs.readLong(offset + 8)) & ((1L << 40) - 1);
    }

    @Override
    public void setExtraTiersInUse(long _extraTiersInUse) {
        if (_extraTiersInUse < 0 || _extraTiersInUse > 1099511627775L) {
            throw new IllegalArgumentException("_extraTiersInUse should be in [0, 1099511627775] range, " + _extraTiersInUse + " is given");
        }
        bs.writeLong(offset + 8, ((bs.readLong(offset + 8)) & 0xFFFFFF0000000000L) | (_extraTiersInUse));
    }

    @Override
    public long getSegmentHeadersOffset() {
        return (bs.readInt(offset + 13)) & 0xFFFFFFFFL;
    }

    @Override
    public void setSegmentHeadersOffset(long _segmentHeadersOffset) {
        if (_segmentHeadersOffset < 0 || _segmentHeadersOffset > 4294967295L) {
            throw new IllegalArgumentException("_segmentHeadersOffset should be in [0, 4294967295] range, " + _segmentHeadersOffset + " is given");
        }
        bs.writeInt(offset + 13, (int) (_segmentHeadersOffset));
    }

    @Override
    public long getDataStoreSize() {
        return bs.readLong(offset + 17);
    }

    @Override
    public void setDataStoreSize(long _dataStoreSize) {
        if (_dataStoreSize < 0) {
            throw new IllegalArgumentException("_dataStoreSize should be in [0, 9223372036854775807] range, " + _dataStoreSize + " is given");
        }
        bs.writeLong(offset + 17, _dataStoreSize);
    }

    @Override
    public long addDataStoreSize(long addition) {
        long oldDataStoreSize = bs.readLong(offset + 17);
        long newDataStoreSize = oldDataStoreSize + addition;
        if (newDataStoreSize < 0) {
            throw new IllegalStateException("bs.readLong(offset + 17) should be in [0, 9223372036854775807] range, the value was " + oldDataStoreSize + ", + " + addition + " = " + newDataStoreSize + " out of the range");
        }
        bs.writeLong(offset + 17, newDataStoreSize);
        return newDataStoreSize;
    }

    @Override
    public void copyFrom(VanillaGlobalMutableState from) {
        setAllocatedExtraTierBulks(from.getAllocatedExtraTierBulks());
        setFirstFreeTierIndex(from.getFirstFreeTierIndex());
        setExtraTiersInUse(from.getExtraTiersInUse());
        setSegmentHeadersOffset(from.getSegmentHeadersOffset());
        setDataStoreSize(from.getDataStoreSize());
    }

    @Override
    public void writeMarshallable(BytesOut bytes) {
        bytes.writeInt((bs.readInt(offset + 0)) & ((1 << 24) - 1));
        bytes.writeLong((bs.readLong(offset + 3)) & ((1L << 40) - 1));
        bytes.writeLong((bs.readLong(offset + 8)) & ((1L << 40) - 1));
        bytes.writeLong((bs.readInt(offset + 13)) & 0xFFFFFFFFL);
        bytes.writeLong(bs.readLong(offset + 17));
    }

    @Override
    public void readMarshallable(BytesIn bytes) {
        bs.writeInt(offset + 0, ((bs.readInt(offset + 0)) & 0xFF000000) | (bytes.readInt()));
        bs.writeLong(offset + 3, ((bs.readLong(offset + 3)) & 0xFFFFFF0000000000L) | (bytes.readLong()));
        bs.writeLong(offset + 8, ((bs.readLong(offset + 8)) & 0xFFFFFF0000000000L) | (bytes.readLong()));
        bs.writeInt(offset + 13, (int) (bytes.readLong()));
        bs.writeLong(offset + 17, bytes.readLong());
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof VanillaGlobalMutableState)) return false;
        VanillaGlobalMutableState other = (VanillaGlobalMutableState) obj;
        if (getAllocatedExtraTierBulks() != other.getAllocatedExtraTierBulks()) return false;
        if (getFirstFreeTierIndex() != other.getFirstFreeTierIndex()) return false;
        if (getExtraTiersInUse() != other.getExtraTiersInUse()) return false;
        if (getSegmentHeadersOffset() != other.getSegmentHeadersOffset()) return false;
        if (getDataStoreSize() != other.getDataStoreSize()) return false;
        return true;
    }

    @Override
    public int hashCode() {
        int hashCode = 1;
        hashCode *= 1000003;
        hashCode ^= java.lang.Integer.hashCode(getAllocatedExtraTierBulks());
        hashCode *= 1000003;
        hashCode ^= java.lang.Long.hashCode(getFirstFreeTierIndex());
        hashCode *= 1000003;
        hashCode ^= java.lang.Long.hashCode(getExtraTiersInUse());
        hashCode *= 1000003;
        hashCode ^= java.lang.Long.hashCode(getSegmentHeadersOffset());
        hashCode *= 1000003;
        hashCode ^= java.lang.Long.hashCode(getDataStoreSize());
        return hashCode;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("VanillaGlobalMutableState");
        sb.append(", allocatedExtraTierBulks=").append(getAllocatedExtraTierBulks());
        sb.append(", firstFreeTierIndex=").append(getFirstFreeTierIndex());
        sb.append(", extraTiersInUse=").append(getExtraTiersInUse());
        sb.append(", segmentHeadersOffset=").append(getSegmentHeadersOffset());
        sb.append(", dataStoreSize=").append(getDataStoreSize());
        sb.setCharAt(25, '{');
        sb.append(' ').append('}');
        return sb.toString();
    }

    @Override
    public void bytesStore(BytesStore bytesStore, long offset, long length) {
        if (length != maxSize()) {
            throw new IllegalArgumentException("Constant size is 25, given length is " + length);
        }
        this.bs = bytesStore;
        this.offset = offset;
    }

    @Override
    public BytesStore bytesStore() {
        return bs;
    }

    @Override
    public long offset() {
        return offset;
    }

    @Override
    public long maxSize() {
        return 25;
    }
}
