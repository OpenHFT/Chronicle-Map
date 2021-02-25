package net.openhft.chronicle.map;

import java.lang.ArrayIndexOutOfBoundsException;
import java.lang.IllegalArgumentException;
import java.lang.IllegalStateException;
import java.lang.Object;
import java.lang.Override;
import java.lang.String;
import java.lang.StringBuilder;

import net.openhft.chronicle.bytes.Byteable;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesMarshallable;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.values.Copyable;

/**
 * This class is a dumped output of {@link ReplicatedGlobalMutableStateV2#main(String[])}. Chronicle
 * Values still have problems with class loading in OSGi/container/module environments. Dumping the
 * class is a practical workaround for Chronicle Map users who don't need Value-interface keys or
 * values, until Chronicle Values learn how to generate classes in compile time.
 *
 * @deprecated don't use this class directly, it might be removed in the future versions. Use
 * {@link net.openhft.chronicle.values.Values#nativeClassFor(Class)
 * Values.nativeClassFor(ReplicatedGlobalMutableStateV2.class)}
 */
@Deprecated(/* to be removed in x.22 */)
public class ReplicatedGlobalMutableStateV2$$Native implements ReplicatedGlobalMutableStateV2, Copyable<ReplicatedGlobalMutableStateV2>, BytesMarshallable, Byteable {
    private BytesStore bs;

    private long offset;

    @Override
    public int getAllocatedExtraTierBulks() {
        return (bs.readInt(offset)) & ((1 << 24) - 1);
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
    public long getDataStoreSize() {
        return bs.readLong(offset + 13);
    }

    @Override
    public void setDataStoreSize(long _dataStoreSize) {
        if (_dataStoreSize < 0) {
            throw new IllegalArgumentException("_dataStoreSize should be in [0, 9223372036854775807] range, " + _dataStoreSize + " is given");
        }
        bs.writeLong(offset + 13, _dataStoreSize);
    }

    @Override
    public long addDataStoreSize(long addition) {
        long oldDataStoreSize = bs.readLong(offset + 13);
        long newDataStoreSize = oldDataStoreSize + addition;
        if (newDataStoreSize < 0) {
            throw new IllegalStateException("bs.readLong(offset + 13) should be in [0, 9223372036854775807] range, the value was " + oldDataStoreSize + ", + " + addition + " = " + newDataStoreSize + " out of the range");
        }
        bs.writeLong(offset + 13, newDataStoreSize);
        return newDataStoreSize;
    }

    @Override
    public int getCurrentCleanupSegmentIndex() {
        return bs.readInt(offset + 21);
    }

    @Override
    public void setCurrentCleanupSegmentIndex(int _currentCleanupSegmentIndex) {
        if (_currentCleanupSegmentIndex < 0) {
            throw new IllegalArgumentException("_currentCleanupSegmentIndex should be in [0, 2147483647] range, " + _currentCleanupSegmentIndex + " is given");
        }
        bs.writeInt(offset + 21, _currentCleanupSegmentIndex);
    }

    @Override
    public int getModificationIteratorsCount() {
        return (bs.readByte(offset + 25)) & 0xFF;
    }

    @Override
    public int addModificationIteratorsCount(int addition) {
        int oldModificationIteratorsCount = (bs.readByte(offset + 25)) & 0xFF;
        int newModificationIteratorsCount = oldModificationIteratorsCount + addition;
        if (newModificationIteratorsCount < 0 || newModificationIteratorsCount > 128) {
            throw new IllegalStateException("(bs.readByte(offset + 25)) & 0xFF should be in [0, 128] range, the value was " + oldModificationIteratorsCount + ", + " + addition + " = " + newModificationIteratorsCount + " out of the range");
        }
        bs.writeByte(offset + 25, (byte) (newModificationIteratorsCount));
        return newModificationIteratorsCount;
    }

    @Override
    public boolean getModificationIteratorInitAt(int index) {
        if (index < 0 || index >= 128) {
            throw new ArrayIndexOutOfBoundsException(index + " is out of bounds, array length 128");
        }
        int bitOffset = 208 + index;
        int byteOffset = bitOffset / 8;
        int bitShift = bitOffset & 7;
        return (bs.readByte(offset + byteOffset) & (1 << bitShift)) != 0;
    }

    @Override
    public void setModificationIteratorInitAt(int index, boolean _modificationIteratorInit) {
        if (index < 0 || index >= 128) {
            throw new ArrayIndexOutOfBoundsException(index + " is out of bounds, array length 128");
        }
        int bitOffset = 208 + index;
        int byteOffset = bitOffset / 8;
        int bitShift = bitOffset & 7;
        int b = bs.readByte(offset + byteOffset);
        if (_modificationIteratorInit) {
            b |= (1 << bitShift);
        } else {
            b &= ~(1 << bitShift);
        }
        bs.writeByte(offset + byteOffset, (byte) b);
    }

    @Override
    public long getSegmentHeadersOffset() {
        return bs.readLong(offset + 42);
    }

    @Override
    public void setSegmentHeadersOffset(long _segmentHeadersOffset) {
        if (_segmentHeadersOffset < 0) {
            throw new IllegalArgumentException("_segmentHeadersOffset should be in [0, 9223372036854775807] range, " + _segmentHeadersOffset + " is given");
        }
        bs.writeLong(offset + 42, _segmentHeadersOffset);
    }

    @Override
    public void copyFrom(ReplicatedGlobalMutableStateV2 from) {
        if (from instanceof ReplicatedGlobalMutableStateV2$$Native) {
            bs.write(offset, from.bytesStore(), from.offset(), 50);
        } else {
            setAllocatedExtraTierBulks(from.getAllocatedExtraTierBulks());
            setFirstFreeTierIndex(from.getFirstFreeTierIndex());
            setExtraTiersInUse(from.getExtraTiersInUse());
            setDataStoreSize(from.getDataStoreSize());
            setCurrentCleanupSegmentIndex(from.getCurrentCleanupSegmentIndex());
            int _modificationIteratorsCount = from.getModificationIteratorsCount();
            if (_modificationIteratorsCount < 0 || _modificationIteratorsCount > 128) {
                throw new IllegalArgumentException("_modificationIteratorsCount should be in [0, 128] range, " + _modificationIteratorsCount + " is given");
            }
            bs.writeByte(offset + 25, (byte) (_modificationIteratorsCount));
            for (int index = 0; index < 128; index++) {
                setModificationIteratorInitAt(index, from.getModificationIteratorInitAt(index));
            }
            setSegmentHeadersOffset(from.getSegmentHeadersOffset());
        }
    }

    @Override
    public void writeMarshallable(BytesOut bytes) {
        bytes.writeInt((bs.readInt(offset + 0)) & ((1 << 24) - 1));
        bytes.writeLong((bs.readLong(offset + 3)) & ((1L << 40) - 1));
        bytes.writeLong((bs.readLong(offset + 8)) & ((1L << 40) - 1));
        bytes.writeLong(bs.readLong(offset + 13));
        bytes.writeInt(bs.readInt(offset + 21));
        bytes.writeInt((bs.readByte(offset + 25)) & 0xFF);
        for (int index = 0; index < 128; index++) {
            bytes.writeBoolean(getModificationIteratorInitAt(index));
        }
        bytes.writeLong(bs.readLong(offset + 42));
    }

    @Override
    public void readMarshallable(BytesIn bytes) {
        bs.writeInt(offset + 0, ((bs.readInt(offset + 0)) & 0xFF000000) | (bytes.readInt()));
        bs.writeLong(offset + 3, ((bs.readLong(offset + 3)) & 0xFFFFFF0000000000L) | (bytes.readLong()));
        bs.writeLong(offset + 8, ((bs.readLong(offset + 8)) & 0xFFFFFF0000000000L) | (bytes.readLong()));
        bs.writeLong(offset + 13, bytes.readLong());
        bs.writeInt(offset + 21, bytes.readInt());
        bs.writeByte(offset + 25, (byte) (bytes.readInt()));
        for (int index = 0; index < 128; index++) {
            setModificationIteratorInitAt(index, bytes.readBoolean());
        }
        bs.writeLong(offset + 42, bytes.readLong());
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof ReplicatedGlobalMutableStateV2)) return false;
        ReplicatedGlobalMutableStateV2 other = (ReplicatedGlobalMutableStateV2) obj;
        if (getAllocatedExtraTierBulks() != other.getAllocatedExtraTierBulks()) return false;
        if (getFirstFreeTierIndex() != other.getFirstFreeTierIndex()) return false;
        if (getExtraTiersInUse() != other.getExtraTiersInUse()) return false;
        if (getDataStoreSize() != other.getDataStoreSize()) return false;
        if (getCurrentCleanupSegmentIndex() != other.getCurrentCleanupSegmentIndex()) return false;
        if (getModificationIteratorsCount() != other.getModificationIteratorsCount()) return false;
        for (int index = 0; index < 128; index++) {
            if (getModificationIteratorInitAt(index) != other.getModificationIteratorInitAt(index)) return false;
        }
        return getSegmentHeadersOffset() == other.getSegmentHeadersOffset();
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
        hashCode ^= java.lang.Long.hashCode(getDataStoreSize());
        hashCode *= 1000003;
        hashCode ^= java.lang.Integer.hashCode(getCurrentCleanupSegmentIndex());
        hashCode *= 1000003;
        hashCode ^= java.lang.Integer.hashCode(getModificationIteratorsCount());
        hashCode *= 1000003;
        int _modificationIteratorInitHashCode = 1;
        for (int index = 0; index < 128; index++) {
            _modificationIteratorInitHashCode *= 1000003;
            _modificationIteratorInitHashCode ^= Boolean.hashCode(getModificationIteratorInitAt(index));
        }
        hashCode ^= _modificationIteratorInitHashCode;
        hashCode *= 1000003;
        hashCode ^= java.lang.Long.hashCode(getSegmentHeadersOffset());
        return hashCode;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ReplicatedGlobalMutableStateV2");
        sb.append(", allocatedExtraTierBulks=").append(getAllocatedExtraTierBulks());
        sb.append(", firstFreeTierIndex=").append(getFirstFreeTierIndex());
        sb.append(", extraTiersInUse=").append(getExtraTiersInUse());
        sb.append(", dataStoreSize=").append(getDataStoreSize());
        sb.append(", currentCleanupSegmentIndex=").append(getCurrentCleanupSegmentIndex());
        sb.append(", modificationIteratorsCount=").append(getModificationIteratorsCount());
        sb.append(", modificationIteratorInit=[");
        for (int index = 0; index < 128; index++) {
            sb.append(getModificationIteratorInitAt(index)).append(',').append(' ');
        }
        sb.setCharAt(sb.length() - 2, ']');
        sb.setLength(sb.length() - 1);
        sb.append(", segmentHeadersOffset=").append(getSegmentHeadersOffset());
        sb.setCharAt(30, '{');
        sb.append(' ').append('}');
        return sb.toString();
    }

    @Override
    public void bytesStore(BytesStore bytesStore, long offset, long length) {
        if (length != maxSize()) {
            throw new IllegalArgumentException("Constant size is 50, given length is " + length);
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
        return 50;
    }
}