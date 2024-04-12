package net.openhft.chronicle.map.perf;

import net.openhft.chronicle.bytes.*;
import net.openhft.chronicle.values.Copyable;

public class MapJLBHTest$IFacade0$$Native implements MapJLBHTest.IFacade0, Copyable<MapJLBHTest.IFacade0>, BytesMarshallable, Byteable {
    private final MapJLBHTest$IFacadeSon$$Native sonCachedValue = new MapJLBHTest$IFacadeSon$$Native();

    private final MapJLBHTest$IFacadeSon$$Native sonOtherCachedValue = new MapJLBHTest$IFacadeSon$$Native();

    private BytesStore<?, ?> bs;

    private long offset;

    @Override
    public MapJLBHTest.IFacadeSon getSonAt(int index) {
        MapJLBHTest$IFacadeSon$$Native sonCachedValue = this.sonCachedValue;
        long elementOffset = index * 141L;
        sonCachedValue.bytesStore(bs, offset + 0 + elementOffset, 141);
        return sonCachedValue;
    }

    @Override
    public void setSonAt(int index, MapJLBHTest.IFacadeSon _son) {
        if (_son instanceof MapJLBHTest$IFacadeSon$$Native) {
            long elementOffset = index * 141L;
            bs.write(offset + 0 + elementOffset, ((MapJLBHTest$IFacadeSon$$Native) _son).bytesStore(), ((MapJLBHTest$IFacadeSon$$Native) _son).offset(), 141);
        } else {
            MapJLBHTest$IFacadeSon$$Native sonCachedValue = this.sonCachedValue;
            long elementOffset = index * 141L;
            sonCachedValue.bytesStore(bs, offset + 0 + elementOffset, 141);
            sonCachedValue.copyFrom(_son);
        }
    }

    @Override
    public long getValue9() {
        return bs.readLong(offset + 423);
    }

    @Override
    public void setValue9(long _value9) {
        bs.writeLong(offset + 423, _value9);
    }

    @Override
    public long getValue15() {
        return bs.readLong(offset + 431);
    }

    @Override
    public void setValue15(long _value15) {
        bs.writeLong(offset + 431, _value15);
    }

    @Override
    public long getValue14() {
        return bs.readLong(offset + 439);
    }

    @Override
    public void setValue14(long _value14) {
        bs.writeLong(offset + 439, _value14);
    }

    @Override
    public long getValue11() {
        return bs.readLong(offset + 447);
    }

    @Override
    public void setValue11(long _value11) {
        bs.writeLong(offset + 447, _value11);
    }

    @Override
    public long getValue10() {
        return bs.readLong(offset + 455);
    }

    @Override
    public void setValue10(long _value10) {
        bs.writeLong(offset + 455, _value10);
    }

    @Override
    public long getValue13() {
        return bs.readLong(offset + 463);
    }

    @Override
    public void setValue13(long _value13) {
        bs.writeLong(offset + 463, _value13);
    }

    @Override
    public long getValue12() {
        return bs.readLong(offset + 471);
    }

    @Override
    public void setValue12(long _value12) {
        bs.writeLong(offset + 471, _value12);
    }

    @Override
    public int getValue3() {
        return bs.readInt(offset + 479);
    }

    @Override
    public void setValue3(int _value3) {
        bs.writeInt(offset + 479, _value3);
    }

    @Override
    public short getValue8() {
        return bs.readShort(offset + 483);
    }

    @Override
    public void setValue8(short _value8) {
        bs.writeShort(offset + 483, _value8);
    }

    @Override
    public short getValue7() {
        return bs.readShort(offset + 485);
    }

    @Override
    public void setValue7(short _value7) {
        bs.writeShort(offset + 485, _value7);
    }

    @Override
    public short getValue4() {
        return bs.readShort(offset + 487);
    }

    @Override
    public void setValue4(short _value4) {
        bs.writeShort(offset + 487, _value4);
    }

    @Override
    public short getValue18() {
        return bs.readShort(offset + 489);
    }

    @Override
    public void setValue18(short _value18) {
        bs.writeShort(offset + 489, _value18);
    }

    @Override
    public short getValue17() {
        return bs.readShort(offset + 491);
    }

    @Override
    public void setValue17(short _value17) {
        bs.writeShort(offset + 491, _value17);
    }

    @Override
    public short getValue0() {
        return bs.readShort(offset + 493);
    }

    @Override
    public void setValue0(short _value0) {
        bs.writeShort(offset + 493, _value0);
    }

    @Override
    public short getValue16() {
        return bs.readShort(offset + 495);
    }

    @Override
    public void setValue16(short _value16) {
        bs.writeShort(offset + 495, _value16);
    }

    @Override
    public byte getValue2() {
        return bs.readByte(offset + 497);
    }

    @Override
    public void setValue2(byte _value2) {
        bs.writeByte(offset + 497, _value2);
    }

    @Override
    public byte getValue1() {
        return bs.readByte(offset + 498);
    }

    @Override
    public void setValue1(byte _value1) {
        bs.writeByte(offset + 498, _value1);
    }

    @Override
    public boolean getValue6() {
        return (bs.readByte(offset + 499) & (1 << 0)) != 0;
    }

    @Override
    public void setValue6(boolean _value6) {
        int b = bs.readByte(offset + 499);
        if (_value6) {
            b |= (1 << 0);
        } else {
            b &= ~(1 << 0);
        }
        bs.writeByte(offset + 499, (byte) b);
    }

    @Override
    public boolean getValue5() {
        return (bs.readByte(offset + 499) & (1 << 1)) != 0;
    }

    @Override
    public void setValue5(boolean _value5) {
        int b = bs.readByte(offset + 499);
        if (_value5) {
            b |= (1 << 1);
        } else {
            b &= ~(1 << 1);
        }
        bs.writeByte(offset + 499, (byte) b);
    }

    @Override
    public void copyFrom(MapJLBHTest.IFacade0 from) {
        if (from instanceof MapJLBHTest$IFacade0$$Native) {
            bs.write(offset, ((MapJLBHTest$IFacade0$$Native) from).bytesStore(), ((MapJLBHTest$IFacade0$$Native) from).offset(), 500);
        } else {
            {
                for (int index = 0; index < 3; index++) {
                    MapJLBHTest$IFacadeSon$$Native sonCachedValue = this.sonCachedValue;
                    long elementOffset = index * 141L;
                    sonCachedValue.bytesStore(bs, offset + 0 + elementOffset, 141);
                    sonCachedValue.copyFrom(from.getSonAt(index));
                }
            }
            {
                setValue9(from.getValue9());
            }
            {
                setValue15(from.getValue15());
            }
            {
                setValue14(from.getValue14());
            }
            {
                setValue11(from.getValue11());
            }
            {
                setValue10(from.getValue10());
            }
            {
                setValue13(from.getValue13());
            }
            {
                setValue12(from.getValue12());
            }
            {
                setValue3(from.getValue3());
            }
            {
                setValue8(from.getValue8());
            }
            {
                setValue7(from.getValue7());
            }
            {
                setValue4(from.getValue4());
            }
            {
                setValue18(from.getValue18());
            }
            {
                setValue17(from.getValue17());
            }
            {
                setValue0(from.getValue0());
            }
            {
                setValue16(from.getValue16());
            }
            {
                setValue2(from.getValue2());
            }
            {
                setValue1(from.getValue1());
            }
            {
                setValue6(from.getValue6());
            }
            {
                setValue5(from.getValue5());
            }
        }
    }

    @Override
    public void writeMarshallable(BytesOut bytes) {
        {
            for (int index = 0; index < 3; index++) {
                MapJLBHTest$IFacadeSon$$Native sonCachedValue = this.sonCachedValue;
                long elementOffset = index * 141L;
                sonCachedValue.bytesStore(bs, offset + 0 + elementOffset, 141);
                sonCachedValue.writeMarshallable(bytes);
            }
        }
        {
            bytes.writeLong(bs.readLong(offset + 423));
        }
        {
            bytes.writeLong(bs.readLong(offset + 431));
        }
        {
            bytes.writeLong(bs.readLong(offset + 439));
        }
        {
            bytes.writeLong(bs.readLong(offset + 447));
        }
        {
            bytes.writeLong(bs.readLong(offset + 455));
        }
        {
            bytes.writeLong(bs.readLong(offset + 463));
        }
        {
            bytes.writeLong(bs.readLong(offset + 471));
        }
        {
            bytes.writeInt(bs.readInt(offset + 479));
        }
        {
            bytes.writeShort(bs.readShort(offset + 483));
        }
        {
            bytes.writeShort(bs.readShort(offset + 485));
        }
        {
            bytes.writeShort(bs.readShort(offset + 487));
        }
        {
            bytes.writeShort(bs.readShort(offset + 489));
        }
        {
            bytes.writeShort(bs.readShort(offset + 491));
        }
        {
            bytes.writeShort(bs.readShort(offset + 493));
        }
        {
            bytes.writeShort(bs.readShort(offset + 495));
        }
        {
            bytes.writeByte(bs.readByte(offset + 497));
        }
        {
            bytes.writeByte(bs.readByte(offset + 498));
        }
        {
            bytes.writeBoolean(getValue6());
        }
        {
            bytes.writeBoolean(getValue5());
        }
    }

    @Override
    public void readMarshallable(BytesIn bytes) {
        {
            for (int index = 0; index < 3; index++) {
                MapJLBHTest$IFacadeSon$$Native sonCachedValue = this.sonCachedValue;
                long elementOffset = index * 141L;
                sonCachedValue.bytesStore(bs, offset + 0 + elementOffset, 141);
                sonCachedValue.readMarshallable(bytes);
            }
        }
        {
            bs.writeLong(offset + 423, bytes.readLong());
        }
        {
            bs.writeLong(offset + 431, bytes.readLong());
        }
        {
            bs.writeLong(offset + 439, bytes.readLong());
        }
        {
            bs.writeLong(offset + 447, bytes.readLong());
        }
        {
            bs.writeLong(offset + 455, bytes.readLong());
        }
        {
            bs.writeLong(offset + 463, bytes.readLong());
        }
        {
            bs.writeLong(offset + 471, bytes.readLong());
        }
        {
            bs.writeInt(offset + 479, bytes.readInt());
        }
        {
            bs.writeShort(offset + 483, bytes.readShort());
        }
        {
            bs.writeShort(offset + 485, bytes.readShort());
        }
        {
            bs.writeShort(offset + 487, bytes.readShort());
        }
        {
            bs.writeShort(offset + 489, bytes.readShort());
        }
        {
            bs.writeShort(offset + 491, bytes.readShort());
        }
        {
            bs.writeShort(offset + 493, bytes.readShort());
        }
        {
            bs.writeShort(offset + 495, bytes.readShort());
        }
        {
            bs.writeByte(offset + 497, bytes.readByte());
        }
        {
            bs.writeByte(offset + 498, bytes.readByte());
        }
        {
            setValue6(bytes.readBoolean());
        }
        {
            setValue5(bytes.readBoolean());
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof MapJLBHTest.IFacade0)) return false;
        MapJLBHTest.IFacade0 other = (MapJLBHTest.IFacade0) obj;
        {
            for (int index = 0; index < 3; index++) {
                MapJLBHTest$IFacadeSon$$Native sonCachedValue = this.sonCachedValue;
                long elementOffset = index * 141L;
                sonCachedValue.bytesStore(bs, offset + 0 + elementOffset, 141);
                if (!sonCachedValue.equals(other.getSonAt(index))) return false;
            }
        }
        {
            if (getValue9() != other.getValue9()) return false;
        }
        {
            if (getValue15() != other.getValue15()) return false;
        }
        {
            if (getValue14() != other.getValue14()) return false;
        }
        {
            if (getValue11() != other.getValue11()) return false;
        }
        {
            if (getValue10() != other.getValue10()) return false;
        }
        {
            if (getValue13() != other.getValue13()) return false;
        }
        {
            if (getValue12() != other.getValue12()) return false;
        }
        {
            if (getValue3() != other.getValue3()) return false;
        }
        {
            if (getValue8() != other.getValue8()) return false;
        }
        {
            if (getValue7() != other.getValue7()) return false;
        }
        {
            if (getValue4() != other.getValue4()) return false;
        }
        {
            if (getValue18() != other.getValue18()) return false;
        }
        {
            if (getValue17() != other.getValue17()) return false;
        }
        {
            if (getValue0() != other.getValue0()) return false;
        }
        {
            if (getValue16() != other.getValue16()) return false;
        }
        {
            if (getValue2() != other.getValue2()) return false;
        }
        {
            if (getValue1() != other.getValue1()) return false;
        }
        {
            if (getValue6() != other.getValue6()) return false;
        }
        {
            if (getValue5() != other.getValue5()) return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int hashCode = 1;
        hashCode *= 1000003;
        {
            int _sonHashCode = 1;
            for (int index = 0; index < 3; index++) {
                _sonHashCode *= 1000003;
                MapJLBHTest$IFacadeSon$$Native sonCachedValue = this.sonCachedValue;
                long elementOffset = index * 141L;
                sonCachedValue.bytesStore(bs, offset + 0 + elementOffset, 141);
                _sonHashCode ^= sonCachedValue.hashCode();
            }
            hashCode ^= _sonHashCode;
        }
        hashCode *= 1000003;
        {
            hashCode ^= java.lang.Long.hashCode(getValue9());
        }
        hashCode *= 1000003;
        {
            hashCode ^= java.lang.Long.hashCode(getValue15());
        }
        hashCode *= 1000003;
        {
            hashCode ^= java.lang.Long.hashCode(getValue14());
        }
        hashCode *= 1000003;
        {
            hashCode ^= java.lang.Long.hashCode(getValue11());
        }
        hashCode *= 1000003;
        {
            hashCode ^= java.lang.Long.hashCode(getValue10());
        }
        hashCode *= 1000003;
        {
            hashCode ^= java.lang.Long.hashCode(getValue13());
        }
        hashCode *= 1000003;
        {
            hashCode ^= java.lang.Long.hashCode(getValue12());
        }
        hashCode *= 1000003;
        {
            hashCode ^= java.lang.Integer.hashCode(getValue3());
        }
        hashCode *= 1000003;
        {
            hashCode ^= java.lang.Short.hashCode(getValue8());
        }
        hashCode *= 1000003;
        {
            hashCode ^= java.lang.Short.hashCode(getValue7());
        }
        hashCode *= 1000003;
        {
            hashCode ^= java.lang.Short.hashCode(getValue4());
        }
        hashCode *= 1000003;
        {
            hashCode ^= java.lang.Short.hashCode(getValue18());
        }
        hashCode *= 1000003;
        {
            hashCode ^= java.lang.Short.hashCode(getValue17());
        }
        hashCode *= 1000003;
        {
            hashCode ^= java.lang.Short.hashCode(getValue0());
        }
        hashCode *= 1000003;
        {
            hashCode ^= java.lang.Short.hashCode(getValue16());
        }
        hashCode *= 1000003;
        {
            hashCode ^= java.lang.Byte.hashCode(getValue2());
        }
        hashCode *= 1000003;
        {
            hashCode ^= java.lang.Byte.hashCode(getValue1());
        }
        hashCode *= 1000003;
        {
            hashCode ^= Boolean.hashCode(getValue6());
        }
        hashCode *= 1000003;
        {
            hashCode ^= Boolean.hashCode(getValue5());
        }
        return hashCode;
    }

    @Override
    public void bytesStore(BytesStore<?, ?> bytesStore, long offset, long length) {
        if (length != maxSize()) {
            throw new IllegalArgumentException("Constant size is 500, given length is " + length);
        }
        this.bs = bytesStore;
        if (offset + length > bytesStore.capacity())
            throw new AssertionError();
        this.offset = offset;
    }

    @Override
    public BytesStore<?, ?> bytesStore() {
        return bs;
    }

    @Override
    public long offset() {
        return offset;
    }

    @Override
    public long maxSize() {
        return 500;
    }
}
