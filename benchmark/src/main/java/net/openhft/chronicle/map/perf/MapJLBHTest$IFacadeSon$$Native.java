package net.openhft.chronicle.map.perf;

import net.openhft.chronicle.bytes.*;
import net.openhft.chronicle.values.Copyable;

public class MapJLBHTest$IFacadeSon$$Native implements MapJLBHTest.IFacadeSon, Copyable<MapJLBHTest.IFacadeSon>, BytesMarshallable, Byteable {
    private final StringBuilder _value23Builder = new StringBuilder(10);

    private BytesStore<?, ?> bs;

    private long offset;

    @Override
    public String getValue23() {
        if (bs.readUtf8Limited(offset + 0, _value23Builder, 10) > 0) {
            return _value23Builder.toString();
        } else {
            return null;
        }
    }

    @Override
    public void setValue23(String _value23) {
        long __endvalue23 = bs.writeUtf8Limited(offset + 0, _value23, 10);
        bs.zeroOut(__endvalue23, offset + 11);
    }

    @Override
    public long getValue9() {
        return bs.readLong(offset + 11);
    }

    @Override
    public void setValue9(long _value9) {
        bs.writeLong(offset + 11, _value9);
    }

    @Override
    public long getValue19() {
        return bs.readLong(offset + 19);
    }

    @Override
    public void setValue19(long _value19) {
        bs.writeLong(offset + 19, _value19);
    }

    @Override
    public long getValue15() {
        return bs.readLong(offset + 27);
    }

    @Override
    public void setValue15(long _value15) {
        bs.writeLong(offset + 27, _value15);
    }

    @Override
    public long getValue14() {
        return bs.readLong(offset + 35);
    }

    @Override
    public void setValue14(long _value14) {
        bs.writeLong(offset + 35, _value14);
    }

    @Override
    public long getValue11() {
        return bs.readLong(offset + 43);
    }

    @Override
    public void setValue11(long _value11) {
        bs.writeLong(offset + 43, _value11);
    }

    @Override
    public long getValue10() {
        return bs.readLong(offset + 51);
    }

    @Override
    public void setValue10(long _value10) {
        bs.writeLong(offset + 51, _value10);
    }

    @Override
    public long getValue13() {
        return bs.readLong(offset + 59);
    }

    @Override
    public void setValue13(long _value13) {
        bs.writeLong(offset + 59, _value13);
    }

    @Override
    public long getValue12() {
        return bs.readLong(offset + 67);
    }

    @Override
    public void setValue12(long _value12) {
        bs.writeLong(offset + 67, _value12);
    }

    @Override
    public double getValue25() {
        return bs.readDouble(offset + 75);
    }

    @Override
    public void setValue25(double _value25) {
        bs.writeDouble(offset + 75, _value25);
    }

    @Override
    public double getValue28() {
        return bs.readDouble(offset + 83);
    }

    @Override
    public void setValue28(double _value28) {
        bs.writeDouble(offset + 83, _value28);
    }

    @Override
    public double getValue27() {
        return bs.readDouble(offset + 91);
    }

    @Override
    public void setValue27(double _value27) {
        bs.writeDouble(offset + 91, _value27);
    }

    @Override
    public double getValue22() {
        return bs.readDouble(offset + 99);
    }

    @Override
    public void setValue22(double _value22) {
        bs.writeDouble(offset + 99, _value22);
    }

    @Override
    public int getValue3() {
        return bs.readInt(offset + 107);
    }

    @Override
    public void setValue3(int _value3) {
        bs.writeInt(offset + 107, _value3);
    }

    @Override
    public int getValue20() {
        return bs.readInt(offset + 111);
    }

    @Override
    public void setValue20(int _value20) {
        bs.writeInt(offset + 111, _value20);
    }

    @Override
    public int getValue21() {
        return bs.readInt(offset + 115);
    }

    @Override
    public void setValue21(int _value21) {
        bs.writeInt(offset + 115, _value21);
    }

    @Override
    public int getValue24() {
        return bs.readInt(offset + 119);
    }

    @Override
    public void setValue24(int _value24) {
        bs.writeInt(offset + 119, _value24);
    }

    @Override
    public short getValue8() {
        return bs.readShort(offset + 123);
    }

    @Override
    public void setValue8(short _value8) {
        bs.writeShort(offset + 123, _value8);
    }

    @Override
    public short getValue7() {
        return bs.readShort(offset + 125);
    }

    @Override
    public void setValue7(short _value7) {
        bs.writeShort(offset + 125, _value7);
    }

    @Override
    public short getValue4() {
        return bs.readShort(offset + 127);
    }

    @Override
    public void setValue4(short _value4) {
        bs.writeShort(offset + 127, _value4);
    }

    @Override
    public short getValue18() {
        return bs.readShort(offset + 129);
    }

    @Override
    public void setValue18(short _value18) {
        bs.writeShort(offset + 129, _value18);
    }

    @Override
    public short getValue17() {
        return bs.readShort(offset + 131);
    }

    @Override
    public void setValue17(short _value17) {
        bs.writeShort(offset + 131, _value17);
    }

    @Override
    public short getValue16() {
        return bs.readShort(offset + 133);
    }

    @Override
    public void setValue16(short _value16) {
        bs.writeShort(offset + 133, _value16);
    }

    @Override
    public short getValue0() {
        return bs.readShort(offset + 135);
    }

    @Override
    public void setValue0(short _value0) {
        bs.writeShort(offset + 135, _value0);
    }

    @Override
    public byte getValue2() {
        return bs.readByte(offset + 137);
    }

    @Override
    public void setValue2(byte _value2) {
        bs.writeByte(offset + 137, _value2);
    }

    @Override
    public byte getValue1() {
        return bs.readByte(offset + 138);
    }

    @Override
    public void setValue1(byte _value1) {
        bs.writeByte(offset + 138, _value1);
    }

    @Override
    public byte getValue26() {
        return bs.readByte(offset + 139);
    }

    @Override
    public void setValue26(byte _value26) {
        bs.writeByte(offset + 139, _value26);
    }

    @Override
    public boolean getValue6() {
        return (bs.readByte(offset + 140) & (1 << 0)) != 0;
    }

    @Override
    public void setValue6(boolean _value6) {
        int b = bs.readByte(offset + 140);
        if (_value6) {
            b |= (1 << 0);
        } else {
            b &= ~(1 << 0);
        }
        bs.writeByte(offset + 140, (byte) b);
    }

    @Override
    public boolean getValue5() {
        return (bs.readByte(offset + 140) & (1 << 1)) != 0;
    }

    @Override
    public void setValue5(boolean _value5) {
        int b = bs.readByte(offset + 140);
        if (_value5) {
            b |= (1 << 1);
        } else {
            b &= ~(1 << 1);
        }
        bs.writeByte(offset + 140, (byte) b);
    }

    @Override
    public void copyFrom(MapJLBHTest.IFacadeSon from) {
        if (from instanceof MapJLBHTest$IFacadeSon$$Native) {
            bs.write(offset, ((MapJLBHTest$IFacadeSon$$Native) from).bytesStore(), ((MapJLBHTest$IFacadeSon$$Native) from).offset(), 141);
        } else {
            {
                long __endvalue23 = bs.writeUtf8Limited(offset + 0, from.getValue23(), 10);
                bs.zeroOut(__endvalue23, offset + 11);
            }
            {
                setValue9(from.getValue9());
            }
            {
                setValue19(from.getValue19());
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
                bs.writeDouble(offset + 75, from.getValue25());
            }
            {
                bs.writeDouble(offset + 83, from.getValue28());
            }
            {
                bs.writeDouble(offset + 91, from.getValue27());
            }
            {
                bs.writeDouble(offset + 99, from.getValue22());
            }
            {
                setValue3(from.getValue3());
            }
            {
                setValue20(from.getValue20());
            }
            {
                setValue21(from.getValue21());
            }
            {
                setValue24(from.getValue24());
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
                setValue16(from.getValue16());
            }
            {
                setValue0(from.getValue0());
            }
            {
                setValue2(from.getValue2());
            }
            {
                setValue1(from.getValue1());
            }
            {
                setValue26(from.getValue26());
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
            if (bs.readUtf8Limited(offset + 0, _value23Builder, 10) > 0) {
                bytes.writeUtf8(_value23Builder);
            } else {
                bytes.writeUtf8(null);
            }
        }
        {
            bytes.writeLong(bs.readLong(offset + 11));
        }
        {
            bytes.writeLong(bs.readLong(offset + 19));
        }
        {
            bytes.writeLong(bs.readLong(offset + 27));
        }
        {
            bytes.writeLong(bs.readLong(offset + 35));
        }
        {
            bytes.writeLong(bs.readLong(offset + 43));
        }
        {
            bytes.writeLong(bs.readLong(offset + 51));
        }
        {
            bytes.writeLong(bs.readLong(offset + 59));
        }
        {
            bytes.writeLong(bs.readLong(offset + 67));
        }
        {
            bytes.writeDouble(getValue25());
        }
        {
            bytes.writeDouble(getValue28());
        }
        {
            bytes.writeDouble(getValue27());
        }
        {
            bytes.writeDouble(getValue22());
        }
        {
            bytes.writeInt(bs.readInt(offset + 107));
        }
        {
            bytes.writeInt(bs.readInt(offset + 111));
        }
        {
            bytes.writeInt(bs.readInt(offset + 115));
        }
        {
            bytes.writeInt(bs.readInt(offset + 119));
        }
        {
            bytes.writeShort(bs.readShort(offset + 123));
        }
        {
            bytes.writeShort(bs.readShort(offset + 125));
        }
        {
            bytes.writeShort(bs.readShort(offset + 127));
        }
        {
            bytes.writeShort(bs.readShort(offset + 129));
        }
        {
            bytes.writeShort(bs.readShort(offset + 131));
        }
        {
            bytes.writeShort(bs.readShort(offset + 133));
        }
        {
            bytes.writeShort(bs.readShort(offset + 135));
        }
        {
            bytes.writeByte(bs.readByte(offset + 137));
        }
        {
            bytes.writeByte(bs.readByte(offset + 138));
        }
        {
            bytes.writeByte(bs.readByte(offset + 139));
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
            setValue23(bytes.readUtf8(_value23Builder) ? _value23Builder.toString() : null);
        }
        {
            bs.writeLong(offset + 11, bytes.readLong());
        }
        {
            bs.writeLong(offset + 19, bytes.readLong());
        }
        {
            bs.writeLong(offset + 27, bytes.readLong());
        }
        {
            bs.writeLong(offset + 35, bytes.readLong());
        }
        {
            bs.writeLong(offset + 43, bytes.readLong());
        }
        {
            bs.writeLong(offset + 51, bytes.readLong());
        }
        {
            bs.writeLong(offset + 59, bytes.readLong());
        }
        {
            bs.writeLong(offset + 67, bytes.readLong());
        }
        {
            bs.writeDouble(offset + 75, bytes.readDouble());
        }
        {
            bs.writeDouble(offset + 83, bytes.readDouble());
        }
        {
            bs.writeDouble(offset + 91, bytes.readDouble());
        }
        {
            bs.writeDouble(offset + 99, bytes.readDouble());
        }
        {
            bs.writeInt(offset + 107, bytes.readInt());
        }
        {
            bs.writeInt(offset + 111, bytes.readInt());
        }
        {
            bs.writeInt(offset + 115, bytes.readInt());
        }
        {
            bs.writeInt(offset + 119, bytes.readInt());
        }
        {
            bs.writeShort(offset + 123, bytes.readShort());
        }
        {
            bs.writeShort(offset + 125, bytes.readShort());
        }
        {
            bs.writeShort(offset + 127, bytes.readShort());
        }
        {
            bs.writeShort(offset + 129, bytes.readShort());
        }
        {
            bs.writeShort(offset + 131, bytes.readShort());
        }
        {
            bs.writeShort(offset + 133, bytes.readShort());
        }
        {
            bs.writeShort(offset + 135, bytes.readShort());
        }
        {
            bs.writeByte(offset + 137, bytes.readByte());
        }
        {
            bs.writeByte(offset + 138, bytes.readByte());
        }
        {
            bs.writeByte(offset + 139, bytes.readByte());
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
        if (!(obj instanceof MapJLBHTest.IFacadeSon)) return false;
        MapJLBHTest.IFacadeSon other = (MapJLBHTest.IFacadeSon) obj;
        {
            CharSequence __othervalue23 = other.getValue23();
            if (__othervalue23 != null && __othervalue23.length() > 10) return false;
            if (!bs.compareUtf8(offset + 0, __othervalue23)) return false;
        }
        {
            if (getValue9() != other.getValue9()) return false;
        }
        {
            if (getValue19() != other.getValue19()) return false;
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
            if (java.lang.Double.doubleToLongBits(getValue25()) != java.lang.Double.doubleToLongBits(other.getValue25()))
                return false;
        }
        {
            if (java.lang.Double.doubleToLongBits(getValue28()) != java.lang.Double.doubleToLongBits(other.getValue28()))
                return false;
        }
        {
            if (java.lang.Double.doubleToLongBits(getValue27()) != java.lang.Double.doubleToLongBits(other.getValue27()))
                return false;
        }
        {
            if (java.lang.Double.doubleToLongBits(getValue22()) != java.lang.Double.doubleToLongBits(other.getValue22()))
                return false;
        }
        {
            if (getValue3() != other.getValue3()) return false;
        }
        {
            if (getValue20() != other.getValue20()) return false;
        }
        {
            if (getValue21() != other.getValue21()) return false;
        }
        {
            if (getValue24() != other.getValue24()) return false;
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
            if (getValue16() != other.getValue16()) return false;
        }
        {
            if (getValue0() != other.getValue0()) return false;
        }
        {
            if (getValue2() != other.getValue2()) return false;
        }
        {
            if (getValue1() != other.getValue1()) return false;
        }
        {
            if (getValue26() != other.getValue26()) return false;
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
            CharSequence __hashCodevalue23 = null;
            if (bs.readUtf8Limited(offset + 0, _value23Builder, 10) > 0) {
                __hashCodevalue23 = _value23Builder;
            }
            hashCode ^= net.openhft.chronicle.values.CharSequences.hashCode(__hashCodevalue23);
        }
        hashCode *= 1000003;
        {
            hashCode ^= java.lang.Long.hashCode(getValue9());
        }
        hashCode *= 1000003;
        {
            hashCode ^= java.lang.Long.hashCode(getValue19());
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
            hashCode ^= java.lang.Double.hashCode(getValue25());
        }
        hashCode *= 1000003;
        {
            hashCode ^= java.lang.Double.hashCode(getValue28());
        }
        hashCode *= 1000003;
        {
            hashCode ^= java.lang.Double.hashCode(getValue27());
        }
        hashCode *= 1000003;
        {
            hashCode ^= java.lang.Double.hashCode(getValue22());
        }
        hashCode *= 1000003;
        {
            hashCode ^= java.lang.Integer.hashCode(getValue3());
        }
        hashCode *= 1000003;
        {
            hashCode ^= java.lang.Integer.hashCode(getValue20());
        }
        hashCode *= 1000003;
        {
            hashCode ^= java.lang.Integer.hashCode(getValue21());
        }
        hashCode *= 1000003;
        {
            hashCode ^= java.lang.Integer.hashCode(getValue24());
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
            hashCode ^= java.lang.Short.hashCode(getValue16());
        }
        hashCode *= 1000003;
        {
            hashCode ^= java.lang.Short.hashCode(getValue0());
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
            hashCode ^= java.lang.Byte.hashCode(getValue26());
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
            throw new IllegalArgumentException("Constant size is 141, given length is " + length);
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
        return 141;
    }
}
