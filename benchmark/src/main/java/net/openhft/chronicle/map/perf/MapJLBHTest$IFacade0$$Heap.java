package net.openhft.chronicle.map.perf;

import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesMarshallable;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.values.Copyable;

import java.util.Objects;

public class MapJLBHTest$IFacade0$$Heap implements MapJLBHTest.IFacade0, Copyable<MapJLBHTest.IFacade0>, BytesMarshallable {
    private final MapJLBHTest$IFacadeSon$$Heap[] __fieldson = new MapJLBHTest$IFacadeSon$$Heap[3];

    private MapJLBHTest$IFacadeSon$$Heap __fieldsonValue = new MapJLBHTest$IFacadeSon$$Heap();

    private long __fieldvalue9;

    private long __fieldvalue15;

    private long __fieldvalue14;

    private long __fieldvalue11;

    private long __fieldvalue10;

    private long __fieldvalue13;

    private long __fieldvalue12;

    private int __fieldvalue3;

    private short __fieldvalue8;

    private short __fieldvalue7;

    private short __fieldvalue4;

    private short __fieldvalue18;

    private short __fieldvalue17;

    private short __fieldvalue0;

    private short __fieldvalue16;

    private byte __fieldvalue2;

    private byte __fieldvalue1;

    private boolean __fieldvalue6;

    private boolean __fieldvalue5;

    public MapJLBHTest$IFacade0$$Heap() {
        for (int index = 0; index < 3; index++) {
            __fieldson[index] = new MapJLBHTest$IFacadeSon$$Heap();
        }
    }

    @Override
    public MapJLBHTest.IFacadeSon getSonAt(int index) {
        MapJLBHTest.IFacadeSon raw__fieldsonValue = __fieldson[index];
        return raw__fieldsonValue;
    }

    @Override
    public void setSonAt(int index, MapJLBHTest.IFacadeSon _son) {
        __fieldson[index].copyFrom(_son);
    }

    @Override
    public long getValue9() {
        long raw__fieldvalue9Value = __fieldvalue9;
        return raw__fieldvalue9Value;
    }

    @Override
    public void setValue9(long _value9) {
        this.__fieldvalue9 = _value9;
    }

    @Override
    public long getValue15() {
        long raw__fieldvalue15Value = __fieldvalue15;
        return raw__fieldvalue15Value;
    }

    @Override
    public void setValue15(long _value15) {
        this.__fieldvalue15 = _value15;
    }

    @Override
    public long getValue14() {
        long raw__fieldvalue14Value = __fieldvalue14;
        return raw__fieldvalue14Value;
    }

    @Override
    public void setValue14(long _value14) {
        this.__fieldvalue14 = _value14;
    }

    @Override
    public long getValue11() {
        long raw__fieldvalue11Value = __fieldvalue11;
        return raw__fieldvalue11Value;
    }

    @Override
    public void setValue11(long _value11) {
        this.__fieldvalue11 = _value11;
    }

    @Override
    public long getValue10() {
        long raw__fieldvalue10Value = __fieldvalue10;
        return raw__fieldvalue10Value;
    }

    @Override
    public void setValue10(long _value10) {
        this.__fieldvalue10 = _value10;
    }

    @Override
    public long getValue13() {
        long raw__fieldvalue13Value = __fieldvalue13;
        return raw__fieldvalue13Value;
    }

    @Override
    public void setValue13(long _value13) {
        this.__fieldvalue13 = _value13;
    }

    @Override
    public long getValue12() {
        long raw__fieldvalue12Value = __fieldvalue12;
        return raw__fieldvalue12Value;
    }

    @Override
    public void setValue12(long _value12) {
        this.__fieldvalue12 = _value12;
    }

    @Override
    public int getValue3() {
        int raw__fieldvalue3Value = __fieldvalue3;
        return raw__fieldvalue3Value;
    }

    @Override
    public void setValue3(int _value3) {
        this.__fieldvalue3 = _value3;
    }

    @Override
    public short getValue8() {
        short raw__fieldvalue8Value = __fieldvalue8;
        return raw__fieldvalue8Value;
    }

    @Override
    public void setValue8(short _value8) {
        this.__fieldvalue8 = _value8;
    }

    @Override
    public short getValue7() {
        short raw__fieldvalue7Value = __fieldvalue7;
        return raw__fieldvalue7Value;
    }

    @Override
    public void setValue7(short _value7) {
        this.__fieldvalue7 = _value7;
    }

    @Override
    public short getValue4() {
        short raw__fieldvalue4Value = __fieldvalue4;
        return raw__fieldvalue4Value;
    }

    @Override
    public void setValue4(short _value4) {
        this.__fieldvalue4 = _value4;
    }

    @Override
    public short getValue18() {
        short raw__fieldvalue18Value = __fieldvalue18;
        return raw__fieldvalue18Value;
    }

    @Override
    public void setValue18(short _value18) {
        this.__fieldvalue18 = _value18;
    }

    @Override
    public short getValue17() {
        short raw__fieldvalue17Value = __fieldvalue17;
        return raw__fieldvalue17Value;
    }

    @Override
    public void setValue17(short _value17) {
        this.__fieldvalue17 = _value17;
    }

    @Override
    public short getValue0() {
        short raw__fieldvalue0Value = __fieldvalue0;
        return raw__fieldvalue0Value;
    }

    @Override
    public void setValue0(short _value0) {
        this.__fieldvalue0 = _value0;
    }

    @Override
    public short getValue16() {
        short raw__fieldvalue16Value = __fieldvalue16;
        return raw__fieldvalue16Value;
    }

    @Override
    public void setValue16(short _value16) {
        this.__fieldvalue16 = _value16;
    }

    @Override
    public byte getValue2() {
        byte raw__fieldvalue2Value = __fieldvalue2;
        return raw__fieldvalue2Value;
    }

    @Override
    public void setValue2(byte _value2) {
        this.__fieldvalue2 = _value2;
    }

    @Override
    public byte getValue1() {
        byte raw__fieldvalue1Value = __fieldvalue1;
        return raw__fieldvalue1Value;
    }

    @Override
    public void setValue1(byte _value1) {
        this.__fieldvalue1 = _value1;
    }

    @Override
    public boolean getValue6() {
        boolean raw__fieldvalue6Value = __fieldvalue6;
        return raw__fieldvalue6Value;
    }

    @Override
    public void setValue6(boolean _value6) {
        this.__fieldvalue6 = _value6;
    }

    @Override
    public boolean getValue5() {
        boolean raw__fieldvalue5Value = __fieldvalue5;
        return raw__fieldvalue5Value;
    }

    @Override
    public void setValue5(boolean _value5) {
        this.__fieldvalue5 = _value5;
    }

    @Override
    public void copyFrom(MapJLBHTest.IFacade0 from) {
        {
            for (int index = 0; index < 3; index++) {
                this.__fieldson[index].copyFrom(from.getSonAt(index));
            }
        }
        {
            long value9Copy = from.getValue9();
            this.__fieldvalue9 = value9Copy;
        }
        {
            long value15Copy = from.getValue15();
            this.__fieldvalue15 = value15Copy;
        }
        {
            long value14Copy = from.getValue14();
            this.__fieldvalue14 = value14Copy;
        }
        {
            long value11Copy = from.getValue11();
            this.__fieldvalue11 = value11Copy;
        }
        {
            long value10Copy = from.getValue10();
            this.__fieldvalue10 = value10Copy;
        }
        {
            long value13Copy = from.getValue13();
            this.__fieldvalue13 = value13Copy;
        }
        {
            long value12Copy = from.getValue12();
            this.__fieldvalue12 = value12Copy;
        }
        {
            int value3Copy = from.getValue3();
            this.__fieldvalue3 = value3Copy;
        }
        {
            short value8Copy = from.getValue8();
            this.__fieldvalue8 = value8Copy;
        }
        {
            short value7Copy = from.getValue7();
            this.__fieldvalue7 = value7Copy;
        }
        {
            short value4Copy = from.getValue4();
            this.__fieldvalue4 = value4Copy;
        }
        {
            short value18Copy = from.getValue18();
            this.__fieldvalue18 = value18Copy;
        }
        {
            short value17Copy = from.getValue17();
            this.__fieldvalue17 = value17Copy;
        }
        {
            short value0Copy = from.getValue0();
            this.__fieldvalue0 = value0Copy;
        }
        {
            short value16Copy = from.getValue16();
            this.__fieldvalue16 = value16Copy;
        }
        {
            byte value2Copy = from.getValue2();
            this.__fieldvalue2 = value2Copy;
        }
        {
            byte value1Copy = from.getValue1();
            this.__fieldvalue1 = value1Copy;
        }
        {
            boolean value6Copy = from.getValue6();
            this.__fieldvalue6 = value6Copy;
        }
        {
            boolean value5Copy = from.getValue5();
            this.__fieldvalue5 = value5Copy;
        }
    }

    @Override
    public void writeMarshallable(BytesOut bytes) {
        {
            for (int index = 0; index < 3; index++) {
                __fieldson[index].writeMarshallable(bytes);
            }
        }
        {
            bytes.writeLong(getValue9());
        }
        {
            bytes.writeLong(getValue15());
        }
        {
            bytes.writeLong(getValue14());
        }
        {
            bytes.writeLong(getValue11());
        }
        {
            bytes.writeLong(getValue10());
        }
        {
            bytes.writeLong(getValue13());
        }
        {
            bytes.writeLong(getValue12());
        }
        {
            bytes.writeInt(getValue3());
        }
        {
            bytes.writeShort(getValue8());
        }
        {
            bytes.writeShort(getValue7());
        }
        {
            bytes.writeShort(getValue4());
        }
        {
            bytes.writeShort(getValue18());
        }
        {
            bytes.writeShort(getValue17());
        }
        {
            bytes.writeShort(getValue0());
        }
        {
            bytes.writeShort(getValue16());
        }
        {
            bytes.writeByte(getValue2());
        }
        {
            bytes.writeByte(getValue1());
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
                __fieldson[index].readMarshallable(bytes);
            }
        }
        {
            __fieldvalue9 = bytes.readLong();
        }
        {
            __fieldvalue15 = bytes.readLong();
        }
        {
            __fieldvalue14 = bytes.readLong();
        }
        {
            __fieldvalue11 = bytes.readLong();
        }
        {
            __fieldvalue10 = bytes.readLong();
        }
        {
            __fieldvalue13 = bytes.readLong();
        }
        {
            __fieldvalue12 = bytes.readLong();
        }
        {
            __fieldvalue3 = bytes.readInt();
        }
        {
            __fieldvalue8 = bytes.readShort();
        }
        {
            __fieldvalue7 = bytes.readShort();
        }
        {
            __fieldvalue4 = bytes.readShort();
        }
        {
            __fieldvalue18 = bytes.readShort();
        }
        {
            __fieldvalue17 = bytes.readShort();
        }
        {
            __fieldvalue0 = bytes.readShort();
        }
        {
            __fieldvalue16 = bytes.readShort();
        }
        {
            __fieldvalue2 = bytes.readByte();
        }
        {
            __fieldvalue1 = bytes.readByte();
        }
        {
            __fieldvalue6 = bytes.readBoolean();
        }
        {
            __fieldvalue5 = bytes.readBoolean();
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof MapJLBHTest.IFacade0)) return false;
        MapJLBHTest.IFacade0 other = (MapJLBHTest.IFacade0) obj;
        {
            for (int index = 0; index < 3; index++) {
                if (!Objects.equals(__fieldson[index], other.getSonAt(index))) return false;
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
                _sonHashCode ^= __fieldson[index].hashCode();
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
            hashCode ^= java.lang.Boolean.hashCode(getValue6());
        }
        hashCode *= 1000003;
        {
            hashCode ^= java.lang.Boolean.hashCode(getValue5());
        }
        return hashCode;
    }
}
