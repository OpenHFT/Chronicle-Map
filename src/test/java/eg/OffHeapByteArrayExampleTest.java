package eg;

import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import net.openhft.lang.model.DataValueClasses;
import net.openhft.lang.model.constraints.MaxSize;
import net.openhft.lang.values.ByteValue;
import net.openhft.lang.values.LongValue;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class OffHeapByteArrayExampleTest {

    public static final char EXPECTED = 'b';

    interface ByteArray {
        void setByteValueAt(@MaxSize(7) int index, ByteValue value);

        ByteValue getByteValueAt(int index);
    }

    private static ChronicleMap<LongValue, ByteArray> chm;

    @BeforeClass
    public static void beforeClass() {
        chm = ChronicleMapBuilder
                .of(LongValue.class, ByteArray.class)
                .entries(1000)
                .create();
    }

    @AfterClass
    public static void afterClass() {
        if (chm != null)
            chm.close();
    }

    @Test
    public void test() {

        // this objects will be reused
        ByteValue byteValue = DataValueClasses.newDirectInstance(ByteValue.class);
        ByteArray value = chm.newValueInstance();
        LongValue key = chm.newKeyInstance();


        key.setValue(1);

        // this is kind of like byteValue[1] = 'b'
        byteValue.setValue((byte) EXPECTED);
        value.setByteValueAt(1, byteValue);

        chm.put(key, value);

        // clear the byteValue just to prove it works when we read it back
        byteValue.setValue((byte) '0');

        chm.getUsing(key, value);


        byte actual = value.getByteValueAt(1).getValue();
        Assert.assertEquals(EXPECTED, actual);

        Assert.assertEquals(0, value.getByteValueAt(2).getValue());
        Assert.assertEquals(0, value.getByteValueAt(3).getValue());
    }
}
