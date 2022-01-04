package net.openhft.chronicle.map;

import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.values.Values;
import org.junit.Assert;
import org.junit.Test;
import shaded.org.apache.commons.codec.binary.Hex;

import java.io.File;

public class LostKeyTest {

    // See https://github.com/OpenHFT/Chronicle-Map/issues/233
    @Test
    public void lostkeysTest() throws Exception {

        File storagetmp = new File("./tmp.dat");
        if (storagetmp.exists()) {
            storagetmp.delete();
        }
        storagetmp.deleteOnExit();
        try {
            LongValue valueSample = Values.newHeapInstance(LongValue.class);
            ChronicleMap<byte[], LongValue> mmap = ChronicleMap
                    .of(byte[].class, LongValue.class)
                    .entries(10)
                    .actualSegments(1)
                    .constantValueSizeBySample(valueSample)
                    .constantKeySizeBySample(new byte[36])
                    .removeReturnsNull(false)
                    .createPersistedTo(storagetmp);

            valueSample.setValue(1);
            String lostKey = "12b5633bad1f9c167d523ad1aa1947b2732a865bf5414eab2f9e5ae5d5c191ba00000001";

            byte[] h1add = Hex.decodeHex("591e91f809d716912ca1d4a9295e70c3e78bab077683f79350f101da6458807300000000");
            mmap.put(h1add, valueSample);

            byte[] bLostKey = Hex.decodeHex(lostKey);
            mmap.put(bLostKey, valueSample);

            LongValue v1 = mmap.remove(h1add);
            Assert.assertEquals(1L, v1.getValue());

            byte[] h3add = Hex.decodeHex("6f80ca7441710cf0942cf99e3e8aa59d38d73b124730306afeb1aec8f08fd76b00000000");
            mmap.put(h3add, valueSample);
            //here we lost key
            LongValue lostKeyVal = mmap.get(bLostKey);
            Assert.assertEquals(1L, lostKeyVal.getValue());

            LongValue v2 = mmap.remove(bLostKey);
            Assert.assertEquals(1L, v2.getValue());
        } finally {
            if (storagetmp.exists()) {
                storagetmp.delete();
            }
        }
    }

}
