package net.openhft.chronicle.map;

import net.openhft.chronicle.hash.serialization.SizeMarshaller;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Random;

public class ValueAlignmentRelocation {

    @Test
    public void testValueAlignmentRelocation() {
        ChronicleMap<byte[], byte[]> map = ChronicleMapBuilder
                .of(byte[].class, byte[].class)
                .averageKeySize(5)
                .averageValueSize(5)
                .keySizeMarshaller(SizeMarshaller.stopBit())
                .valueSizeMarshaller(SizeMarshaller.stopBit())
                .entryAndValueOffsetAlignment(8)
                .actualSegments(1)
                .actualChunkSize(2)
                .entries(10)
                .create();
        Random r = new Random(0);

        for (int firstKeySize = 1; firstKeySize < 10; firstKeySize++) {
            byte[] firstKey = new byte[firstKeySize];
            byte[] firstValue = new byte[8];
            r.nextBytes(firstKey);
            r.nextBytes(firstValue);
            for (int secondKeySize = 1; secondKeySize < 10; secondKeySize++) {
                byte[] secondKey = new byte[secondKeySize];
                r.nextBytes(secondKey);
                while (Arrays.equals(secondKey, firstKey)) {
                    r.nextBytes(secondKey);
                }
                byte[] secondValue = new byte[1];
                r.nextBytes(secondValue);
                map.clear();
                map.put(firstKey, firstValue);
                map.put(secondKey, secondValue);
                byte[] thirdValue = new byte[16];
                r.nextBytes(thirdValue);
                map.put(firstKey, thirdValue);
                for (int i = 0; i < 10; i++) {
                    map.put(new byte[]{(byte) i}, new byte[]{(byte) i});
                    map.put(("Hello" + i).getBytes(), "world".getBytes());
                }
                Assert.assertTrue(Arrays.equals(map.get(firstKey), thirdValue));
            }
        }
    }

}
