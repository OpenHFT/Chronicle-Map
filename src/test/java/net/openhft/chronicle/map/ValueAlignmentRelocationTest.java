package net.openhft.chronicle.map;

import net.openhft.chronicle.hash.serialization.SizeMarshaller;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import static java.nio.charset.StandardCharsets.ISO_8859_1;

@RunWith(Parameterized.class)
public class ValueAlignmentRelocationTest {

    private final boolean persisted;
    private final int alignment;
    private final int chunk;

    public ValueAlignmentRelocationTest(String name, boolean persisted, int alignment, int chunk) {
        this.persisted = persisted;
        this.alignment = alignment;
        this.chunk = chunk;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {"Volatile a=1, c=1", false, 1, 1},
                {"Volatile a=1, c=2", false, 1, 2},
                {"Volatile a=4, c=4", false, 4, 4},
                {"Volatile a=4, c=8", false, 4, 8},
                {"Persisted a=1, c=1", true, 1, 1},
                {"Persisted a=4, c=8", true, 4, 8}
        });
    }

    @NotNull
    private static String toString(final byte[] value) {
        return new String(value, 0, 0, value.length);
    }

    @Test
    public void testValueAlignmentRelocation() throws IOException {

        File file = Files.createTempFile("test", ".cm3").toFile();
        file.deleteOnExit();

        ChronicleMapBuilder<byte[], byte[]> builder = ChronicleMapBuilder
                .of(byte[].class, byte[].class)
                .averageKeySize(5)
                .averageValueSize(5)
                .keySizeMarshaller(SizeMarshaller.stopBit())
                .valueSizeMarshaller(SizeMarshaller.stopBit())
                .entryAndValueOffsetAlignment(alignment)
                .actualSegments(1)
                .actualChunkSize(chunk)
                .entries(10);
        ChronicleMap<byte[], byte[]> map = persisted ? builder.createPersistedTo(file) : builder.create();
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
//                System.out.println("firstKeySize=" + firstKeySize + ",second key=" + secondKeySize);
                Assert.assertEquals(Arrays.toString(map.get(firstKey)), Arrays.toString(thirdValue));
                Assert.assertTrue(Arrays.equals(map.get(firstKey), thirdValue));
            }
        }
    }

    @Test
    public void testValueAlignmentRelocationNoRandomTest() throws IOException {
        File file = Files.createTempFile("test", ".cm3").toFile();
        file.deleteOnExit();

        ChronicleMapBuilder<byte[], byte[]> builder = ChronicleMapBuilder
                .of(byte[].class, byte[].class)
                .averageKeySize(5)
                .averageValueSize(5)
                .keySizeMarshaller(SizeMarshaller.stopBit())
                .valueSizeMarshaller(SizeMarshaller.stopBit())
                .entryAndValueOffsetAlignment(alignment)
                .actualSegments(1)
                .actualChunkSize(chunk)
                .entries(10);
        ChronicleMap<byte[], byte[]> map = persisted ? builder.createPersistedTo(file) : builder.create();

        for (int k = 1; k <= 16; k++) {
            for (int i = 1; i < 10; i++) {
                for (int j = i + 1; j <= i + 10; j++) {
                    map.clear();

                    byte[] _austi = "abcdefghijklmnopqrstuvwxyz".substring(0, k).getBytes(ISO_8859_1);
                    byte[] _shorter = "1234567890".substring(0, i).getBytes(ISO_8859_1);
                    byte[] _h = "h".getBytes(ISO_8859_1);
                    byte[] _a = "a".getBytes(ISO_8859_1);
                    String expected = "abcdefghijklmnopqrstuvwxyz".substring(0, j);
                    byte[] _longer = expected.getBytes(ISO_8859_1);
                    byte[] _Hello = "Hello".getBytes(ISO_8859_1);
                    byte[] _world = "world".getBytes(ISO_8859_1);

                    map.put(_austi, _shorter);
                    map.put(_h, _a);

                    map.put(_austi, _longer);
                    String actual0 = toString(map.get(_austi));
                    Assert.assertEquals(expected, actual0);

                    map.put(_Hello, _world);
                    String actual = toString(map.get(_austi));

                    if (expected.equals(actual))
                        Assert.assertEquals(expected, actual);
                    else
                        System.out.println("k= " + k + ", i= " + i + ", j=" + j);
                }
            }
        }
    }
}
