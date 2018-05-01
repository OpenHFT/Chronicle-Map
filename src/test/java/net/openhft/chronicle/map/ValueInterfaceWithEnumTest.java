package net.openhft.chronicle.map;

import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.values.Values;
import org.junit.Test;

import java.util.stream.IntStream;

/**
 * @author ges
 * @since 3/2/16.
 */
public class ValueInterfaceWithEnumTest {

    /**
     * This test will throw an {@link ArrayIndexOutOfBoundsException}. This seems to occur only with Enums having even number of
     * values
     */
    @Test
    public void testValueInterface() {
        LongValue longValue = Values.newHeapInstance(LongValue.class);
        SimpleValueInterface simpleValueInterface = Values.newHeapInstance(SimpleValueInterface.class);

        ChronicleMap<LongValue, SimpleValueInterface> map = ChronicleMapBuilder.of(LongValue.class, SimpleValueInterface.class).entries(50).create();

        IntStream.range(1, 20).forEach(value -> {
            longValue.setValue(value);
            simpleValueInterface.setId(value);
            simpleValueInterface.setTruth(false);
            simpleValueInterface.setSVIEnum(SimpleValueInterface.SVIEnum.SIX);

            map.put(longValue, simpleValueInterface);
        });

        IntStream.range(1, 10).forEach(value -> {
            longValue.setValue(value);
            SimpleValueInterface simpleValueInterface1 = map.get(longValue);
            System.out.println(simpleValueInterface1.getId());
        });
    }

    public interface SimpleValueInterface {
        int getId();

        void setId(int id);

        boolean getTruth();

        void setTruth(boolean truth);

        SVIEnum getSVIEnum();

        void setSVIEnum(SVIEnum val);

        enum SVIEnum {
            ONE, TWO, THREE, FOUR, FIVE, SIX
        }
    }
}
