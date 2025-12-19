/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.values.Values;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

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

        try (ChronicleMap<LongValue, SimpleValueInterface> map = ChronicleMapBuilder
                .of(LongValue.class, SimpleValueInterface.class)
                .entries(50)
                .create()) {
            for (int value = 1; value < 20; value++) {
                longValue.setValue(value);
                simpleValueInterface.setId(value);
                simpleValueInterface.setTruth(false);
                simpleValueInterface.setSVIEnum(SimpleValueInterface.SVIEnum.SIX);

                map.put(longValue, simpleValueInterface);
            }

            assertEquals(19, map.size(), "map size after insert");

            for (int value = 1; value < 10; value++) {
                longValue.setValue(value);
                SimpleValueInterface simpleValueInterface1 = map.get(longValue);
                assertNotNull(simpleValueInterface1, "map.get should return value for key=" + value);
                assertEquals(value, simpleValueInterface1.getId(), "id should match key=" + value);
            }
        }
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
