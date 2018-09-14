/*
 * Copyright 2012-2018 Chronicle Map Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
