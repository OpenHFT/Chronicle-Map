/*
 * Copyright 2016-2020 Chronicle Software
 *
 * https://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.openhft.chronicle.map;

import net.openhft.chronicle.core.pool.ClassAliasPool;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;
import net.openhft.chronicle.wire.WireOut;
import net.openhft.chronicle.wire.Wires;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Objects;

public class MarshallableReaderWriterTest {
    @Test
    public void test() throws IOException {
        ClassAliasPool.CLASS_ALIASES.addAlias(MyOrder.class);

        try (ChronicleMap<CharSequence, MyOrder> map = ChronicleMap
                .of(CharSequence.class, MyOrder.class)
                .entries(10)
//                .valueMarshaller(new MarshallableReaderWriter<>(MyOrder.class))
                .averageKeySize(32)
                .averageValueSize(64)
                .create()) {

            final MyOrder myOrder = Marshallable.fromString("!MyOrder {\n" +
                    "  orderId: 123\n" +
                    "}\n");
            // Omit "instrument" on purpose to trigger ValueInState.addUnexpected logic

            map.put("1", myOrder);

            MyOrder retrieved = map.get("1");
            Assert.assertEquals(myOrder, retrieved);

            retrieved = map.get("1");
            Assert.assertEquals(myOrder, retrieved);
        }
    }

    public static class MyOrder extends SelfDescribingMarshallable {
        private String instrument;
        private String orderId;

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            MyOrder myOrder = (MyOrder) o;
            return Objects.equals(instrument, myOrder.instrument) &&
                    Objects.equals(orderId, myOrder.orderId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), instrument, orderId);
        }

        public void writeMarshallable(@NotNull WireOut wire) {
            Wires.writeMarshallable(this, wire, false);
        }
    }
}
