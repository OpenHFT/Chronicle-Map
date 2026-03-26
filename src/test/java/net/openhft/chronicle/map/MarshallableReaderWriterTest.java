/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import net.openhft.chronicle.core.pool.ClassAliasPool;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;
import net.openhft.chronicle.wire.WireOut;
import net.openhft.chronicle.wire.Wires;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.*;

class MarshallableReaderWriterTest {
    @Test
    void test() throws IOException {
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
            assertEquals(myOrder, retrieved);

            retrieved = map.get("1");
            assertEquals(myOrder, retrieved);
        }
    }

    static class MyOrder extends SelfDescribingMarshallable {
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
