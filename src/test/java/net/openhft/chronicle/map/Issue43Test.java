/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.chronicle.hash.serialization.BytesWriter;
import net.openhft.chronicle.hash.serialization.impl.EnumMarshallable;
import net.openhft.chronicle.set.Builder;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

public class Issue43Test {

    public static void main(String[] args) {
        new Issue43Test().testIssue43();
    }

    @Test
    public void testIssue43() {
        try {
            ChronicleMap<Long, ValueWrapper> map = ChronicleMapBuilder
                    .of(Long.class, ValueWrapper.class)
                    .entries(512)
                    .valueMarshaller(ArrayMarshaller.INSTANCE)
                    .constantValueSizeBySample(new ValueWrapper(new double[128]))
                    .createPersistedTo(Builder.getPersistenceFile());
            //System.out.println("Created the monkey map ValueWrapper 128");
        } catch (Throwable ex) {
            System.out.println(ex);
        }
    }

    private static class ValueWrapper {
        private final double[] values;

        public ValueWrapper(double[] values) {
            this.values = values;
        }
    }

    private static final class ArrayMarshaller
            implements BytesReader<ValueWrapper>, BytesWriter<ValueWrapper>,
            EnumMarshallable<ArrayMarshaller> {
        public static final ArrayMarshaller INSTANCE = new ArrayMarshaller();

        private ArrayMarshaller() {
        }

        @Override
        public void write(Bytes<?> bytes, @NotNull ValueWrapper vw) {
            bytes.writeInt(vw.values.length);

            for (int i = 0; i < vw.values.length; i++) {
                bytes.writeDouble(vw.values[i]);
            }
        }

        @NotNull
        @Override
        public ValueWrapper read(Bytes<?> in, ValueWrapper using) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @NotNull
        @Override
        public ArrayMarshaller readResolve() {
            return INSTANCE;
        }
    }
}
