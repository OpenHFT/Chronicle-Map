package net.openhft.chronicle.map.example;

import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.serialization.BytesMarshallable;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by rob on 18/09/2015.
 * supports and array of up to 128 doubles
 */

public class DoubleArrayExample {

    static final long SIZE_OF_DOUBLE = 8;
    static final long SIZE_OF_INT = 4;

    public static void main(String[] args) throws IOException {

        ChronicleMap<Long, ValueWrapper> map = ChronicleMapBuilder.of(Long.class, ValueWrapper.class)
                .entries(512)
                .averageValueSize(SIZE_OF_INT + (128 * SIZE_OF_DOUBLE)).create();

        //        .createPersistedTo(File.createTempFile("map", "chron"));
        System.out.println("Created the monkey map ValueWrapper 128");

        double[] values = new double[125];

        Arrays.fill(values, 1.1);

        final ValueWrapper value = new ValueWrapper(values);

        map.put(1L, value);

        ValueWrapper valueRead = map.get(1L);

        for (double v : valueRead.getValues()) {
            System.out.println("" + v);
        }
    }

    private static class ValueWrapper implements BytesMarshallable {

        private double values[];

        public ValueWrapper(@NotNull double[] values) {
            this.values = values;
        }

        public double[] getValues() {
            return values;
        }

        @Override
        public void readMarshallable(Bytes bytes) throws IllegalStateException {

            assert bytes.readInt() <= 128;

            values = new double[bytes.readInt()];

            for (int i = 0; i < values.length; i++) {
                values[i] = bytes.readDouble();
            }
        }

        @Override
        public void writeMarshallable(Bytes bytes) {

            bytes.writeInt(values.length);

            for (double value : values) {
                bytes.writeDouble(value);
            }
        }
    }

}

