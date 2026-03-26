/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import net.openhft.chronicle.set.Builder;
import org.junit.jupiter.api.Test;

import java.io.*;

import static org.junit.jupiter.api.Assertions.*;

@SuppressWarnings({"rawtypes", "unchecked", "serial"})
class DatasetTrackerIssue61Test {

    @Test
    void issue61Test() throws IOException {

        // replace Externalizable with DatasetTrackerIssue61Test to make this work
        ChronicleMapBuilder<String, Externalizable> builder = ChronicleMapBuilder
                .of(String.class, Externalizable.class)
                .averageKeySize(200)
                .averageValueSize(200)
                .entries(100);

        File dbFile = Builder.getPersistenceFile();

        try (ChronicleMap<String, Externalizable> datasetMap = builder.createPersistedTo(dbFile)) {
            System.out.printf("%s%n", datasetMap);

            String key = "esg_dataroot/obs4MIPs/observations/atmos/husNobs/mon/grid/NASA-JPL/AIRS/v20110608/husNobs_AIRS_L3_RetStd-v5_200209-201105.nc";
            datasetMap.put(key, new Value("value"));

            Value saved = (Value) datasetMap.get(key);
            assertEquals("value", saved.value);
        }
    }

    static class Value implements Externalizable {

        String value;

        public Value(String value) {
            this.value = value;
        }

        /**
         * According to Externalizable spec, there should be a public no-arg constructor
         */
        public Value() {
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeUTF(value);
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            value = in.readUTF();
        }
    }
}
