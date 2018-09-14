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

import net.openhft.chronicle.set.Builder;
import org.junit.Assert;
import org.junit.Test;

import java.io.*;

public class DatasetTrackerIssue61Test {

    @Test
    public void issue61Test() throws IOException {

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
            Assert.assertEquals("value", saved.value);
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
