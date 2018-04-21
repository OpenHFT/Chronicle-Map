/*
 *      Copyright (C) 2012, 2016  higherfrequencytrading.com
 *      Copyright (C) 2016 Roman Leventov
 *
 *      This program is free software: you can redistribute it and/or modify
 *      it under the terms of the GNU Lesser General Public License as published by
 *      the Free Software Foundation, either version 3 of the License.
 *
 *      This program is distributed in the hope that it will be useful,
 *      but WITHOUT ANY WARRANTY; without even the implied warranty of
 *      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *      GNU Lesser General Public License for more details.
 *
 *      You should have received a copy of the GNU Lesser General Public License
 *      along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
