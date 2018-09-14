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

import net.openhft.chronicle.bytes.Byteable;
import net.openhft.chronicle.values.Array;
import net.openhft.chronicle.values.Values;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.fail;

public class AcquireGetUsingMain {
    public static void main(String[] args) throws IOException {
        File file = new File(System.getProperty("java.io.tmpdir") + "/test1");
        ChronicleMap<String, Data> theSharedMap =
                ChronicleMapBuilder.of(String.class, Data.class)
                        .createPersistedTo(file);
        Data data = Values.newNativeReference(Data.class);
        String processType = "testkey";
        if (theSharedMap.getUsing(processType, data) == null) {
            System.out.println("Key " + processType + " does not exist, " + data);
        } else {
            System.out.println("Key " + processType + "  exists " + data);
        }

        // you can't have off heap objects, but you can an on heap object which proxy references off heap data.
        // this reference is not usable until it references something concrete.
        Data data2 = Values.newNativeReference(Data.class);
        String processType2 = "testkey2";
        if (theSharedMap.getUsing(processType2, data2) == null) {
            // should be unset given we don't set it.
            System.out.println("Key " + processType2 + " does not exist, " + data2);
            try {
                data2.setMaxNumberOfProcessesAllowed(4);
                fail("Expected a NPE because this data2 value hasn't set to anything because there was no key in the map.");
            } catch (NullPointerException npe) {
                // expected as there was not value to set the data2 to.
            }
        } else {
            System.out.println("Key " + processType2 + "  exists " + data2);
        }

        String processType3 = "testkey3";
        if (theSharedMap.getUsing(processType3, data) == null) {
            // should be unset as this key is not present.
            System.out.println("Key " + processType3 + " does not exist, " + data);
        } else {
            System.out.println("Key " + processType3 + "  exists " + data);
        }

        // populate with an entry, creating as required.
        Data data3 = theSharedMap.acquireUsing(processType, data);
        assert data3 == data;

        System.out.println("getting " + ((Byteable) data).bytesStore());
        data.setMaxNumberOfProcessesAllowed(3);
        data.setTimeAt(0, 100);
        data.setTimeAt(1, 111);
        data.setTimeAt(2, 222);

        // an on heap object exists when created as it doesn't use indirection to where the data is actually held.
        Data data4 = Values.newHeapInstance(Data.class);

        // put is not needed as we created something already.
        // theSharedMap.put(processType, data);
        System.out.println("1 " + data.getMaxNumberOfProcessesAllowed());
        System.out.println("2 " + data.getTimeAt(1));
    }

    public static interface Data {
        @Array(length = 8)
        void setTimeAt(int index, long time);

        long getTimeAt(int index);

        int getMaxNumberOfProcessesAllowed();

        void setMaxNumberOfProcessesAllowed(int num);
    }

}
