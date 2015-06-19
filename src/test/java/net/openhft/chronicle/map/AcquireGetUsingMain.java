/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.map;

import net.openhft.lang.model.Byteable;
import net.openhft.lang.model.DataValueClasses;
import net.openhft.lang.model.constraints.MaxSize;

import java.io.File;

import static org.junit.Assert.fail;

public class AcquireGetUsingMain {
    public static void main(String[] args) throws Exception {
        File file = new File(System.getProperty("java.io.tmpdir") + "/test1");
        ChronicleMap<String, Data> theSharedMap =
                ChronicleMapBuilder.of(String.class, Data.class)
                        .createPersistedTo(file);
        Data data = DataValueClasses.newDirectReference(Data.class);
        String processType = "testkey";
        if (theSharedMap.getUsing(processType, data) == null) {
            System.out.println("Key " + processType + " does not exist, " + data);
        } else {
            System.out.println("Key " + processType + "  exists " + data);
        }

        // you can't have off heap objects, but you can an on heap object which proxy references off heap data.
        // this reference is not usable until it references something concrete.
        Data data2 = DataValueClasses.newDirectReference(Data.class);
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

        System.out.println("getting " + ((Byteable) data).bytes());
        data.setMaxNumberOfProcessesAllowed(3);
        data.setTimeAt(0, 100);
        data.setTimeAt(1, 111);
        data.setTimeAt(2, 222);

        // an on heap object exists when created as it doesn't use indirection to where the data is actually held.
        Data data4 = DataValueClasses.newInstance(Data.class);

        // put is not needed as we created something already.
        // theSharedMap.put(processType, data);
        System.out.println("1 " + data.getMaxNumberOfProcessesAllowed());
        System.out.println("2 " + data.getTimeAt(1));
    }

    public static interface Data {
        void setTimeAt(@MaxSize(8) int index, long time);

        long getTimeAt(int index);

        int getMaxNumberOfProcessesAllowed();

        void setMaxNumberOfProcessesAllowed(int num);
    }

}
