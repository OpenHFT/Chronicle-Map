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

import net.openhft.chronicle.values.MaxUtf8Length;
import net.openhft.chronicle.values.Values;

public class LataTest {

    private static int max = 6000000;
    private static int run = 100;
    private static int currentRun = 0;

    public static void main(String args[]) throws Exception {
        long startTime = 0;
        long endTime = 0;

        ChronicleMapBuilder<StringValue, IData> builder = ChronicleMapBuilder
                .of(StringValue.class, IData.class)
                .entries(max + 1000000);

        ChronicleMap<StringValue, IData> map = builder.create();
        StringValue[] keys = new StringValue[300];
        for (int i = 0; i < keys.length; i++) {
            keys[i] = Values.newHeapInstance(StringValue.class);
            keys[i].setValue("" + i);
        }
        IData value = Values.newHeapInstance(IData.class);
        IData dataValue = Values.newHeapInstance(IData.class);
        for (int index = 0; index < run; index++) {
            currentRun++;
            startTime = System.nanoTime();
            for (int i = 0; i < max; i++) {
                StringValue key = keys[i % keys.length];
                if (!(map.containsKey(key))) {
                    value.setData(i);
                    map.put(key, value);
                } else {
                    value = map.acquireUsing(key, dataValue);
                    value.addAtomicData(10);
                }
            }
            endTime = System.nanoTime();
            map.clear();
            System.out.println("Run" + currentRun + "Time taken"
                    + (endTime - startTime));
        }
        map.close();
    }

    public interface IData {
        int getData();

        void setData(int data);

        int addAtomicData(int addData);
    }

    interface StringValue {
        CharSequence getValue();

        void setValue(@MaxUtf8Length(64) CharSequence value);
    }
}
