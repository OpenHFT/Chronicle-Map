/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import net.openhft.chronicle.values.MaxUtf8Length;
import net.openhft.chronicle.values.Values;

public class LataTest {

    private static final int max = 6000000;
    private static final int run = 100;
    private static int currentRun = 0;

    public static void main(String[] args) {
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
