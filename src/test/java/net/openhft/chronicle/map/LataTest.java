package net.openhft.chronicle.map;


import net.openhft.lang.values.StringValue;

public class LataTest {

    public interface IData {
        int getData();
        void setData(int data);
        int addAtomicData(int addData);
    }

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
            keys[i] = map.newKeyInstance();
            keys[i].setValue("" + i);
        }
        IData value = map.newValueInstance();
        IData dataValue = map.newValueInstance();
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
}
