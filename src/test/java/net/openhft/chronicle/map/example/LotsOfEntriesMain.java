package net.openhft.chronicle.map.example;

import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import net.openhft.lang.model.constraints.MaxSize;

import java.io.File;
import java.io.IOException;
import java.util.Random;

/**
 * Created by peter on 19/12/14.
 */
public class LotsOfEntriesMain {
    public static void main(String[] args) throws IOException {
        workEntries(true);
        workEntries(false);
    }

    private static void workEntries(boolean add) throws IOException {
        long entries = 100_000_000;
        File file = new File("/tmp/lotsOfEntries.dat");
        ChronicleMap<CharSequence, MyFloats> map = ChronicleMapBuilder
                .of(CharSequence.class, MyFloats.class)
                .entries(entries)
                .entrySize(80)
                .createPersistedTo(file);
        Random rand = new Random();
        StringBuilder sb = new StringBuilder();
        MyFloats mf = map.newValueInstance();
        if (add)
            for (int i = 0; i < 6; i++)
                mf.setValueAt(i, i);
        long start = System.nanoTime();
        for (long i = 0; i < entries; i++) {
            sb.setLength(0);
            int length = (int) (24 / (rand.nextFloat() + 24.0 / 1000));
            if (length > 2000)
                throw new AssertionError();
            sb.append(i);
            while (sb.length() < length)
                sb.append("-key");
            if (add)
                map.put(sb, mf);
            else
                map.getUsing(sb, mf);
            if (i << -7 == 0 && i % 2000000 == 0)
                System.out.println(i);
        }
        long time = System.nanoTime() - start;
        System.out.printf("Map.size: %,d took an average of %,d ns to %s.%n",
                map.size(), time / entries, add ? "add" : "get");
        map.close();
    }
}

interface MyFloats {
    public void setValueAt(@MaxSize(6) int index, float f);

    public float getValueAt(int index);
}
