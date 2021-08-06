package net.openhft.chronicle.map;

import java.io.File;
import java.io.IOException;
import java.util.Random;

public class LargeMapMain {
    public static void main(String[] args) throws IOException {
        ChronicleMap<CharSequence, CharSequence> map = createMap(false);
        ChronicleMap<CharSequence, CharSequence> map2 = createMap(true);
        StringBuilder key = new StringBuilder();
        StringBuilder value = new StringBuilder();
        Random rand = new Random();
        for (int j = 0; j < 10; j++) {
            for (CharSequence cs : map.keySet()) {
                generate(rand, value, (int) Math.pow(9200, rand.nextFloat())); // average ~= 1000
                map.put(cs, value);
                map2.put(cs, value);
            }

            for (int i = 0; i < 1_000_000; i++) {
                generate(rand, key, rand.nextInt(128));
                generate(rand, value, (int) Math.pow(9200, rand.nextFloat())); // average ~= 1000
                map.put(key, value);
                map2.put(key, value);
            }
            System.out.println("any key map.size()= " + map.size() + ", sparse.size()= " + map2.size());
            System.in.read();
        }
    }

    private static ChronicleMap<CharSequence, CharSequence> createMap(boolean sparseFile) throws IOException {
        ChronicleMapBuilder<CharSequence, CharSequence> builder = ChronicleMapBuilder
                .of(CharSequence.class, CharSequence.class)
                .entries(4_000_000)
                .averageKeySize(64)
                .averageValueSize(1000)
                .maxBloatFactor(3)
                .sparseFile(sparseFile);
        ChronicleMap<CharSequence, CharSequence> map = builder.createPersistedTo(new File((sparseFile ? "sparse" : "default") + "-deleteme.map"));
        return map;
    }

    private static void generate(Random rand, StringBuilder sb, int length) {
        sb.setLength(0);
        while (sb.length() <= length)
            sb.append(rand.nextInt());
    }
}
