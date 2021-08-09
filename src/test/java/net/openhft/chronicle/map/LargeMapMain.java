package net.openhft.chronicle.map;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Random;

public class LargeMapMain {
    public static void main(String[] args) throws IOException {
        ChronicleMap<CharSequence, CharSequence> map = createMap(false);
        ChronicleMap<CharSequence, CharSequence> map2 = createMap(true);
        StringBuilder key = new StringBuilder();
        StringBuilder value = new StringBuilder();
        Random rand = new Random(1);
        for (int j = 0; j < 40; j++) {
            for (int i = 0; i < 10_000; i++) {
                generate(rand, key, 1 + rand.nextInt(128));
                generate(rand, value, (int) Math.pow(40_000, rand.nextFloat())); // average ~= 4K
                map.put(key, value);
                map2.put(key, value);
            }

            int repeat = Math.max(1, 9 - j);
            for (int r = 0; r < repeat; r++) {
                for (CharSequence cs : map2.keySet()) {
                    generate(rand, value, (int) Math.pow(40_000, rand.nextFloat())); // average ~= 4K
                    map.put(cs, value);
                    map2.put(cs, value);
                }
            }

            System.out.println("any key map.size()= " + map.size() + ", sparse.size()= " + map2.size());
            ChronicleMap.SegmentStats[] ss = map.segmentStats();
            Arrays.sort(ss, Comparator.comparingLong(ChronicleMap.SegmentStats::usedBytes));
            System.out.println("segments " + ss.length + ", from " + ss[0] + " to " + ss[ss.length - 1]);

            ChronicleMap.SegmentStats[] ss2 = map2.segmentStats();
            Arrays.sort(ss2, Comparator.comparingLong(ChronicleMap.SegmentStats::usedBytes));
            System.out.println("segments " + ss2.length + ", from " + ss2[0] + " to " + ss2[ss2.length - 1]);

            ProcessBuilder pb = new ProcessBuilder("bash", "-c", "ls -l *.map ; du *.map");
            pb.redirectErrorStream(true);
            Process process = pb.start();
            try (BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                for (String line; (line = br.readLine()) != null; ) {
                    System.out.println(line);
                }
            }
            System.out.println("...");
        }
    }

    private static ChronicleMap<CharSequence, CharSequence> createMap(boolean sparseFile) throws IOException {
        File file = new File((sparseFile ? "sparse" : "default") + "-deleteme.map");
        file.delete();
        ChronicleMapBuilder<CharSequence, CharSequence> builder = ChronicleMapBuilder
                .of(CharSequence.class, CharSequence.class)
                .entries(100_000)
                .actualSegments(64)
                .averageKeySize(64)
                .averageValueSize(4096) // 4 KB
                .maxBloatFactor(4)
                .sparseFile(sparseFile);
        ChronicleMap<CharSequence, CharSequence> map = builder.createPersistedTo(file);
        return map;
    }

    private static void generate(Random rand, StringBuilder sb, int length) {
        sb.setLength(0);
        while (sb.length() <= length)
            sb.append(rand.nextInt());
    }
}
