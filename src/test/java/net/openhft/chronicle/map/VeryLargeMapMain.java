package net.openhft.chronicle.map;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Random;
import java.util.stream.IntStream;

public class VeryLargeMapMain {
    static int avgSize = 16000, maxSize = 190_000, entries = 50_000_000; // average ~= 16K

    public static void main(String[] args) throws IOException {
        ChronicleMap<CharSequence, CharSequence> map = createMap(true);
        while (map.size() < entries) {
            long start = System.nanoTime();
            IntStream.range(0, 1000)
                    .parallel()
                    .forEach(t -> {
                        Random rand = new Random();
                        StringBuilder key = new StringBuilder();
                        StringBuilder value = new StringBuilder();
                        for (int k = 0; k < 1000; k++) {
                            generate(rand, key, 1 + rand.nextInt(128));
                            generate(rand, value, (int) Math.pow(maxSize, rand.nextFloat()));
                            map.put(key, value);
                        }
                    });
            long time = System.nanoTime() - start;
            System.out.println("Entries per second: " + (long) (1e6 * 1e9 / time));

            System.out.println("any key sparse.size()= " + map.size());
            ChronicleMap.SegmentStats[] ss = map.segmentStats();
            Arrays.sort(ss, Comparator.comparingLong(ChronicleMap.SegmentStats::usedBytes));
            System.out.println("segments " + ss.length + ", from " + ss[0] + " to " + ss[ss.length - 1]);

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
        File file = new File("verylarge-deleteme.map");
        file.delete();
        ChronicleMapBuilder<CharSequence, CharSequence> builder = ChronicleMapBuilder
                .of(CharSequence.class, CharSequence.class)
                .entries(entries)
                .averageKeySize(64)
                .averageValueSize(avgSize)
//                .maxBloatFactor(2)
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
