package net.openhft.chronicle.map.perf;

import net.openhft.chronicle.map.ChronicleMap;

import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

public class MapStress {
    private static final File file = new File("./StressTested.cm3");
    private static final int N_WRITE_THREADS = 100;
    private static final int N_READ_THREADS = 20;
    private ChronicleMap<String, String> map = ChronicleMap.of(String.class, String.class).averageKeySize(50).averageValueSize(500).entries(100_000).createOrRecoverPersistedTo(file);
    private ChronicleMap<String, String> readerMap = ChronicleMap.of(String.class, String.class).averageKeySize(500).averageValueSize(500).entries(100_000).createOrRecoverPersistedTo(file);

    private ExecutorService executor = Executors.newFixedThreadPool(N_WRITE_THREADS + N_READ_THREADS);
    private ThreadLocal<Random> randomTL = ThreadLocal.withInitial(Random::new);

    public MapStress() throws IOException {
    }

    public void stress() throws ExecutionException, InterruptedException {
        Future<?>[] futures = new Future[N_WRITE_THREADS + N_READ_THREADS];
        for (int i = 0; i < N_WRITE_THREADS; i++) {
            String id = i + " -> ";
            futures[i] = executor.submit(() -> {
                final long started = System.currentTimeMillis();
                long written = 0;
                do {
                    IntStream.range(0, 10).forEach(x -> {
                        final int keyIdx = randomTL.get().nextInt(N_WRITE_THREADS);
                        map.put("valuekdljashjgffkljakljsdffhh" + keyIdx, "aksjdfhljkhadfjklahfjkladshfjklahdsljkfch aljiocufoipreyuvoiurywcoriucy8oiuxrauoi;dajfhadsih" + System.nanoTime() + System.currentTimeMillis());
                    });
                    written += 10;

                    if (written % 100_000 == 0)
                        System.err.println(id + "Wrote " + written);
                } while (System.currentTimeMillis() - started < 100_000);
            });
        }

        for (int i = 0; i < N_READ_THREADS; i++) {
            String id = i + " -> ";
            futures[i + N_WRITE_THREADS] = executor.submit(() -> {
                final long started = System.currentTimeMillis();
                long got = 0;
                do {
                    final int keyIdx = randomTL.get().nextInt(N_WRITE_THREADS);
                    if (null != readerMap.get("valuekdljashjgffkljakljsdffhh" + keyIdx)) {
                        got++;
                    }
                } while (System.currentTimeMillis() - started < 100_000);
                System.err.println(id + "Read total: " + got);
            });
        }

        for (Future<?> f : futures) {
            f.get();
        }
        executor.shutdown();
    }

    public static void main(String[] args) throws Exception {
//        if (file.exists())
//            file.delete();
        new MapStress().stress();
    }
}
