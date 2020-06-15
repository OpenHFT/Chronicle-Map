package net.openhft.chronicle.map.perf;

import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.threads.NamedThreadFactory;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

public class MapStress {
    private static final File file = new File("./StressTested.cm3");
    private static final int N_WRITE_THREADS = 100;
    private static final int N_READ_THREADS = 20;
    private ChronicleMap<String, Security> map = ChronicleMap.of(String.class, Security.class).averageKeySize(500).averageValueSize(500).entries(100_000).createOrRecoverPersistedTo(file);
    private ChronicleMap<String, Security> readerMap = ChronicleMap.of(String.class, Security.class).averageKeySize(50).averageValueSize(500).entries(100_000).createOrRecoverPersistedTo(file);

    private ExecutorService executor = Executors.newFixedThreadPool(N_WRITE_THREADS + N_READ_THREADS,
            new NamedThreadFactory("mapStress"));

    public MapStress() throws IOException {
    }

    public void stress() throws ExecutionException, InterruptedException {
        Security[] secs = new Security[5];
        secs[0] = new Security();
        secs[0].region = "EMEA";
        secs[0].countryCode = "UK";
        secs[0].currency = "GBP";
        secs[0].industryGroup = "ASDFH";
        secs[0].issuer = "ASDF";
        secs[0].otc = "kajsdf";
        secs[0].securityTicker = "ASJDF";
        secs[0].securityType = "DFGH";
        secs[0].sector = "askdf";
        secs[0].securityUltimateUnderlyingTicker = "RTYU";
        secs[0].securityUnderlyingTicker = "WERT";

        secs[1] = new Security();
        secs[1].region = "APAC";
        secs[1].countryCode = "CN";
        secs[1].currency = "CNY";
        secs[1].industryGroup = "&^*$";
        secs[1].issuer = "ASDF";
        secs[1].otc = "kajsdf";
        secs[1].securityTicker = "ASJDF";
        secs[1].securityType = "DFGH";
        secs[1].sector = "askdf";
        secs[1].securityUltimateUnderlyingTicker = "RTYU";
        secs[1].securityUnderlyingTicker = "WERT";

        secs[2] = new Security();
        secs[2].region = "EMEA";
        secs[2].countryCode = "FR";
        secs[2].currency = "EUR";
        secs[2].industryGroup = "ASDFH";
        secs[2].issuer = "ASDF";
        secs[2].otc = "kajsdf";
        secs[2].securityTicker = "ASJDF";
        secs[2].securityType = "DFGH";
        secs[2].sector = "askdf";
        secs[2].securityUltimateUnderlyingTicker = "RTYU";
        secs[2].securityUnderlyingTicker = "TYUIO";

        secs[3] = new Security();
        secs[3].region = "NAM";
        secs[3].countryCode = "US";
        secs[3].currency = "USD";
        secs[3].industryGroup = "ASDFH";
        secs[3].issuer = "ASDF";
        secs[3].otc = "kajsdf";
        secs[3].securityTicker = "ASJDF";
        secs[3].securityType = "DFGH";
        secs[3].sector = "askdf";
        secs[3].securityUltimateUnderlyingTicker = "RTYU";
        secs[3].securityUnderlyingTicker = "QEDFW";

        secs[4] = new Security();
        secs[4].region = "LATAM";
        secs[4].countryCode = "PA";
        secs[4].currency = "REL";
        secs[4].industryGroup = "ASDFH";
        secs[4].issuer = "ASDF";
        secs[4].otc = "kajsdf";
        secs[4].securityTicker = "ASJDF";
        secs[4].securityType = "DFGH";
        secs[4].sector = "askdf";
        secs[4].securityUltimateUnderlyingTicker = "RTYU";
        secs[4].securityUnderlyingTicker = "RTYU";


        Future<?>[] futures = new Future[N_WRITE_THREADS + N_READ_THREADS];
        for (int i = 0; i < N_WRITE_THREADS; i++) {
            String id = i + " -> ";
            futures[i] = executor.submit(() -> {
                final long started = System.currentTimeMillis();
                long written = 0;
                do {
                    long time = System.currentTimeMillis();
                    final Random random = ThreadLocalRandom.current();
                    IntStream.range(0, 10).forEach(x -> {
                        final int keyIdx = random.nextInt(N_WRITE_THREADS * 100);
                        Security sec = secs[x%5];
                        sec.calculationTimeMillis = time;
                        map.put("valuekdljashjgffkljakljsdffhh" + keyIdx, sec);
                    });
                    int keyIdx = random.nextInt(N_WRITE_THREADS * 100);
                    map.remove("valuekdljashjgffkljakljsdffhh" + keyIdx);
                    written += 10;

                    if (written % 1_000 == 0)
                        System.err.println(id + "Wrote " + written);
                } while (System.currentTimeMillis() - started < 100_000);
            });
        }

        for (int i = 0; i < N_READ_THREADS; i++) {
            String id = i + " -> ";
            futures[i + N_WRITE_THREADS] = executor.submit(() -> {
                final long started = System.currentTimeMillis();
                AtomicInteger got = new AtomicInteger(0);
                do {
                    if (map.size() % 1000 == 0) {
                        System.err.print(".");
                    }
                    readerMap.values().forEach(s -> got.incrementAndGet());
                } while (System.currentTimeMillis() - started < 100_000);
                System.err.println(id + "Read total: " + got.get());
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

    public static class Security implements Serializable {

        String securityTicker;
        String securityType;
        String currency;
        String securityUnderlyingTicker;
        String sector;
        String industryGroup;
        String countryCode;
        String issuer;
        String securityUltimateUnderlyingTicker;
        String otc;
        String region;
        long calculationTimeMillis;

    }
}
