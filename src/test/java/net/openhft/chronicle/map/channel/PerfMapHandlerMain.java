package net.openhft.chronicle.map.channel;

import net.openhft.affinity.AffinityLock;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.util.Mocker;
import net.openhft.chronicle.hash.serialization.impl.BytesMarshallableReaderWriter;
import net.openhft.chronicle.hash.serialization.impl.BytesSizedMarshaller;
import net.openhft.chronicle.jlbh.JLBH;
import net.openhft.chronicle.jlbh.JLBHOptions;
import net.openhft.chronicle.jlbh.JLBHTask;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.threads.PauserMode;
import net.openhft.chronicle.wire.channel.ChronicleChannel;
import net.openhft.chronicle.wire.channel.ChronicleContext;
import net.openhft.chronicle.wire.channel.ChronicleGatewayMain;
import net.openhft.chronicle.wire.converter.NanoTime;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.locks.LockSupport;

/*
Ryzen 9 5950X with Ubuntu 21.10 run with
-Durl=tcp://:1248 -Dkeys=100000 -Dsize=256 -Dpath=/tmp -Dthroughput=100000 -Diterations=3000000 -DpauseMode=balanced -Dbuffered=false
-------------------------------- SUMMARY (end to end) us -------------------------------------------
Percentile   run1         run2         run3         run4         run5      % Variation
50.0:            7.77         7.78         7.74         7.75         7.78         0.41
90.0:            8.30         8.27         8.24         8.27         8.30         0.52
99.0:            8.66         8.53         8.46         8.50         8.50         0.50
99.7:           10.03         9.36         9.39         9.55         9.71         2.45
99.9:           14.35        10.67        10.83        13.58        14.35        18.69
99.97:          16.34        14.54        14.80        15.44        16.27         7.34
99.99:          16.99        15.57        16.54        17.25        16.93         6.71
99.997:         19.36        16.99        18.46        19.42        18.02         8.71
worst:         255.23        96.13        46.91        48.58        69.25        41.16

-Durl=tcp://:1248 -Dkeys=500000 -Dsize=256 -Dpath=/tmp -Dthroughput=500000 -Diterations=15000000 -DpauseMode=balanced -Dbuffered=true
Percentile   run1         run2         run3         run4         run5      % Variation
50.0:           19.87        19.87        20.00        20.00        19.87         0.43
90.0:           24.93        24.93        25.06        25.06        24.93         0.34
99.0:           28.83        28.83        29.02        29.09        28.96         0.59
99.7:           30.62        30.56        30.82        30.88        30.75         0.69
99.9:           32.67        32.42        32.67        32.67        32.61         0.52
99.97:          34.24        33.86        33.98        33.98        33.98         0.25
99.99:          42.43        34.62        34.62        34.50        34.50         0.25
99.997:        230.66        37.70        37.95        35.39        35.52         4.60
99.999:        374.27       144.13       209.66        36.93        37.57        75.72
worst:         545.79       302.59       350.72        43.97        80.26        82.30


-Durl=tcp://:1248 -Dkeys=1000000 -Dsize=256 -Dpath=/tmp -Dthroughput=1000000 -Diterations=30000000 -DpauseMode=balanced -Dbuffered=true
-------------------------------- SUMMARY (end to end) us -------------------------------------------
Percentile   run1         run2         run3         run4         run5      % Variation
50.0:           28.51        30.05        30.05        30.11        29.92         0.43
90.0:           35.14        35.39        35.39        35.39        35.39         0.00
99.0:           40.26        39.87        39.87        39.87        39.74         0.21
99.7:           44.86        42.43        42.18        42.18        42.05         0.61
99.9:         6119.42        44.86        44.35        44.22        44.10         1.15
99.97:        7987.20        62.78        46.66        46.02        45.89        19.71
99.99:        8167.42       294.40       128.38        47.30        47.17        77.75
99.997:       8200.19       578.56       282.11        48.45        48.06        88.04
99.999:       8232.96       599.04       300.54        49.34        48.96        88.22
99.9997:      8249.34       613.38       308.74        51.14        49.86        88.28
worst:        8249.34       625.66       318.98        70.53        91.26        83.99
 */

@SuppressWarnings({"rawtypes", "unchecked"})
public class PerfMapHandlerMain implements JLBHTask {
    static final int THROUGHPUT = Integer.getInteger("throughput", 100_000);
    static final int KEYS = Integer.getInteger("keys", THROUGHPUT);
    static final int ITERATIONS = Integer.getInteger("iterations", THROUGHPUT * 30);
    static final int SIZE = Integer.getInteger("size", 256);
    static final boolean BUFFERED = Jvm.getBoolean("buffered");
    static final String URL = System.getProperty("url", "tcp://:1248");
    private static final PauserMode PAUSER_MODE = PauserMode.valueOf(System.getProperty("pauserMode", PauserMode.balanced.name()));
    private static final String PATH = System.getProperty("path", OS.isLinux() ? "/dev/shm" : OS.TMP);
    final Bytes<Void> key = Bytes.allocateElasticDirect();
    private DummyData data;
    private TimedPassMapServiceIn serviceIn;
    private MethodReader reader;
    private Thread readerThread;
    private ChronicleChannel client;
    private volatile boolean complete;
    private int sent;
    private volatile int count;
    private boolean warmedUp;
    private ChronicleGatewayMain main;
    private ChronicleContext context;

    public static void main(String[] args) {
        if (!URL.contains("//:"))
            System.out.println("Make sure " + MapChronicleGatewayMain.class + " is running first");

        System.out.println("" +
                "-Durl=" + URL + " " +
                "-Dkeys=" + KEYS + " " +
                "-Dsize=" + SIZE + " " +
                "-Dpath=" + PATH + " " +
                "-Dthroughput=" + THROUGHPUT + " " +
                "-Diterations=" + ITERATIONS + " " +
                "-DpauseMode=" + PAUSER_MODE + " " +
                "-Dbuffered=" + BUFFERED);

        JLBHOptions lth = new JLBHOptions()
                .warmUpIterations(50_000)
                .iterations(ITERATIONS)
                .throughput(THROUGHPUT)
                .acquireLock(AffinityLock::acquireLock)
                // disable as otherwise single GC event skews results heavily
                .recordOSJitter(false)
                .accountForCoordinatedOmission(false)
                .runs(5)
                .jlbhTask(new PerfMapHandlerMain());
        new JLBH(lth).start();
    }

    @Override
    public void init(JLBH jlbh) {
        this.data = new DummyData();
        this.data.data(new byte[SIZE - Long.BYTES]);

        final File file = new File(PATH, "perf-map-" + KEYS + ".cm3");
        file.delete();

        final Class<Bytes<?>> keyClass = (Class) Bytes.class;
        try (ChronicleMap<Bytes<?>, DummyData> map = ChronicleMap.of(keyClass, DummyData.class)
                .averageKeySize(8)
                .averageValueSize(16 + SIZE)
                .keyMarshaller(new BytesSizedMarshaller())
                .valueMarshaller(new BytesMarshallableReaderWriter<>(DummyData.class))
                .entries(KEYS)
                .createPersistedTo(file)) {
            Bytes<?> key0 = Bytes.allocateElasticDirect();
            for (int i = 0; i < KEYS; i++) {
                key0.clear().append('k').append(i);
                map.put(key0, data);
            }
            System.out.println(file + " created");
        } catch (IOException e) {
            throw new IORuntimeException(e);
        }

        context = ChronicleContext.newContext(URL).buffered(BUFFERED);

        final MapHandler<DummyData, TimedReply> mapHandler =
                MapHandler.createMapHandler(file.getAbsolutePath(), new PassMapService())
                        .buffered(BUFFERED);

        client = context.newChannelSupplier(mapHandler).get();

        serviceIn = client.methodWriter(TimedPassMapServiceIn.class);
        Reply ignoreReply = Mocker.ignored(Reply.class);
        reader = client.methodReader((TimedReply) timeNS -> {
            jlbh.sample(System.nanoTime() - timeNS);
            count++;
            return ignoreReply;
        });
        readerThread = new Thread(() -> {
            try (AffinityLock lock = AffinityLock.acquireLock()) {
                assert lock != null;
                while (!Thread.currentThread().isInterrupted()) {
                    reader.readOne();
                }
            } catch (Throwable t) {
                if (!complete)
                    t.printStackTrace();
            }
        }, "reply-reader");
        readerThread.setDaemon(true);
        readerThread.start();
    }


    @Override
    public void warmedUp() {
        JLBHTask.super.warmedUp();
        warmedUp = true;
    }

    @Override
    public void run(long startTimeNS) {
        data.timeNS(startTimeNS);
        final PassMapServiceIn serviceIn2 = serviceIn.on(startTimeNS);
        key.clear().append('k').append((int) (startTimeNS / 7 % KEYS));
        switch ((int) (startTimeNS / 11 % 6)) {
            case 0:
            case 1:
                serviceIn2.put(key, data);
                break;
            case 2:
                serviceIn2.remove(key);
                break;
            case 3:
            case 4:
            case 5:
                serviceIn2.get(key);
                break;
        }

        // throttling when warming up.
        if (!warmedUp) {
            long lag = sent++ - count;
            if (lag >= 50)
                LockSupport.parkNanos(lag * 500L);
        }
    }

    @Override
    public void complete() {
        this.complete = true;
        readerThread.interrupt();
        Closeable.closeQuietly(client, main);
    }

    interface TimedPassMapServiceIn {
        PassMapServiceIn on(@NanoTime long timeNS);
    }

    interface PassMapServiceIn extends Closeable {
        void put(Bytes<?> key, DummyData value);

        void get(Bytes<?> key);

        void remove(Bytes<?> key);

        void goodbye();
    }

    interface TimedReply {
        Reply on(@NanoTime long timeNS);
    }

    interface Reply {
        void status(boolean ok);

        void reply(DummyData t);

        void goodbye();
    }

    static class PassMapService extends AbstractMapService<DummyData, TimedReply> implements TimedPassMapServiceIn, PassMapServiceIn {
        private transient long timeNS;
        private transient DummyData dataValue;

        @Override
        public PassMapServiceIn on(long timeNS) {
            this.timeNS = timeNS;
            return this;
        }

        @Override
        public void put(Bytes<?> key, DummyData value) {
            map.put(key, value);
            reply.on(timeNS).status(true);
        }

        @Override
        public void get(Bytes<?> key) {
            reply.on(timeNS).reply(map.getUsing(key, dataValue()));
        }

        private DummyData dataValue() {
            return dataValue == null ? dataValue = new DummyData() : dataValue;
        }

        @Override
        public void remove(Bytes<?> key) {
            reply.on(timeNS).status(map.remove(key, dataValue()));
        }

        @Override
        public void goodbye() {
            reply.on(timeNS).goodbye();
            close();
        }

        @Override
        public Class<DummyData> valueClass() {
            return DummyData.class;
        }

        @Override
        public Class<TimedReply> replyClass() {
            return TimedReply.class;
        }
    }
}

