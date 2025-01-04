/*
 * Copyright 2014-2020 chronicle.software
 *
 * http://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.openhft.chronicle.map.perf;

import net.openhft.chronicle.bytes.Byteable;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.util.NanoSampler;
import net.openhft.chronicle.jlbh.JLBH;
import net.openhft.chronicle.jlbh.JLBHOptions;
import net.openhft.chronicle.jlbh.JLBHTask;
import net.openhft.chronicle.jlbh.TeamCityHelper;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import net.openhft.chronicle.threads.NamedThreadFactory;
import net.openhft.chronicle.values.Array;
import net.openhft.chronicle.values.MaxUtf8Length;
import net.openhft.chronicle.values.Values;

import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * model name      : Intel(R) Core(TM) i7-10710U CPU @ 1.10GHz
 * -------------------------------- SUMMARY (end to end) -----------------------------------------------------------
 * Percentile   run1         run2         run3         run4         run5      % Variation
 * 50:             0.66         0.63         0.64         0.64         0.64         0.83
 * 90:             0.81         0.77         0.78         0.79         0.78         1.02
 * 99:             1.36         1.18         1.19         1.21         1.19         1.78
 * 99.7:           1.76         1.34         1.36         1.48         1.36         6.69
 * 99.9:           2.86         2.00         1.84         2.78         2.33        25.45
 * 99.97:         20.80        20.80        18.88        20.80        20.80         6.35
 * 99.99:         23.10        23.10        22.85        23.23        23.10         1.11
 * worst:       4276.22      2035.71      2465.79      2121.73      2068.48        12.35
 * -------------------------------------------------------------------------------------------------------------------
 */
public class MapJLBHTest implements JLBHTask {
    private static final int KEYS = Integer.getInteger("keys", 10_000_000);
    private static final int ITERATIONS = Math.max(10_000_000, KEYS);
    private static final int WARM_UP_ITERATIONS = Math.max(100_000, KEYS / 2);
    private static final String PATH = System.getProperty("path", OS.getTarget());
    private static final int THROUGHPUT = Integer.getInteger("throughput", 1_000_000);
    private static final int THREADS = 6;
    private int count = 0;
    private ChronicleMap<Long, IFacade0> map;
    private NanoSampler readSampler;
    private NanoSampler writeSampler;
    private JLBH e2eSampler;
    private File mapFile = new File(PATH, "map" + System.nanoTime() + ".cm3");
    private ThreadLocal<Worker> workerTL = ThreadLocal.withInitial(Worker::new);
    private ExecutorService service = new ThreadPoolExecutor(THREADS, THREADS,
            1L, TimeUnit.MINUTES,
            new ArrayBlockingQueue<>(THREADS * 4),
            new NamedThreadFactory("worker", true),
            new ThreadPoolExecutor.CallerRunsPolicy());

    public static void main(String[] args) {
        //Create the JLBH options you require for the benchmark
        JLBHOptions options = new JLBHOptions()
                .warmUpIterations(WARM_UP_ITERATIONS)
                .iterations(ITERATIONS)
                .throughput(THROUGHPUT)
                .runs(5)
                .recordOSJitter(false)
                .accountForCoordinatedOmission(true)
                .jlbhTask(new MapJLBHTest());
        new JLBH(options).start();
    }

    @Override
    public void init(JLBH jlbh) {
        if (mapFile.exists() && !mapFile.delete()) {
            throw new AssertionError("Unable to delete " + mapFile);
        }
        mapFile.deleteOnExit();
        readSampler = jlbh.addProbe("Read");
        writeSampler = jlbh.addProbe("Write");
        e2eSampler = jlbh;

        IFacade0 datum = Values.newNativeReference(IFacade0.class);
        Byteable byteable = (Byteable) datum;
        long capacity = byteable.maxSize();
        System.out.println("Data size: " + capacity);
        byteable.bytesStore(BytesStore.nativeStore(capacity), 0, capacity);

        try {
            final long entries = KEYS;
            map = ChronicleMapBuilder.of(Long.class, IFacade0.class)
                    .constantValueSizeBySample(datum)
                    .entries(entries)
                    .sparseFile(true)
                    .maxBloatFactor(1.2)
                    .actualSegments(32)
                    .createPersistedTo(mapFile);
        } catch (IOException ex) {
            throw Jvm.rethrow(ex);
        }
    }

    @Override
    public void run(long startTimeNS) {
        service.execute(() -> workerTL.get().run(startTimeNS));
    }

    @Override
    public void complete() {
        map.close();
        TeamCityHelper.teamCityStatsLastRun(getClass().getSimpleName(), e2eSampler, ITERATIONS, System.out);
    }

    interface IFacadeBase {
        short getValue0();

        void setValue0(short value);

        byte getValue1();

        void setValue1(byte value);

        byte getValue2();

        void setValue2(byte value);

        int getValue3();

        void setValue3(int value);

        short getValue4();

        void setValue4(short value);

        boolean getValue5();

        void setValue5(boolean value);

        boolean getValue6();

        void setValue6(boolean value);

        short getValue7();

        void setValue7(short value);

        short getValue8();

        void setValue8(short value);

        long getValue9();

        void setValue9(long value);

        long getValue10();

        void setValue10(long value);

        long getValue11();

        void setValue11(long value);

        long getValue12();

        void setValue12(long value);

        long getValue13();

        void setValue13(long value);

        long getValue14();

        void setValue14(long value);

        long getValue15();

        void setValue15(long value);

        short getValue16();

        void setValue16(short value);

        short getValue17();

        void setValue17(short value);

        short getValue18();

        void setValue18(short value);
    }

    //IFacade (at the bottom) is the façade we need tested

    interface IFacadeSon extends IFacadeBase {
        long getValue19();

        void setValue19(long value);

        int getValue20();

        void setValue20(int value);

        int getValue21();

        void setValue21(int value);

        double getValue22();

        void setValue22(double value);

        String getValue23();

        void setValue23(@MaxUtf8Length(10) String value);

        int getValue24();

        void setValue24(int value);

        double getValue25();

        void setValue25(double value);

        byte getValue26();

        void setValue26(byte value);

        double getValue27();

        void setValue27(double value);

        double getValue28();

        void setValue28(double value);
    }

    interface IFacadeDaughter extends IFacadeBase {

        double getValue29();

        void setValue29(double value);

        double getValue30();

        void setValue30(double value);

        double getValue31();

        void setValue31(double value);

        double getValue32();

        void setValue32(double value);

        long getValue33();

        void setValue33(long value);

        String getValue34();

        void setValue34(@MaxUtf8Length(11) String value);

        int getValue35();

        void setValue35(int value);

        String getValue36();

        void setValue36(@MaxUtf8Length(11) String value);

        int getValue37();

        void setValue37(int value);

        long getValue38();

        void setValue38(long value);

        short getValue39();

        void setValue39(short value);

        long getValue40();

        void setValue40(long value);

        String getValue41();

        void setValue41(@MaxUtf8Length(43) String value);

        long getValue42();

        void setValue42(long value);

        long getValue43();

        void setValue43(long value);

        long getValue44();

        void setValue44(long value);

        long getValue45();

        void setValue45(long value);

        long getValue46();

        void setValue46(long value);

        byte getValue47();

        void setValue47(byte value);

        byte getValue48();

        void setValue48(byte value);

        double getValue49();

        void setValue49(double value);

        double getValue50();

        void setValue50(double value);

        double getValue51();

        void setValue51(double value);

        byte getValue52();

        void setValue52(byte value);

        byte getValue53();

        void setValue53(byte value);

        byte getValue54();

        void setValue54(byte value);

        byte getValue55();

        void setValue55(byte value);

        byte getValue56();

        void setValue56(byte value);

        long getValue57();

        void setValue57(long value);

        byte getValue58();

        void setValue58(byte value);

        double getValue59();

        void setValue59(double value);

        double getValue60();

        void setValue60(double value);

        double getValue61();

        void setValue61(double value);

        double getValue62();

        void setValue62(double value);

        double getValue63();

        void setValue63(double value);

        long getValue64();

        void setValue64(long value);

        long getValue65();

        void setValue65(long value);

        double getValue66();

        void setValue66(double value);

        double getValue67();

        void setValue67(double value);

        short getValue68();

        void setValue68(short value);

        String getValue69();

        void setValue69(@MaxUtf8Length(101) String value);

        String getValue70();

        void setValue70(@MaxUtf8Length(17) String value);

        boolean getValue71();

        void setValue71(boolean value);

        boolean getValue72();

        void setValue72(boolean value);

        String getValue73();

        void setValue73(@MaxUtf8Length(11) String value);

        String getValue74();

        void setValue74(@MaxUtf8Length(9) String value);

        byte getValue75();

        void setValue75(byte value);

        int getValue76();

        void setValue76(int value);

        int getValue77();

        void setValue77(int value);

        String getValue78();

        void setValue78(@MaxUtf8Length(3) String value);

        int getValue79();

        void setValue79(int value);

        double getValue80();

        void setValue80(double value);

        byte getValue81();

        void setValue81(byte value);

        byte getValue82();

        void setValue82(byte value);

        byte getValue83();

        void setValue83(byte value);

        byte getValue84();

        void setValue84(byte value);

        byte getValue85();

        void setValue85(byte value);

        byte getValue86();

        void setValue86(byte value);

        byte getValue87();

        void setValue87(byte value);

        byte getValue88();

        void setValue88(byte value);

        int getValue89();

        void setValue89(int value);

        int getValue90();

        void setValue90(int value);

        int getValue91();

        void setValue91(int value);

        int getValue92();

        void setValue92(int value);

        int getValue93();

        void setValue93(int value);

        int getValue94();

        void setValue94(int value);

        int getValue95();

        void setValue95(int value);

        int getValue96();

        void setValue96(int value);

        int getValue97();

        void setValue97(int value);

        int getValue98();

        void setValue98(int value);

        int getValue99();

        void setValue99(int value);
    }

    interface IFacade0 extends IFacadeBase {
        @Array(length = 3)
        void setSonAt(int idx, IFacadeSon son);

        IFacadeSon getSonAt(int idx);
    }

    interface IFacade extends IFacadeBase {
        @Array(length = 3)
        void setDaughterAt(int idx, IFacadeDaughter daughter);

        IFacadeDaughter getDaughterAt(int idx);

        @Array(length = 3)
        void setSonAt(int idx, IFacadeSon son);

        IFacadeSon getSonAt(int idx);
    }

    class Worker {
        private IFacade0 datum = Values.newNativeReference(IFacade0.class);
        private IFacade0 datum2 = Values.newNativeReference(IFacade0.class);
        private Random random = new Random();

        public Worker() {
            Byteable byteable = (Byteable) datum;
            long capacity = byteable.maxSize();
            byteable.bytesStore(BytesStore.nativeStore(capacity), 0, capacity);
        }

        public void run(long startTimeNS) {
            long runNo = generateKey();
            datum.setValue10(startTimeNS);
            long startWrite = System.nanoTime();
            map.put(runNo, datum);
            long endWrite = System.nanoTime();
            long writeTime = endWrite - startWrite;

            long runNo2 = generateKey();
            long startRead = System.nanoTime();
            IFacade0 dataRead = map.getUsing(runNo2, datum2);
            long endRead = System.nanoTime();

            synchronized (e2eSampler) {
                writeSampler.sampleNanos(writeTime);
                readSampler.sampleNanos(endRead - startRead);
                e2eSampler.sampleNanos(endRead - startTimeNS);
            }
        }

        private long generateKey() {
            // warmup
            if (count < WARM_UP_ITERATIONS)
                return count++;
            // logarithmic distribution
            return (long) Math.pow(KEYS, random.nextDouble());
        }
    }
}
