/*
 * Copyright 2014-2018 Chronicle Software
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

import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesMarshallable;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.jlbh.JLBH;
import net.openhft.chronicle.core.jlbh.JLBHOptions;
import net.openhft.chronicle.core.jlbh.JLBHTask;
import net.openhft.chronicle.core.util.NanoSampler;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;

import java.io.File;
import java.io.IOException;

public class MapJLBHTest implements JLBHTask {
    private static final int WARM_UP_ITERATIONS = 40_000;
    private ChronicleMap<Long, Datum> read;
    private ChronicleMap<Long, Datum> write;
    private NanoSampler readSampler;
    private NanoSampler writeSampler;
    private NanoSampler e2eSampler;
    private File mapFile = new File("perfmap/map.cm3");

    public static void main(String[] args) {
        //Create the JLBH options you require for the benchmark
        JLBHOptions options = new JLBHOptions()
                .warmUpIterations(WARM_UP_ITERATIONS)
                .iterations(500_000)
                .throughput(40_000)
                .runs(3)
                .recordOSJitter(false)
                .accountForCoordinatedOmmission(false)
                .jlbhTask(new MapJLBHTest());
        new JLBH(options).start();
    }

    @Override
    public void init(JLBH jlbh) {
        IOTools.deleteDirWithFiles("perfmap", 10);
        mapFile.getParentFile().mkdirs();
        readSampler = jlbh.addProbe("Read");
        writeSampler = jlbh.addProbe("Write");
        e2eSampler = jlbh;

        Datum sampleValue = new Datum();
        try {
            write = ChronicleMapBuilder.of(Long.class, Datum.class).constantValueSizeBySample(sampleValue).entries(1_100_000).createOrRecoverPersistedTo(mapFile);
            read = ChronicleMapBuilder.of(Long.class, Datum.class).constantValueSizeBySample(sampleValue).entries(1_100_000).createOrRecoverPersistedTo(mapFile);
        } catch (IOException ex) {
            Jvm.rethrow(ex);
        }
    }


    private long counter = -WARM_UP_ITERATIONS;
    private Datum datum = new Datum();
    @Override
    public void run(long startTimeNS) {
        long runNo = counter++;
        datum.ts = startTimeNS;
        long startWrite = System.nanoTime();
        write.put(runNo, datum);
        long endWrite = System.nanoTime();
        long writeTime = endWrite - startWrite;
        writeSampler.sampleNanos(writeTime);

        Datum dataRead = read.getUsing(runNo - 1, datum);
        long nanos = System.nanoTime();
        if (dataRead != null) {
            readSampler.sampleNanos(nanos - endWrite);
        }
        e2eSampler.sampleNanos(nanos - startTimeNS);
    }

    @Override
    public void warmedUp() {
        counter = 0;
    }

    @Override
    public void complete() {
        write.close();
        read.close();
    }

    private static class Datum implements BytesMarshallable {
        public long ts = 0;
        public byte[] filler = new byte[4088];

        @Override
        public void readMarshallable(BytesIn bytes) throws IORuntimeException {
            ts = bytes.readLong();
            bytes.read(filler);
        }

        @Override
        public void writeMarshallable(BytesOut bytes) {
            bytes.writeLong(ts);
            bytes.write(filler);
        }
    }
}
