/*
 * Copyright 2012-2018 Chronicle Map Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.map;

import net.openhft.chronicle.core.OS;

import java.io.*;
import java.util.Arrays;

/**
 * This example shows that the OS resizes the usage of a CHM as needed.  It is not as critical to worry about this.
 * <p>
 * System memory: 7.7 GB, Extents of map: 2199.0 GB, disk used: 13MB, addressRange: 7d380b7bd000-7f380c000000
 * </p>
 */
public class OSResizesMain {
    public static void main(String[] args) throws IOException, InterruptedException {
        File file = File.createTempFile("over-sized", "deleteme");
        int valueSize = 1000 * 1000;
        byte[] chars = new byte[valueSize];
        ByteArray ba = new ByteArray(chars);
        ChronicleMap<String, ByteArray> map = ChronicleMapBuilder.of(String.class, ByteArray.class)
                .averageKeySize("key-".length() + 4)
                .constantValueSizeBySample(ba)
                .createPersistedTo(file);
        Arrays.fill(chars, (byte) '+');
        for (int i = 0; i < 1000; i++) {
            map.put("key-" + i, ba);
        }
        long start = System.currentTimeMillis();
        System.gc();
        long time0 = System.currentTimeMillis() - start;
        System.out.printf("GC time: %,d ms%n", time0);
        System.out.printf("System memory: %.1f GB, Extents of map: %.1f GB, disk used: %sB, addressRange: %s%n",
                Double.parseDouble(run("head", "-1", "/proc/meminfo").split("\\s+")[1]) / 1e6,
                file.length() / 1e9,
                run("du", "-h", file.getAbsolutePath()).split("\\s")[0],
                run("grep", "over-sized", "/proc/" + OS.getProcessId() + "/maps").split("\\s")[0]);
        // show up in top.
        long time = System.currentTimeMillis();
        while (time + 30000 > System.currentTimeMillis())
            Thread.yield();
        map.close();
        file.delete();
    }

    static String run(String... cmd) throws IOException, InterruptedException {
        ProcessBuilder pb = new ProcessBuilder(cmd);
        pb.redirectErrorStream(true);
        Process p = pb.start();
        InputStreamReader reader = new InputStreamReader(p.getInputStream());
        StringWriter sw = new StringWriter();
        char[] chars = new char[512];
        for (int len; (len = reader.read(chars)) > 0; )
            sw.write(chars, 0, len);
        int exitValue = p.waitFor();
        if (exitValue != 0)
            sw.write("\nexit=" + exitValue);
        p.destroy();
        return sw.toString();
    }
}

class ByteArray implements Serializable {
    final byte[] bytes;

    ByteArray(byte[] bytes) {
        this.bytes = bytes;
    }
}