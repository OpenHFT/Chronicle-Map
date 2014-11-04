/*
 * Copyright 2014 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
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

package net.openhft.chronicle.map;

import net.openhft.lang.Jvm;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringWriter;
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
        ChronicleMap<String, String> map = ChronicleMapBuilder.of(String.class, String.class)
                .entrySize(100 * 1024 * 1024)
                .entries(1000000).create();
        for (int i = 0; i < 10000; i++) {
            char[] chars = new char[i];
            Arrays.fill(chars, '+');
            map.put("key-" + i, new String(chars));
        }
        System.out.printf("System memory: %.1f GB, Extents of map: %.1f GB, disk used: %sB, addressRange: %s%n",
                Double.parseDouble(run("head", "-1", "/proc/meminfo").split("\\s+")[1]) / 1e6,
                file.length() / 1e9,
                run("du", "-h", file.getAbsolutePath()).split("\\s")[0],
                run("grep", "over-sized", "/proc/" + Jvm.getProcessId() + "/maps").split("\\s")[0]);
        // show up in top.
        long time = System.currentTimeMillis();
        while (time + 20000 > System.currentTimeMillis())
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
