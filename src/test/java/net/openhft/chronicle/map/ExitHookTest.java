/*
 *      Copyright (C) 2016 Roman Leventov
 *
 *      This program is free software: you can redistribute it and/or modify
 *      it under the terms of the GNU Lesser General Public License as published by
 *      the Free Software Foundation, either version 3 of the License.
 *
 *      This program is distributed in the hope that it will be useful,
 *      but WITHOUT ANY WARRANTY; without even the implied warranty of
 *      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *      GNU Lesser General Public License for more details.
 *
 *      You should have received a copy of the GNU Lesser General Public License
 *      along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.map;

import net.openhft.chronicle.core.OS;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;

public class ExitHookTest {

    private static final int KEY = 1;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testExitHook() throws IOException, InterruptedException {
        if (!OS.isLinux() && !OS.isMacOSX())
            return; // This test runs only in Unix-like OSes
        File mapFile = folder.newFile();
        System.out.println("map file: " + mapFile.getAbsolutePath());
        // Create a process which opens the map, acquires the lock and "hangs" for 30 seconds
        Process process = startOtherProcess(mapFile);
        // Let the other process actually reach the moment when it locks the map
        // (JVM startup and chronicle map creation are not instant)
        Thread.sleep(10_000);
        // Interrupt that process to trigger Chronicle Map's shutdown hooks
        interruptProcess(getPidOfProcess(process));
        process.waitFor();
        System.out.println("other process exit code: " + process.exitValue());
        ChronicleMap<Integer, Integer> map = createMap(mapFile);
        try (ExternalMapQueryContext<Integer, Integer, ?> c = map.queryContext(KEY)) {
            // Test that we are able to lock the segment, i. e. the lock was released in other
            // process thanks to shutdown hooks
            c.writeLock().lock();
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        System.out.println("Other process started");
        File mapFile = new File(args[0]);
        ChronicleMap<Integer, Integer> map = createMap(mapFile);
        try (ExternalMapQueryContext<Integer, Integer, ?> c = map.queryContext(KEY)) {
            c.writeLock().lock();
            Thread.sleep(30_000);
        }
    }

    private static ChronicleMap<Integer, Integer> createMap(File mapFile) throws IOException {
        return ChronicleMap
                .of(Integer.class, Integer.class)
                .entries(1)
                .createPersistedTo(mapFile);
    }

    // http://stackoverflow.com/a/7835467/648955
    private void interruptProcess(long pidOfProcess) throws IOException {
        Runtime.getRuntime().exec("kill -SIGINT " + pidOfProcess);
    }

    // http://stackoverflow.com/a/723914/648955
    private Process startOtherProcess(File mapFile) throws IOException {
        String javaHome = System.getProperty("java.home");
        String javaBin = javaHome +
                File.separator + "bin" +
                File.separator + "java";
        String classpath = System.getProperty("java.class.path");
        System.out.println("Classpath: " + classpath);
        String className = ExitHookTest.class.getCanonicalName();

        ProcessBuilder builder = new ProcessBuilder(
                javaBin, "-cp", classpath, className, mapFile.getAbsolutePath());
        builder.inheritIO();
        return builder.start();
    }

    // http://stackoverflow.com/a/33171840/648955
    public static long getPidOfProcess(Process p) {
        long pid = -1;

        try {
            if (p.getClass().getName().equals("java.lang.UNIXProcess")) {
                Field f = p.getClass().getDeclaredField("pid");
                f.setAccessible(true);
                pid = f.getLong(p);
                f.setAccessible(false);
            }
        } catch (Exception e) {
            pid = -1;
        }
        return pid;
    }
}
