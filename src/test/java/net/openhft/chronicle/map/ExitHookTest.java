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

import com.google.common.base.Preconditions;
import net.openhft.chronicle.core.OS;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.util.Iterator;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ExitHookTest {

    private static final int KEY = 1;
    private static final String PRE_SHUTDOWN_ACTION_EXECUTED = "PRE_SHUTDOWN_ACTION_EXECUTED";

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    public static void main(String[] args) throws IOException, InterruptedException {
        System.out.println("Other process started");
        File mapFile = new File(args[0]);
        File preShutdownActionExecutionConfirmationFile = new File(args[1]);
        ChronicleMapBuilder<Integer, Integer> mapBuilder = createMapBuilder();
        mapBuilder.setPreShutdownAction(() -> {
            try {
                System.out.println("Executing pre-shutdown action");
                Preconditions.checkState(preShutdownActionExecutionConfirmationFile.exists());
                Files.write(preShutdownActionExecutionConfirmationFile.toPath(),
                        PRE_SHUTDOWN_ACTION_EXECUTED.getBytes());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        ChronicleMap<Integer, Integer> map = mapBuilder.createPersistedTo(mapFile);
        try (ExternalMapQueryContext<Integer, Integer, ?> c = map.queryContext(KEY)) {
            c.writeLock().lock();
            Thread.sleep(30_000);
        }
    }

    private static ChronicleMapBuilder<Integer, Integer> createMapBuilder()
            throws IOException {
        return ChronicleMapBuilder
                .of(Integer.class, Integer.class)
                .entries(1);
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

    @Test
    public void testExitHook() throws IOException, InterruptedException {
        if (!OS.isLinux() && !OS.isMacOSX())
            return; // This test runs only in Unix-like OSes
        File mapFile = folder.newFile();
        File preShutdownActionExecutionConfirmationFile = folder.newFile();
        // Create a process which opens the map, acquires the lock and "hangs" for 30 seconds
        Process process = startOtherProcess(mapFile, preShutdownActionExecutionConfirmationFile);
        // Let the other process actually reach the moment when it locks the map
        // (JVM startup and chronicle map creation are not instant)
        Thread.sleep(10_000);
        // Interrupt that process to trigger Chronicle Map's shutdown hooks
        interruptProcess(getPidOfProcess(process));
        process.waitFor();
        int actual = process.exitValue();
        if (actual != 0) // clean shutdown
            assertEquals(130, actual); // 130 is exit code for SIGINT (interruption).
        ChronicleMap<Integer, Integer> map = createMapBuilder().createPersistedTo(mapFile);
        try (ExternalMapQueryContext<Integer, Integer, ?> c = map.queryContext(KEY)) {
            // Test that we are able to lock the segment, i. e. the lock was released in other
            // process thanks to shutdown hooks
            c.writeLock().lock();
        }
        try (Stream<String> lines = Files.lines(preShutdownActionExecutionConfirmationFile.toPath())) {
            Iterator<String> lineIterator = lines.iterator();
            assertTrue(lineIterator.hasNext());
            String line = lineIterator.next();
            assertEquals(PRE_SHUTDOWN_ACTION_EXECUTED, line);
            assertFalse(lineIterator.hasNext());
        }
    }

    // http://stackoverflow.com/a/7835467/648955
    private void interruptProcess(long pidOfProcess) throws IOException {
        Runtime.getRuntime().exec("kill -SIGINT " + pidOfProcess);
    }

    // http://stackoverflow.com/a/723914/648955
    private Process startOtherProcess(File mapFile, File outputFile) throws IOException {
        String javaHome = System.getProperty("java.home");
        String javaBin = javaHome +
                File.separator + "bin" +
                File.separator + "java";
        String classpath = System.getProperty("java.class.path");
        System.out.println("Classpath: " + classpath);
        String className = ExitHookTest.class.getCanonicalName();

        String[] command = new String[] {javaBin, "-cp", classpath, className,
                mapFile.getAbsolutePath(), outputFile.getAbsolutePath()};
        ProcessBuilder builder = new ProcessBuilder(command);
        builder.inheritIO();
        return builder.start();
    }
}
