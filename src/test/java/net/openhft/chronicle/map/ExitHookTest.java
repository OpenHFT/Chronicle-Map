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
import net.bytebuddy.implementation.bytecode.Throw;
import net.openhft.chronicle.core.OS;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.junit.Assert.*;

public class ExitHookTest {

    private static final int KEY = 1;
    private static final int JVM_STARTUP_WAIT_TIME_MS = 15_000;
    private static final String PRE_SHUTDOWN_ACTION_EXECUTED = "PRE_SHUTDOWN_ACTION_EXECUTED";
    private static final String USER_SHUTDOWN_HOOK_EXECUTED = "USER_SHUTDOWN_HOOK_EXECUTED";

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    public static void main(String[] args) throws IOException, InterruptedException {
        try {
            System.out.println("Other process started, yo");
            File mapFile = new File(args[0]);
            System.out.println("1");
            File shutdownActionConfirmationFile = new File(args[1]);
            System.out.println("2");
            ChronicleMapBuilder<Integer, Integer> mapBuilder = null;
            try {
                mapBuilder = createMapBuilder();
            } catch (Throwable e) {
                e.printStackTrace();
            }
            System.out.println("3");
            boolean skipCloseOnExitHook = false;
            System.out.println("4");
            if (Boolean.parseBoolean(args[2])) {
                skipCloseOnExitHook = true;
            }
            AtomicReference<ChronicleMap<Integer, Integer>> mapReference = new AtomicReference<>();
            System.out.println("exit hook" + skipCloseOnExitHook);
            if (skipCloseOnExitHook) {
                mapBuilder.skipCloseOnExitHook(skipCloseOnExitHook);
                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    // In practice, do something with map before shutdown - like create a new map using data from this map.
                    mapReference.get().close();
                    try {
                        System.out.println("Executing user defined shutdown hook");
                        Preconditions.checkState(shutdownActionConfirmationFile.exists());
                        Files.write(shutdownActionConfirmationFile.toPath(),
                                USER_SHUTDOWN_HOOK_EXECUTED.getBytes());
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }));
            } else {
                mapBuilder.setPreShutdownAction(() -> {
                    try {
                        System.out.println("Executing pre-shutdown action");
                        Preconditions.checkState(shutdownActionConfirmationFile.exists());
                        Files.write(shutdownActionConfirmationFile.toPath(),
                                PRE_SHUTDOWN_ACTION_EXECUTED.getBytes());
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
            }
            mapReference.set(mapBuilder.createPersistedTo(mapFile));
            try (ExternalMapQueryContext<Integer, Integer, ?> c = mapReference.get().queryContext(KEY)) {
                c.writeLock().lock();
                Thread.sleep(30_000);
            }
        } catch (Exception e) {
            e.printStackTrace();
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
        Process process = startOtherProcess(mapFile, preShutdownActionExecutionConfirmationFile, false);
        // Let the other process actually reach the moment when it locks the map
        // (JVM startup and chronicle map creation are not instant)
        Thread.sleep(JVM_STARTUP_WAIT_TIME_MS);
        // Interrupt that process to trigger Chronicle Map's shutdown hooks
        interruptProcess(getPidOfProcess(process));
        process.waitFor();
        int actual = process.exitValue();
        if (actual != 0) // clean shutdown
            assertEquals(130, actual); // 130 is exit code for SIGINT (interruption).
        ChronicleMap<Integer, Integer> map = createMapBuilder().createPersistedTo(mapFile);
        try (ExternalMapQueryContext<Integer, Integer, ?> c = map.queryContext(KEY)) {
            // Test that we are able to lock the segment, i. e. the lock was released in other
            // process, thanks to default shutdown hook.
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

    @Test
    public void testSkipExitHook() throws IOException, InterruptedException {
        if (!OS.isLinux() && !OS.isMacOSX())
            return; // This test runs only in Unix-like OSes
        File mapFile = folder.newFile();
        File shutdownActionConfirmationFile = folder.newFile();
        // Create a process which opens the map, acquires the lock and "hangs" for 30 seconds
        Process process = startOtherProcess(mapFile, shutdownActionConfirmationFile, true);
        // Let the other process actually reach the moment when it locks the map
        // (JVM startup and chronicle map creation are not instant)
        Thread.sleep(JVM_STARTUP_WAIT_TIME_MS);
        // Interrupt that process to trigger Chronicle Map's shutdown hooks
        interruptProcess(getPidOfProcess(process));
        process.waitFor();
        int actual = process.exitValue();
        if (actual != 0) // clean shutdown
            assertEquals(130, actual); // 130 is exit code for SIGINT (interruption).
        ChronicleMap<Integer, Integer> map = createMapBuilder().createPersistedTo(mapFile);
        try (ExternalMapQueryContext<Integer, Integer, ?> c = map.queryContext(KEY)) {
            // Test that we are able to lock the segment, i. e. the lock was released in other
            // process, thanks to user shutdown hook.
            c.writeLock().lock();
        }
        try (Stream<String> lines = Files.lines(shutdownActionConfirmationFile.toPath())) {
            Iterator<String> lineIterator = lines.iterator();
            assertTrue(lineIterator.hasNext());
            String line = lineIterator.next();
            assertEquals(USER_SHUTDOWN_HOOK_EXECUTED, line);
            assertFalse(lineIterator.hasNext());
        }
    }

    // http://stackoverflow.com/a/7835467/648955
    private void interruptProcess(long pidOfProcess) throws IOException {
        Runtime.getRuntime().exec("kill -SIGINT " + pidOfProcess);
    }

    // http://stackoverflow.com/a/723914/648955
    private Process startOtherProcess(File mapFile, File outputFile, boolean skipCloseOnExitHook) throws IOException {
        String javaHome = System.getProperty("java.home");
        String javaBin = javaHome +
                File.separator + "bin" +
                File.separator + "java";
        String classpath = System.getProperty("java.class.path");
        System.out.println("Classpath: " + classpath);
        String className = ExitHookTest.class.getCanonicalName();

        String[] command = new String[] {javaBin, "-cp", classpath, className,
                mapFile.getAbsolutePath(), outputFile.getAbsolutePath(), String.valueOf(skipCloseOnExitHook)};
        ProcessBuilder builder = new ProcessBuilder(command);
        builder.inheritIO();
        return builder.start();
    }
}
