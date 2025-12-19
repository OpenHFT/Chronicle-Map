/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import com.google.common.base.Preconditions;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.hash.impl.VanillaChronicleHash;
import net.openhft.chronicle.testframework.process.JavaProcessBuilder;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

public class ExitHookTest {

    private static final int KEY = 1;
    private static final int JVM_STARTUP_WAIT_TIME_MS = 10_000;
    public static final int CHILD_PROCESS_WAIT_TIME_MS = 500;

    private static final String PRE_SHUTDOWN_ACTION_EXECUTED = "PRE_SHUTDOWN_ACTION_EXECUTED";
    private static final String USER_SHUTDOWN_HOOK_EXECUTED = "USER_SHUTDOWN_HOOK_EXECUTED";
    private static final String LOCKED = "LOCKED";
    private static AtomicReference<ChronicleMap<Integer, Integer>> mapReference;

    @TempDir
    Path folder;

    public static void main(String[] args) throws IOException, InterruptedException {

        System.out.println("Child process started, yo");
        File mapFile = new File(args[0]);
        File lockConfirmationFile = new File(args[1]);
        File shutdownActionConfirmationFile = new File(args[2]);
        final ChronicleMapBuilder<Integer, Integer> mapBuilder = createMapBuilder();

        boolean skipCloseOnExitHook = Boolean.parseBoolean(args[3]);

        mapReference = new AtomicReference<>();
        System.out.println("exit hook: " + skipCloseOnExitHook);
        try {
            if (skipCloseOnExitHook) {
                mapBuilder.skipCloseOnExitHook(true);
                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    // In practice, do something with map before shutdown - like create a new map using data from this map.
                    long started = System.currentTimeMillis();

                    ChronicleMap<Integer, Integer> map = mapReference.get();
                    if (map != null)
                        map.close();
                    System.out.println("Closing map took " + (System.currentTimeMillis() - started) + " ms");

                    try {
                        System.out.println("Executing user defined shutdown hook");
                        Preconditions.checkState(shutdownActionConfirmationFile.exists());
                        Files.write(shutdownActionConfirmationFile.toPath(),
                                USER_SHUTDOWN_HOOK_EXECUTED.getBytes());
                    } catch (Exception e) {
                        e.printStackTrace();
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
                        e.printStackTrace();
                        throw new RuntimeException(e);
                    }
                });
            }

            mapReference.set(mapBuilder.createPersistedTo(mapFile));
            System.out.println("Locking");
            try (ExternalMapQueryContext<Integer, Integer, ?> c = mapReference.get().queryContext(KEY)) {
                c.writeLock().lock();
                Files.write(lockConfirmationFile.toPath(), LOCKED.getBytes());
                Thread.sleep(CHILD_PROCESS_WAIT_TIME_MS);
            }
        } catch (Throwable e) {
            e.printStackTrace();
            throw e;
        }
    }

    private static ChronicleMapBuilder<Integer, Integer> createMapBuilder() {
        return ChronicleMapBuilder
                .of(Integer.class, Integer.class)
                .entries(1);
    }

    // http://stackoverflow.com/a/33171840/648955
    public static long getPidOfProcess(Process p) {
        Number pid = Jvm.getValue(p, "pid");
        return pid.longValue();
    }

    @Test
    public void testExitHook() throws IOException, InterruptedException {
        if (!OS.isLinux() && !OS.isMacOSX())
            return; // This test runs only in Unix-like OSes
        File mapFile = newTempFile();
        File lockingConfirmationFile = newTempFile();
        File preShutdownActionExecutionConfirmationFile = newTempFile();
        // Create a process which opens the map, acquires the lock and "hangs" for 30 seconds
        Process process = startOtherProcess(mapFile, lockingConfirmationFile, preShutdownActionExecutionConfirmationFile, false);
        // Let the other process actually reach the moment when it locks the map
        // (JVM startup and chronicle map creation are not instant)
        waitForLockingConfirmation(lockingConfirmationFile);
        // Interrupt that process to trigger Chronicle Map's shutdown hooks
        interruptProcess(getPidOfProcess(process));
        process.waitFor();
        int actual = process.exitValue();
        if (actual != 0) // clean shutdown
            assertEquals(130, actual, "exit code should be 130 for SIGINT interruption when not clean shutdown"); // 130 is exit code for SIGINT (interruption).
        ChronicleMap<Integer, Integer> map = createMapBuilder().createPersistedTo(mapFile);
        try (ExternalMapQueryContext<Integer, Integer, ?> c = map.queryContext(KEY)) {
            // Test that we are able to lock the segment, i. e. the lock was released in other
            // process, thanks to default shutdown hook.
            c.writeLock().lock();
        }

        waitForShutdownConfirmation(preShutdownActionExecutionConfirmationFile, PRE_SHUTDOWN_ACTION_EXECUTED, process);
    }

    @Test
    public void testSkipExitHook() throws IOException, InterruptedException {
        Assumptions.assumeTrue(OS.isLinux() || OS.isMacOSX(), "This test runs only in Unix-like OSes");
        File mapFile = newTempFile();
        File lockingConfirmationFile = newTempFile();
        File shutdownActionConfirmationFile = newTempFile();
        // Create a process which opens the map, acquires the lock and "hangs" for 30 seconds
        Process process = startOtherProcess(mapFile, lockingConfirmationFile, shutdownActionConfirmationFile, true);
        // Let the other process actually reach the moment when it locks the map
        // (JVM startup and chronicle map creation are not instant)
        waitForLockingConfirmation(lockingConfirmationFile);
        // Interrupt that process to trigger Chronicle Map's shutdown hooks
        interruptProcess(getPidOfProcess(process));
        process.waitFor();
        int actual = process.exitValue();
        if (actual != 0) // clean shutdown
            assertEquals(130, actual, "exit code should be 130 for SIGINT interruption when not clean shutdown"); // 130 is exit code for SIGINT (interruption).
        ChronicleMap<Integer, Integer> map = createMapBuilder().createPersistedTo(mapFile);
        try (ExternalMapQueryContext<Integer, Integer, ?> c = map.queryContext(KEY)) {
            // Test that we are able to lock the segment, i. e. the lock was released in other
            // process, thanks to user shutdown hook.
            c.writeLock().lock();
        }

        waitForShutdownConfirmation(shutdownActionConfirmationFile, USER_SHUTDOWN_HOOK_EXECUTED, process);
    }

    @Test
    public void testSerialization1() throws Exception {
        File mapFile = newTempFile();
        ChronicleMap<Integer, Integer> expected = createMapBuilder()
                .skipCloseOnExitHook(true)
                .createPersistedTo(mapFile);
        ChronicleMap<Integer, Integer> actual = createMapBuilder()
                .createPersistedTo(mapFile);
        Field skipCloseOnExitHook = getPrivateField(VanillaChronicleHash.class, "skipCloseOnExitHook");
        assertEquals(skipCloseOnExitHook.get(expected), skipCloseOnExitHook.get(actual), "skipCloseOnExitHook setting should be preserved when map is reopened");
    }

    @Test
    public void testSerialization2() throws Exception {
        File mapFile = newTempFile();
        ChronicleMap<Integer, Integer> expected = createMapBuilder()
                .createPersistedTo(mapFile);
        expected.close();
        ChronicleMap<Integer, Integer> actual = createMapBuilder()
                .createPersistedTo(mapFile);
        Field skipCloseOnExitHook = getPrivateField(VanillaChronicleHash.class, "skipCloseOnExitHook");
        assertEquals(false, skipCloseOnExitHook.get(actual), "skipCloseOnExitHook should default to false when map is reopened");
    }

    private static @NotNull Field getPrivateField(Class<?> aClass, String fieldName) throws NoSuchFieldException {
        Field field = aClass.getDeclaredField(fieldName);
        field.setAccessible(true);
        return field;
    }

    private File newTempFile() throws IOException {
        return Files.createTempFile(folder, "chm", ".tmp").toFile();
    }

    private void waitForLockingConfirmation(File lockingConfirmationFile) throws IOException {
        long started = System.currentTimeMillis();
        do {
            try (Stream<String> lines = Files.lines(lockingConfirmationFile.toPath())) {
                Iterator<String> lineIterator = lines.iterator();
                if (lineIterator.hasNext()) {
                    String line = lineIterator.next();
                    assertEquals(LOCKED, line, "lock confirmation file should contain LOCKED message from child process");
                    assertFalse(lineIterator.hasNext(), "lineIterator.hasNext()");
                    break;
                } else {
                    Jvm.pause(10);
                }
            }
        } while (System.currentTimeMillis() - started < JVM_STARTUP_WAIT_TIME_MS);
    }

    private void waitForShutdownConfirmation(File actionConfirmationFile, String userShutdownHookExecuted, Process process) throws IOException {
        try (Stream<String> lines = Files.lines(actionConfirmationFile.toPath())) {
            Iterator<String> lineIterator = lines.iterator();
            assertTrue(lineIterator.hasNext(), "lineIterator.hasNext()");
            String line = lineIterator.next();
            assertEquals(userShutdownHookExecuted, line, "shutdown confirmation file should contain expected shutdown action message");
            assertFalse(lineIterator.hasNext(), "lineIterator.hasNext()");
        } catch (AssertionError | IOException err) {
            JavaProcessBuilder.printProcessOutput("event hook process", process);
            throw err;
        }
    }

    // http://stackoverflow.com/a/7835467/648955
    @SuppressWarnings("deprecation")
    private void interruptProcess(long pidOfProcess) throws IOException {
        Runtime.getRuntime().exec("kill -SIGINT " + pidOfProcess);
    }

    private Process startOtherProcess(File mapFile, File lockingFile, File outputFile, boolean skipCloseOnExitHook) {
        return JavaProcessBuilder.create(ExitHookTest.class)
                //.inheritingIO()
                .withProgramArguments(mapFile.getAbsolutePath(),
                    lockingFile.getAbsolutePath(),
                    outputFile.getAbsolutePath(),
                    String.valueOf(skipCloseOnExitHook)).start();
    }
}
