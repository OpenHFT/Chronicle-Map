/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map.issue;

import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import org.junit.jupiter.api.RepeatedTest;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static org.junit.jupiter.api.Assertions.fail;

class ParallelStartupTest {

    @RepeatedTest(5)
    void test() {
        final File file = IOTools.createTempFile("issue342");
        final long started = System.nanoTime();
        final Thread[] workers = new Thread[16];
        final AtomicInteger successes = new AtomicInteger();
        final AtomicReferenceArray<Throwable> failures = new AtomicReferenceArray<>(workers.length);
        final AtomicReferenceArray<String> stages = new AtomicReferenceArray<>(workers.length);
        for (int i = 0; i < workers.length; i++) {
            final int id = i;
            stages.set(id, "not started");
            workers[i] = new Thread(() -> {
                stages.set(id, "opening");
                try (ChronicleMap<Integer, CharSequence> map = ChronicleMapBuilder
                        .of(Integer.class, CharSequence.class)
                        .entries(100).averageValueSize(100).createPersistedTo(file)) {
                    stages.set(id, "writing");
                    map.put(id, Thread.currentThread().getName());
                    stages.set(id, "closing");
                } catch (Throwable failure) {
                    failures.set(id, new AssertionError("Worker " + id + " failed while "
                            + stages.get(id) + " after " + elapsedMillis(started)
                            + " ms; fileLength=" + file.length(), failure));
                    return;
                }
                stages.set(id, "closed");
                successes.incrementAndGet();
            }, "parallel-startup-" + id);
            // A stuck worker must not keep the test JVM alive after the diagnostic deadline.
            workers[i].setDaemon(true);
        }
        for (Thread worker : workers)
            worker.start();

        boolean interrupted = false;
        final long deadline = started + TimeUnit.SECONDS.toNanos(90);
        try {
            for (Thread worker : workers) {
                long remaining = deadline - System.nanoTime();
                if (remaining > 0)
                    worker.join(Math.max(1, TimeUnit.NANOSECONDS.toMillis(remaining)));
            }
        } catch (InterruptedException e) {
            interrupted = true;
            Thread.currentThread().interrupt();
        }

        if (interrupted || successes.get() != workers.length) {
            StringBuilder detail = new StringBuilder("Parallel startup failed: expected ")
                    .append(workers.length).append(" successful workers, completed=").append(successes.get())
                    .append(", elapsedMs=").append(elapsedMillis(started))
                    .append(", interrupted=").append(interrupted)
                    .append("\nfile=").append(file.getAbsolutePath())
                    .append(", exists=").append(file.exists()).append(", length=").append(file.length())
                    .append(", freeBytes=").append(file.getParentFile().getFreeSpace())
                    .append("\nheader[0..63]=").append(header(file))
                    .append("\njava=").append(System.getProperty("java.runtime.version"))
                    .append(", os=").append(System.getProperty("os.name"));
            for (int i = 0; i < workers.length; i++) {
                Thread worker = workers[i];
                detail.append('\n').append(worker.getName()).append(": stage=").append(stages.get(i))
                        .append(", state=").append(worker.getState());
                for (StackTraceElement frame : worker.getStackTrace())
                    detail.append("\n    at ").append(frame);
            }
            AssertionError error = new AssertionError(detail.toString());
            for (int i = 0; i < workers.length; i++) {
                if (failures.get(i) != null)
                    error.addSuppressed(failures.get(i));
                if (workers[i].isAlive())
                    workers[i].interrupt();
            }
            fail(error.getMessage(), error);
        }
    }

    private static long elapsedMillis(long started) {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
    }

    private static String header(File file) {
        // Read only a bounded prefix after failure; never repair or rewrite the evidence.
        try (RandomAccessFile input = new RandomAccessFile(file, "r")) {
            byte[] bytes = new byte[64];
            int count = input.read(bytes);
            StringBuilder hex = new StringBuilder();
            for (int i = 0; i < count; i++)
                hex.append(String.format("%02x ", bytes[i] & 0xff));
            return hex.toString();
        } catch (IOException e) {
            return "unavailable: " + e;
        }
    }
}
