/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map.issue;

import org.junit.jupiter.api.Test;

import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class ParallelStartupCleanupTest {
    @Test
    void waitsForInterruptedWorkerToCloseHandleAndRestoresInterrupt() throws Exception {
        Path file = Files.createTempFile("parallel-startup-cleanup-", ".bin");
        CountDownLatch opened = new CountDownLatch(1);
        CountDownLatch interrupted = new CountDownLatch(1);
        CountDownLatch allowClose = new CountDownLatch(1);
        CountDownLatch returned = new CountDownLatch(1);
        AtomicBoolean closed = new AtomicBoolean();
        AtomicBoolean interruptRestored = new AtomicBoolean();
        AtomicReference<Throwable> workerFailure = new AtomicReference<>();
        AtomicReference<Throwable> callerFailure = new AtomicReference<>();
        AssertionError primary = new AssertionError("original startup timeout");
        Thread worker = new Thread(() -> {
            try (RandomAccessFile handle = new RandomAccessFile(file.toFile(), "rw")) {
                handle.writeInt(42);
                opened.countDown();
                try {
                    new CountDownLatch(1).await();
                } catch (InterruptedException expected) {
                    interrupted.countDown();
                    allowClose.await();
                }
            } catch (Throwable failure) {
                workerFailure.set(failure);
            } finally {
                closed.set(true);
            }
        }, "cleanup-file-worker");
        Thread caller = new Thread(() -> {
            Thread.currentThread().interrupt();
            try {
                ParallelStartupTest.interruptAndJoin(new Thread[]{worker}, primary,
                        TimeUnit.SECONDS.toNanos(2), true);
                interruptRestored.set(Thread.currentThread().isInterrupted());
            } catch (Throwable failure) {
                callerFailure.set(failure);
            } finally {
                returned.countDown();
            }
        }, "cleanup-caller");
        worker.setDaemon(true);
        caller.setDaemon(true);
        try {
            worker.start();
            assertTrue(opened.await(2, TimeUnit.SECONDS));
            caller.start();
            assertTrue(interrupted.await(2, TimeUnit.SECONDS));
            assertFalse(returned.await(50, TimeUnit.MILLISECONDS), "Cleanup returned while the worker still owned its handle");
            allowClose.countDown();
            assertTrue(returned.await(2, TimeUnit.SECONDS));
            worker.join(2000);
            assertFalse(worker.isAlive());
            assertTrue(closed.get());
            assertTrue(interruptRestored.get());
            assertNull(workerFailure.get());
            assertNull(callerFailure.get());
            assertEquals("original startup timeout", primary.getMessage());
            assertEquals(0, primary.getSuppressed().length);
        } finally {
            allowClose.countDown();
            caller.join(2500);
            worker.join(2500);
            if (!worker.isAlive())
                Files.deleteIfExists(file);
        }
    }

    @Test
    void reportsSurvivorsWithoutReplacingThePrimaryFailure() throws Exception {
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch release = new CountDownLatch(1);
        Thread[] workers = new Thread[2];
        for (int index = 0; index < workers.length; index++) {
            workers[index] = new Thread(() -> {
                ready.countDown();
                while (release.getCount() != 0) {
                    try {
                        release.await();
                    } catch (InterruptedException ignored) {
                        // Deliberately uncooperative control; the test releases it below.
                    }
                }
            }, "cleanup-survivor-" + index);
            workers[index].setDaemon(true);
            workers[index].start();
        }
        AssertionError primary = new AssertionError("original failure");
        try {
            assertTrue(ready.await(2, TimeUnit.SECONDS));
            ParallelStartupTest.interruptAndJoin(workers, primary, TimeUnit.MILLISECONDS.toNanos(50), false);
            assertEquals("original failure", primary.getMessage());
            assertEquals(2, primary.getSuppressed().length);
            for (int index = 0; index < workers.length; index++) {
                assertTrue(workers[index].isAlive());
                assertTrue(primary.getSuppressed()[index].getMessage().contains(workers[index].getName()));
            }
        } finally {
            release.countDown();
            for (Thread worker : workers)
                worker.join(2500);
        }
    }
}
