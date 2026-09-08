/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map.jsr166;

import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class JSR166ThreadFailureTest {
    private final Consumer<JSR166TestCase> assertion;

    public JSR166ThreadFailureTest(String name, Consumer<JSR166TestCase> assertion) {
        this.assertion = assertion;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> assertions() {
        return Arrays.asList(new Object[][]{
                {"fail", (Consumer<JSR166TestCase>) test -> test.threadFail("worker failure")},
                {"true", (Consumer<JSR166TestCase>) test -> test.threadAssertTrue(false)},
                {"false", (Consumer<JSR166TestCase>) test -> test.threadAssertFalse(true)},
                {"null", (Consumer<JSR166TestCase>) test -> test.threadAssertNull(new Object())},
                {"long equality", (Consumer<JSR166TestCase>) test -> test.threadAssertEquals(1L, 2L)},
                {"object equality", (Consumer<JSR166TestCase>) test -> test.threadAssertEquals(new Object(), new Object())},
                {"string equality", (Consumer<JSR166TestCase>) test -> test.threadAssertEquals("expected", "actual")},
                {"identity", (Consumer<JSR166TestCase>) test -> test.threadAssertSame(new Object(), new Object())}
        });
    }

    @Test
    public void workerFailureFailsOwningJUnitInvocation() throws Exception {
        WorkerFixture fixture = new WorkerFixture();
        fixture.assertion = assertion;
        Result result = new JUnitCore().run(new BlockJUnit4ClassRunner(WorkerFixture.class) {
            @Override
            protected Object createTest() {
                return fixture;
            }
        });

        assertEquals(1, result.getRunCount());
        assertTrue("the worker must rethrow its assertion", fixture.workerFailure.get() instanceof AssertionError);
        assertEquals("the owning JUnit invocation must fail", 1, result.getFailureCount());
        //! Matching the original throwable excludes failures caused only by fixture setup or worker joining.
        assertSame(fixture.workerFailure.get(), result.getFailures().get(0).getException());
    }

    public static class WorkerFixture extends JSR166TestCase {
        private Consumer<JSR166TestCase> assertion;
        private final AtomicReference<Throwable> workerFailure = new AtomicReference<>();

        @Test
        public void runWorkerAssertion() throws InterruptedException {
            Thread worker = new Thread(() -> assertion.accept(this), "jsr166-assertion-worker");
            // Observe the uncaught failure without recording it on the fixture's behalf.
            worker.setUncaughtExceptionHandler((thread, failure) -> workerFailure.set(failure));
            worker.start();
            worker.join(5_000);
            assertFalse("worker did not terminate", worker.isAlive());
        }
    }
}
