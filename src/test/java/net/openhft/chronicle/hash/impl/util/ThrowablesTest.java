/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash.impl.util;

import org.junit.Test;

import java.io.IOException;

@SuppressWarnings("ThrowableResultOfMethodCallIgnored")
public class ThrowablesTest {

    @Test(expected = NullPointerException.class)
    public void testPropagateNull() {
        Throwables.propagate(null);
    }

    @Test(expected = AssertionError.class)
    public void testPropagateError() {
        Throwables.propagate(new AssertionError());
    }

    @Test(expected = IllegalStateException.class)
    public void testPropagateUncheckedException() {
        Throwables.propagate(new IllegalStateException());
    }

    @Test(expected = RuntimeException.class)
    public void testPropagateCheckedException() {
        Throwables.propagate(new IOException());
    }

    @Test(expected = NullPointerException.class)
    public void testPropagateNotWrappingNull() throws IOException {
        Throwables.propagateNotWrapping(null, IOException.class);
    }

    @Test(expected = NullPointerException.class)
    public void testPropagateNotWrappingNullWrappingType() throws Throwable {
        Throwables.propagateNotWrapping(new IOException(), null);
    }

    @Test(expected = AssertionError.class)
    public void testPropagateNotWrappingError() throws IOException {
        Throwables.propagateNotWrapping(new AssertionError(), IOException.class);
    }

    @Test(expected = IllegalStateException.class)
    public void testPropagateNotWrappingUncheckedException() throws IOException {
        Throwables.propagateNotWrapping(new IllegalStateException(), IOException.class);
    }

    @Test(expected = IOException.class)
    public void testPropagateNotWrappingNotWrappingEException() throws IOException {
        Throwables.propagateNotWrapping(new IOException(), IOException.class);
    }

    @Test(expected = RuntimeException.class)
    public void testPropagateNotWrappingCheckedException() throws IOException {
        Throwables.propagateNotWrapping(new Exception(), IOException.class);
    }
}
