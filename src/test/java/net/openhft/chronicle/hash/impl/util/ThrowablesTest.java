/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash.impl.util;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertThrows;

@SuppressWarnings("ThrowableResultOfMethodCallIgnored")
public class ThrowablesTest {

    @Test
    public void testPropagateNull() {
        assertThrows(NullPointerException.class, () -> Throwables.propagate(null));
    }

    @Test
    public void testPropagateError() {
        assertThrows(AssertionError.class, () -> Throwables.propagate(new AssertionError()));
    }

    @Test
    public void testPropagateUncheckedException() {
        assertThrows(IllegalStateException.class, () -> Throwables.propagate(new IllegalStateException()));
    }

    @Test
    public void testPropagateCheckedException() {
        assertThrows(RuntimeException.class, () -> Throwables.propagate(new IOException()));
    }

    @Test
    public void testPropagateNotWrappingNull() throws IOException {
        assertThrows(NullPointerException.class, () -> Throwables.propagateNotWrapping(null, IOException.class));
    }

    @Test
    public void testPropagateNotWrappingNullWrappingType() throws Throwable {
        assertThrows(NullPointerException.class, () -> Throwables.propagateNotWrapping(new IOException(), null));
    }

    @Test
    public void testPropagateNotWrappingError() throws IOException {
        assertThrows(AssertionError.class, () -> Throwables.propagateNotWrapping(new AssertionError(), IOException.class));
    }

    @Test
    public void testPropagateNotWrappingUncheckedException() throws IOException {
        assertThrows(IllegalStateException.class, () -> Throwables.propagateNotWrapping(new IllegalStateException(), IOException.class));
    }

    @Test
    public void testPropagateNotWrappingNotWrappingEException() throws IOException {
        assertThrows(IOException.class, () -> Throwables.propagateNotWrapping(new IOException(), IOException.class));
    }

    @Test
    public void testPropagateNotWrappingCheckedException() throws IOException {
        assertThrows(RuntimeException.class, () -> Throwables.propagateNotWrapping(new Exception(), IOException.class));
    }
}
