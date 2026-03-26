/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash.impl.util;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

@SuppressWarnings("ThrowableResultOfMethodCallIgnored")
class ThrowablesTest {

    @Test
    void testPropagateNull() {
        assertThrows(NullPointerException.class, () -> {
            Throwables.propagate(null);
        });
    }

    @Test
    void testPropagateError() {
        assertThrows(AssertionError.class, () -> {
            Throwables.propagate(new AssertionError());
        });
    }

    @Test
    void testPropagateUncheckedException() {
        assertThrows(IllegalStateException.class, () -> {
            Throwables.propagate(new IllegalStateException());
        });
    }

    @Test
    void testPropagateCheckedException() {
        assertThrows(RuntimeException.class, () -> {
            Throwables.propagate(new IOException());
        });
    }

    @Test
    void testPropagateNotWrappingNull() throws IOException {
        assertThrows(NullPointerException.class, () -> {
            Throwables.propagateNotWrapping(null, IOException.class);
        });
    }

    @Test
    void testPropagateNotWrappingNullWrappingType() throws Throwable {
        assertThrows(NullPointerException.class, () -> {
            Throwables.propagateNotWrapping(new IOException(), null);
        });
    }

    @Test
    void testPropagateNotWrappingError() throws IOException {
        assertThrows(AssertionError.class, () -> {
            Throwables.propagateNotWrapping(new AssertionError(), IOException.class);
        });
    }

    @Test
    void testPropagateNotWrappingUncheckedException() throws IOException {
        assertThrows(IllegalStateException.class, () -> {
            Throwables.propagateNotWrapping(new IllegalStateException(), IOException.class);
        });
    }

    @Test
    void testPropagateNotWrappingNotWrappingEException() throws IOException {
        assertThrows(IOException.class, () -> {
            Throwables.propagateNotWrapping(new IOException(), IOException.class);
        });
    }

    @Test
    void testPropagateNotWrappingCheckedException() throws IOException {
        assertThrows(RuntimeException.class, () -> {
            Throwables.propagateNotWrapping(new Exception(), IOException.class);
        });
    }
}
