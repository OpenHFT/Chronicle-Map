/*
 * Copyright 2013-2026 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash.locks;

/**
 * Thrown from {@link InterProcessLock#lock()} and {@link InterProcessLock#lockInterruptibly()} when
 * if fails to acquire the lock for some implementation-defined period of time.
 */
public final class InterProcessDeadLockException extends RuntimeException {
    private static final long serialVersionUID = 0L;

    /**
     * Constructs a new {@code InterProcessDeadLockException} with the specified detail message.
     *
     * @param message the detail message. The detail message is saved for later retrieval by the
     *                {@link #getMessage()} method.
     */
    public InterProcessDeadLockException(String message) {
        super(message);
    }

    /**
     * Constructs a new {@code InterProcessDeadLockException} with the specified detail message and
     * cause.
     *
     * @param message the detail message. The detail message is saved for later retrieval by the
     *                {@link #getMessage()} method.
     * @param cause   the cause (which is saved for later retrieval by the {@link #getCause()} method)
     */
    public InterProcessDeadLockException(String message, Throwable cause) {
        super(message, cause);
    }
}
