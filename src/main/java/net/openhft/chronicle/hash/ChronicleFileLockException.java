/*
 * Copyright 2013-2026 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash;

/**
 * This exception is thrown, when a Chronicle Hash cannot acquire a required file lock.
 */
public final class ChronicleFileLockException extends RuntimeException {
    private static final long serialVersionUID = -2034623786298623984L;

    /**
     * Constructs a new {@code ChronicleHashLockException} with the specified cause.
     *
     * @param cause the cause (which is saved for later retrieval by the {@link #getCause()} method)
     */
    public ChronicleFileLockException(Throwable cause) {
        super(cause);
    }

    /**
     * Constructs a new {@code ChronicleHashLockException} with the specified detail
     * message.
     *
     * @param message the detail message. The detail message is saved for later retrieval by the
     *                {@link #getMessage()} method.
     */
    public ChronicleFileLockException(String message) {
        super(message);
    }

    public ChronicleFileLockException(String message, Throwable cause) {
        super(message, cause);
    }
}
