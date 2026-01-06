/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
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

    /**
     * Creates an exception with the given message and cause.
     *
     * @param message detail message
     * @param cause   underlying cause
     */
    public ChronicleFileLockException(String message, Throwable cause) {
        super(message, cause);
    }
}
