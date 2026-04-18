/*
 * Copyright 2013-2026 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash;

import java.io.File;

/**
 * This exception is thrown, when a Chronicle Hash recovery using {@link
 * ChronicleHashBuilder#recoverPersistedTo(File, boolean)} method is impossible, for example,
 * if the persistence file is corrupted too much.
 *
 * @see ChronicleHashBuilder#recoverPersistedTo(File, boolean)
 */
public final class ChronicleHashRecoveryFailedException extends RuntimeException {
    private static final long serialVersionUID = 0L;

    /**
     * Constructs a new {@code ChronicleHashRecoveryFailedException} with the specified cause.
     *
     * @param cause the cause (which is saved for later retrieval by the {@link #getCause()} method)
     */
    public ChronicleHashRecoveryFailedException(Throwable cause) {
        super(cause);
    }

    /**
     * Constructs a new {@code ChronicleHashRecoveryFailedException} with the specified detail
     * message.
     *
     * @param message the detail message. The detail message is saved for later retrieval by the
     *                {@link #getMessage()} method.
     */
    public ChronicleHashRecoveryFailedException(String message) {
        super(message);
    }
}
