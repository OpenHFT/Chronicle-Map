/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash;

import net.openhft.chronicle.core.io.ClosedIllegalStateException;

/**
 * Thrown when a {@link ChronicleHash} is accessed after {@link ChronicleHash#close()}.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public final class ChronicleHashClosedException extends ClosedIllegalStateException {
    private static final long serialVersionUID = 0L;

    /**
     * Creates an exception for the given hash instance.
     *
     * @param hash closed hash
     */
    public ChronicleHashClosedException(ChronicleHash hash) {
        this(hash.toIdentityString());
    }

    /**
     * Creates an exception with the provided identity string.
     *
     * @param chronicleHashIdentityString identity description
     */
    public ChronicleHashClosedException(String chronicleHashIdentityString) {
        super("Access to " + chronicleHashIdentityString + " after close()");
    }

    /**
     * Creates an exception with custom message and cause.
     *
     * @param s message
     * @param t cause
     */
    public ChronicleHashClosedException(String s, Throwable t) {
        super(s, t);
    }
}
