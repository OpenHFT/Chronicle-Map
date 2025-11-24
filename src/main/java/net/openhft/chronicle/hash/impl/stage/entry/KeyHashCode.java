/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash.impl.stage.entry;

/**
 * Stage interface that exposes the hash code of the current key.
 *
 * <p>Implementations compute the hash once for the key associated with the
 * surrounding context and make it available to other stages such as segment
 * selection and hash-lookup probing.
 */
public interface KeyHashCode {

    long keyHashCode();
}
