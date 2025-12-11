/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash.impl.stage.entry;

/**
 * Strategy for computing and validating checksums on hash entries.
 */
public interface ChecksumStrategy {

    /**
     * Number of bytes used to store the checksum.
     */
    int CHECKSUM_STORED_BYTES = 4;

    /** Computes and writes the checksum into the entry. */
    void computeAndStoreChecksum();

    /** @return true if checksum covers the payload inside the entry. */
    boolean innerCheckSum();

    /** @return freshly computed checksum value. */
    int computeChecksum();

    /** @return previously stored checksum value. */
    int storedChecksum();

    /** @return additional bytes required to store checksum metadata. */
    long extraEntryBytes();
}
