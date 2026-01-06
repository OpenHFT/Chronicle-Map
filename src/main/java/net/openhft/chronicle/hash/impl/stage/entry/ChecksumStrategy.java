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

    /**
     * Indicates whether checksum covers the payload inside the entry.
     *
     * @return true if checksum covers the payload inside the entry
     */
    boolean innerCheckSum();

    /**
     * Computes a checksum using the current entry state.
     *
     * @return freshly computed checksum value
     */
    int computeChecksum();

    /**
     * Returns the checksum value stored with the entry.
     *
     * @return previously stored checksum value
     */
    int storedChecksum();

    /**
     * Returns the extra bytes required to store checksum metadata.
     *
     * @return additional bytes required to store checksum metadata
     */
    long extraEntryBytes();
}
