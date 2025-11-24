/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash.impl.stage.entry;

/**
 * {@link ChecksumStrategy} implementation used when checksums are disabled.
 *
 * <p>Attempts to compute and store a checksum are rejected, but all read-side
 * verification methods return success and no additional bytes are reserved in
 * the entry layout.
 */
public enum NoChecksumStrategy implements ChecksumStrategy {
    INSTANCE;

    @Override
    public void computeAndStoreChecksum() {
        throw new UnsupportedOperationException("Checksum is not stored in this Chronicle Hash");
    }

    @Override
    public boolean innerCheckSum() {
        return true;
    }

    @Override
    public int computeChecksum() {
        return 0;
    }

    @Override
    public int storedChecksum() {
        return 0;
    }

    @Override
    public long extraEntryBytes() {
        return 0; // no extra bytes to store checksum
    }
}
