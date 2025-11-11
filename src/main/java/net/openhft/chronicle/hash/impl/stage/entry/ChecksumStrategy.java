/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash.impl.stage.entry;

public interface ChecksumStrategy {

    int CHECKSUM_STORED_BYTES = 4;

    void computeAndStoreChecksum();

    boolean innerCheckSum();

    int computeChecksum();

    int storedChecksum();

    long extraEntryBytes();
}
