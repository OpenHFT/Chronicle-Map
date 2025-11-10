//
// Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
//

package net.openhft.chronicle.hash;

import net.openhft.chronicle.map.MapAbsentEntry;
import net.openhft.chronicle.map.MapEntry;
import net.openhft.chronicle.set.SetEntry;

/**
 * Abstracts entries of hash containers, created by {@link ChronicleHashBuilder}s
 * with {@link ChronicleHashBuilder#checksumEntries(boolean)} configured to {@code true}. There is
 * no method that returns {@code ChecksumEntry}, {@link MapEntry} or {@link SetEntry} could be
 * <i>casted</i> to {@code ChecksumEntry} to access it's methods.
 * <p>
 * See <a href="https://github.com/OpenHFT/Chronicle-Map/blob/ea/docs/CM_Tutorial.adoc#entry-checksums">Entry checksums</a>
 * section in the Chronicle Map tutorial for usage examples of this interface.
 */
public interface ChecksumEntry {

    /**
     * Re-computes and stores checksum for the entry. This method <i>shouldn't</i> be called before
     * or after ordinary operations like {@link MapAbsentEntry#doInsert(Data)}, {@link
     * MapEntry#doReplaceValue(Data)}: it is performed automatically underneath. Call this method,
     * only when value bytes was updated directly, for example though flyweight implementation of
     * a <a href="https://github.com/OpenHFT/Chronicle-Values#value-interface-specification">value
     * interface</a>.
     *
     * @throws UnsupportedOperationException if checksums are not stored in the containing Chronicle
     *                                       Hash
     * @throws RuntimeException              if the context of this entry is locked improperly, e. g. on the
     *                                       {@linkplain HashQueryContext#readLock() read} level, that is not upgradable to the
     *                                       {@linkplain HashQueryContext#updateLock() update} level. Calling {@code updateChecksum()}
     *                                       method is enabled when at least update lock is held.
     */
    void updateChecksum();

    /**
     * Computes checksum from the entry bytes and checks whether it is equal to the stored checksum.
     *
     * @return {@code true} if stored checksum equals to checksum computed from the entry bytes
     * @throws UnsupportedOperationException if checksums are not stored in the containing Chronicle
     *                                       Hash
     * @throws RuntimeException              if the context of this entry is locked improperly, e. g. on the
     *                                       {@linkplain HashQueryContext#readLock() read} level, that is not upgradable to the
     *                                       {@linkplain HashQueryContext#updateLock() update} level. Calling {@code checkSum()} method is
     *                                       enabled when at least update lock is held.
     */
    boolean checkSum();
}
