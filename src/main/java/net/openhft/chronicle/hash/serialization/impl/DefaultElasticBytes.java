//
// Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
//

package net.openhft.chronicle.hash.serialization.impl;

import net.openhft.chronicle.bytes.Bytes;

final class DefaultElasticBytes {

    static final int DEFAULT_BYTES_CAPACITY = 32;

    private DefaultElasticBytes() {
    }

    static Bytes<?> allocateDefaultElasticBytes(long bytesCapacity) {
        if (bytesCapacity <= 0x7FFFFFF0) {
            return Bytes.elasticHeapByteBuffer((int) bytesCapacity);
        } else {
            return Bytes.allocateElasticDirect(bytesCapacity);
        }
    }
}
