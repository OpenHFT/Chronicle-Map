//
// Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
//

package net.openhft.chronicle.hash.impl.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public final class FileIOUtils {

    private FileIOUtils() {
    }

    public static void readFully(FileChannel fileChannel, long filePosition, ByteBuffer buffer)
            throws IOException {
        int startBufferPosition = buffer.position();
        while (buffer.remaining() > 0 &&
                buffer.position() < fileChannel.size()) {
            int bytesRead = fileChannel.read(buffer,
                    filePosition + buffer.position() - startBufferPosition);
            if (bytesRead == -1)
                break;
        }
    }

    public static void writeFully(FileChannel fileChannel, long filePosition, ByteBuffer buffer)
            throws IOException {
        int startBufferPosition = buffer.position();
        while (buffer.remaining() > 0) {
            fileChannel.write(buffer, filePosition + buffer.position() - startBufferPosition);
        }
    }
}
