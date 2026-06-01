/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash.impl.util;

import net.openhft.chronicle.core.Jvm;

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
            long position = filePosition + buffer.position() - startBufferPosition;
            try {
                int bytesRead = fileChannel.read(buffer, position);
                if (bytesRead == -1)
                    break;
            } catch (IOException e) {
                Jvm.warn().on(FileIOUtils.class, "IOException in readFully: " +
                        "fileChannel: " + fileChannel +
                        ", position= " + position +
                        ", filePosition=" + filePosition +
                        ", buffer.position()=" + buffer.position() +
                        ", buffer.remaining()=" + buffer.remaining() +
                        ", startBufferPosition=" + startBufferPosition, e);
                throw e;
            }
        }
    }

    public static void writeFully(FileChannel fileChannel, long filePosition, ByteBuffer buffer)
            throws IOException {
        int startBufferPosition = buffer.position();
        while (buffer.remaining() > 0) {
            long position = filePosition + buffer.position() - startBufferPosition;
            try {
                fileChannel.write(buffer, position);
            } catch (IOException ioe) {
                Jvm.warn().on(FileIOUtils.class, "IOException in writeFully: " +
                        "fileChannel: " + fileChannel +
                        ", position= " + position +
                        ", filePosition=" + filePosition +
                        ", buffer.position()=" + buffer.position() +
                        ", buffer.remaining()=" + buffer.remaining() +
                        ", startBufferPosition=" + startBufferPosition, ioe);
                throw ioe;
            }
        }
    }
}
