/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash.impl.util;

import net.openhft.chronicle.core.Jvm;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public final class FileIOUtils {

    private FileIOUtils() {
    }

    /**
     * Grows a file without racing positional I/O on its shared channel.
     * On Windows, RandomAccessFile.setLength() moves the same native file pointer
     * used by positional channel I/O. All these operations use the channel monitor.
     * Cross-process allocation still requires the caller's existing file/map locks.
     *
     * @return whether the file was enlarged
     */
    public static boolean growFile(RandomAccessFile file, long minimumSize) throws IOException {
        FileChannel channel = file.getChannel();
        synchronized (channel) {
            if (channel.size() >= minimumSize)
                return false;
            file.setLength(minimumSize);
            return true;
        }
    }

    public static void readFully(FileChannel fileChannel, long filePosition, ByteBuffer buffer)
            throws IOException {
        synchronized (fileChannel) {
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
    }

    public static void writeFully(FileChannel fileChannel, long filePosition, ByteBuffer buffer)
            throws IOException {
        synchronized (fileChannel) {
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
}
