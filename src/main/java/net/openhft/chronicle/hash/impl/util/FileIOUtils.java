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
        //! Serialize extension with positional I/O on this channel: Windows Java 8 setLength moves its file pointer.
        //! Regression: SharedFileChannelTest.fileGrowthPreservesConcurrentPositionalReadsAndWrites detects
        //! misplaced header/data bytes without this monitor and verifies that smaller requests never truncate.
        synchronized (channel) {
            if (channel.size() >= minimumSize)
                return false;
            file.setLength(minimumSize);
            return true;
        }
    }

    //! The shared channel is deliberately the lock identity used by growFile and writeFully; a per-call lock
    //! would not coordinate users of the same handle. This is an intentional exception to Sonar S2445.
    //! Regression: SharedFileChannelTest.fileGrowthPreservesConcurrentPositionalReadsAndWrites checks offsets
    //! and header bytes while another thread grows the file on Windows Java 8.
    @SuppressWarnings("java:S2445")
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

    //! Use the same shared-channel monitor as growFile/readFully so extension cannot redirect a positional write.
    //! An independent lock would break that contract; suppress Sonar S2445 only for this cooperating I/O helper.
    //! Regression: SharedFileChannelTest.fileGrowthPreservesConcurrentPositionalReadsAndWrites verifies the
    //! written value at its requested offset after concurrent growth, as well as the unchanged header.
    @SuppressWarnings("java:S2445")
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
