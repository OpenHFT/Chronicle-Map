/*
 *      Copyright (C) 2012, 2016  higherfrequencytrading.com
 *      Copyright (C) 2016 Roman Leventov
 *
 *      This program is free software: you can redistribute it and/or modify
 *      it under the terms of the GNU Lesser General Public License as published by
 *      the Free Software Foundation, either version 3 of the License.
 *
 *      This program is distributed in the hope that it will be useful,
 *      but WITHOUT ANY WARRANTY; without even the implied warranty of
 *      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *      GNU Lesser General Public License for more details.
 *
 *      You should have received a copy of the GNU Lesser General Public License
 *      along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

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
