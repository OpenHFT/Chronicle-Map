/*
 * Copyright 2012-2018 Chronicle Map Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
