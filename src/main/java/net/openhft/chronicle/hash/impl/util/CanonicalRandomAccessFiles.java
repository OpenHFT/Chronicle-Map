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

import net.openhft.chronicle.core.CleaningRandomAccessFile;
import net.openhft.chronicle.core.Jvm;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.ConcurrentHashMap;

public final class CanonicalRandomAccessFiles {

    private static final ConcurrentHashMap<File, RafReference> CANONICAL_RAFS = new ConcurrentHashMap<>();

    private CanonicalRandomAccessFiles() {
    }

    public static RandomAccessFile acquire(@NotNull final File file) throws FileNotFoundException {
        return CANONICAL_RAFS.compute(file, (f, ref) -> {
            if (ref == null) {
                try {
                    return new RafReference(new CleaningRandomAccessFile(f, "rw"));
                } catch (FileNotFoundException e) {
                    throw Jvm.rethrow(e);
                }
            } else {
                ref.refCount++;
                return ref;
            }
        }).raf;
    }

    public static void release(@NotNull final File file) throws IOException {
        CANONICAL_RAFS.computeIfPresent(file, (f, ref) -> {
            if (--ref.refCount == 0) {
                try {
                    ref.raf.close();
                } catch (IOException e) {
                    throw Jvm.rethrow(e);
                }
                return null;
            } else {
                return ref;
            }
        });
    }

    // This class is not thread-safe but instances
    // are protected by means of the CANONICAL_RAFS map
    private static final class RafReference {
        private final RandomAccessFile raf;
        private int refCount;

        RafReference(@NotNull final RandomAccessFile raf) {
            this.raf = raf;
            refCount = 1;
        }
    }
}
