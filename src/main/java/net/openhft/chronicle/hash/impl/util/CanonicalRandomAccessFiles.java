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

import net.openhft.chronicle.core.Jvm;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.ConcurrentHashMap;

public final class CanonicalRandomAccessFiles {

    private static final ConcurrentHashMap<File, RafReference> canonicalRafs =
            new ConcurrentHashMap<>();

    private CanonicalRandomAccessFiles() {
    }

    public static RandomAccessFile acquire(File file) throws FileNotFoundException {
        return canonicalRafs.compute(file, (f, ref) -> {
            if (ref == null) {
                try {
                    return new RafReference(new RandomAccessFile(f, "rw"));
                } catch (FileNotFoundException e) {
                    throw Jvm.rethrow(e);
                }
            } else {
                ref.refCount++;
                return ref;
            }
        }).raf;
    }

    public static void release(File file) throws IOException {
        canonicalRafs.computeIfPresent(file, (f, ref) -> {
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

    private static class RafReference {
        RandomAccessFile raf;
        int refCount;

        RafReference(RandomAccessFile raf) {
            this.raf = raf;
            refCount = 1;
        }
    }
}
