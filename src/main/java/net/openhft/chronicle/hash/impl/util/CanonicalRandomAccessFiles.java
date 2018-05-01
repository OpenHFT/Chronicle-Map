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
