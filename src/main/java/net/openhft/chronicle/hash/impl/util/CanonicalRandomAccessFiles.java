/*
 *      Copyright (C) 2015  higherfrequencytrading.com
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

    private static class RafReference {
        RandomAccessFile raf;
        int refCount;

        RafReference(RandomAccessFile raf) {
            this.raf = raf;
            refCount = 1;
        }
    }

    private static final ConcurrentHashMap<File, RafReference> canonicalRafs =
            new ConcurrentHashMap<>();

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
        canonicalRafs.compute(file, (f, ref) -> {
            if (ref == null)
                throw new IllegalStateException("releasing not referenced RAF of file " + f);
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
}
