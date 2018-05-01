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

package net.openhft.chronicle.set;

import net.openhft.chronicle.core.Jvm;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Map;

/**
 * @author Rob Austin.
 */
public class Builder {

    public static final int SIZE = 10_000;
    // added to ensure uniqueness
    static int count;
    static String WIN_OS = "WINDOWS";

    public static File getPersistenceFile() throws IOException {

        final File file = File.createTempFile("chm-test-", "map");

        //Not Guaranteed to work on Windows, since OS file-lock takes precedence
        if (System.getProperty("os.name").indexOf(WIN_OS) > 0) {
            /*Windows will lock a file that are currently in use. You cannot delete it, however,
              using setwritable() and then releasing RandomRW lock adds the file to JVM exit cleanup.
    		  This will only work if the user is an admin on windows.
    		*/
            file.setWritable(true);//just in case relative path was used.
            RandomAccessFile raf = new RandomAccessFile(file, "rw");
            raf.close();//allows closing the file access on windows. forcing to close access. Only works for admin-access.
        }

        //file.delete(); //isnt guaranteed on windows.
        file.deleteOnExit();//isnt guaranteed on windows.

        return file;
    }

    public static void waitTillEqual(Map map1, Map map2, int timeOutMs) {
        int numberOfTimesTheSame = 0;
        long startTime = System.currentTimeMillis();
        for (int t = 0; t < timeOutMs + 100; t++) {
            // not map1.equals(map2), the reason is described above
            if (map1.equals(map2)) {
                numberOfTimesTheSame++;
                Jvm.pause(1);
                if (numberOfTimesTheSame == 10) {
                    System.out.println("same");
                    break;
                }
            }
            Jvm.pause(1);
            if (System.currentTimeMillis() - startTime > timeOutMs)
                break;
        }
    }

}
