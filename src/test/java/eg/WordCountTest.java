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

package eg;

import com.google.common.io.ByteStreams;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.values.IntValue;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.values.Values;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.zip.GZIPInputStream;

import static java.nio.charset.StandardCharsets.UTF_8;

public class WordCountTest {

    static String[] words;
    static int expectedSize;

    static {
        try {
            // english version of war and peace ->  ascii
            ClassLoader cl = Thread.currentThread().getContextClassLoader();
            InputStream zippedIS = cl.getResourceAsStream("war_and_peace.txt.gz");
            GZIPInputStream binaryIS = new GZIPInputStream(zippedIS);
            String fullText =
                    new String(ByteStreams.toByteArray(binaryIS), UTF_8);
            words = fullText.split("\\s+");
            expectedSize = (int) Arrays.stream(words).distinct().count();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void wordCountTest() throws IOException {
        try (ChronicleMap<CharSequence, IntValue> map = ChronicleMap
                .of(CharSequence.class, IntValue.class)
                .averageKeySize(7) // average word is 7 ascii bytes long (text in english)
                .entries(expectedSize)
                .create()) {
            IntValue v = Values.newNativeReference(IntValue.class);
            for (String word : words) {
                try (Closeable ignored = map.acquireContext(word, v)) {
                    v.addValue(1);
                }
            }
        }
    }
}

