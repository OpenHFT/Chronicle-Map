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

package net.openhft.chronicle.map.fromdocs;

import net.openhft.chronicle.map.ChronicleMap;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class CustomCharSequenceEncodingTest {

    @Test
    public void customCharSequenceEncodingTest() {
        Charset charset = StandardCharsets.UTF_8;
        int charBufferSize = 4;
        int bytesBufferSize = 8;
        CharSequenceCustomEncodingBytesWriter writer =
                new CharSequenceCustomEncodingBytesWriter(charset, charBufferSize);
        CharSequenceCustomEncodingBytesReader reader =
                new CharSequenceCustomEncodingBytesReader(charset, bytesBufferSize);
        try (ChronicleMap<String, CharSequence> map = ChronicleMap
                .of(String.class, CharSequence.class)
                .valueMarshallers(reader, writer)
                .averageKey("Russian")
                .averageValue("Всем нравится субботний вечерок")
                .entries(10)
                .create()) {
            map.put("Russian", "Всем нравится субботний вечерок");
            map.put("", "Quick brown fox jumps over the lazy dog");
        }
    }

    @Test
    public void gbkCharSequenceEncodingTest() {
        Charset charset = Charset.forName("GBK");
        int charBufferSize = 100;
        int bytesBufferSize = 200;
        CharSequenceCustomEncodingBytesWriter writer =
                new CharSequenceCustomEncodingBytesWriter(charset, charBufferSize);
        CharSequenceCustomEncodingBytesReader reader =
                new CharSequenceCustomEncodingBytesReader(charset, bytesBufferSize);
        try (ChronicleMap<String, CharSequence> englishToChinese = ChronicleMap
                .of(String.class, CharSequence.class)
                .valueMarshallers(reader, writer)
                .averageKey("hello")
                .averageValue("你好")
                .entries(10)
                .create()) {
            englishToChinese.put("hello", "你好");
            englishToChinese.put("bye", "再见");

            Assert.assertEquals("你好", englishToChinese.get("hello").toString());
            Assert.assertEquals("再见", englishToChinese.get("bye").toString());
        }
    }
}
