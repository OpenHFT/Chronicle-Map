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
