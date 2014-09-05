/*
 * Copyright 2014 Higher Frequency Trading http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.map.fromdocs;

import net.openhft.chronicle.map.ChronicleMapBuilder;
import org.junit.Ignore;
import org.junit.Test;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Created by peter on 28/02/14.
 */
public class GettingStartedTest {
    @Test
    @Ignore
    public void testTheCodeInGuide() throws IOException {
        String tmpdir = System.getProperty("java.io.tmpdir");
        Map<String, String> map = ChronicleMapBuilder.of(String.class, String.class)

                .create(new File(tmpdir + "/shared.map"));
        map.put("some.json", "missing");
        map.get("some.json");

        map.put("Hello", "World");
        String hi = map.get("Hello");
        map.put("Long String", "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789");
        String json = "{\"menu\": {\n" +
                "  \"id\": \"file\",\n" +
                "  \"value\": \"File\",\n" +
                "  \"popup\": {\n" +
                "    \"menuitem\": [\n" +
                "      {\"value\": \"New\", \"onclick\": \"CreateNew()\"},\n" +
                "      {\"value\": \"Open\", \"onclick\": \"OpenDoc()\"},\n" +
                "      {\"value\": \"Close\", \"onclick\": \"CloseDoc()\"}\n" +
                "    ]\n" +
                "  }\n" +
                "}}";
        map.put("some.json", json);

        String s2 = map.get("some.json");
        assertEquals(json, s2);

        ((Closeable) map).close();

    }
}
