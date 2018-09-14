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

package net.openhft.chronicle.map;/*
 * Copyright 2013 peter.lawrey
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesMarshallable;
import net.openhft.chronicle.bytes.BytesOut;
import org.jetbrains.annotations.NotNull;

enum BuySell {
    Buy, Sell
}

/**
 * Sample entry of 10 fields with two String, Enum, int, double, long
 */
public class SampleValues implements BytesMarshallable {
    String aa = "aaaaaaaaaa";
    String bb = "bbbbbbbbbb";
    BuySell cc = BuySell.Buy;
    BuySell dd = BuySell.Sell;
    int ee = 123456;
    int ff = 654321;
    double gg = 1.23456789;
    double hh = 9.87654321;
    long ii = 987654321;
    long jj = 123456789;

    @Override
    public void readMarshallable(@NotNull BytesIn in) throws IllegalStateException {
        aa = in.readUtf8();
        bb = in.readUtf8();
        cc = (BuySell) in.readEnum(BuySell.class);
        dd = (BuySell) in.readEnum(BuySell.class);
        ee = in.readInt();
        ff = in.readInt();
        gg = in.readDouble();
        hh = in.readDouble();
        ii = in.readLong();
        jj = in.readLong();
    }

    @Override
    public void writeMarshallable(@NotNull BytesOut out) {
        out.writeUtf8(aa);
        out.writeUtf8(bb);
        out.writeEnum(cc);
        out.writeEnum(dd);
        out.writeInt(ee);
        out.writeInt(ff);
        out.writeDouble(gg);
        out.writeDouble(hh);
        out.writeLong(ii);
        out.writeLong(jj);
    }
}
