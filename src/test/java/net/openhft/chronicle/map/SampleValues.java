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
