/*
 * Copyright 2014 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
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

package net.openhft.chronicle.map.fromdocs.pingpong_latency;

import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.fromdocs.BondVOInterface;

import java.io.IOException;

import static net.openhft.chronicle.map.fromdocs.pingpong_latency.PingPongAddLeft.playPingPong;

public class PingPongAddRight {
    public static void main(String... ignored) throws IOException {
        ChronicleMap<String, BondVOInterface> chm = PingPongCASLeft.acquireCHM();

        playPingPong(chm, -1, false, "PingPongAddRIGHT");
    }
}