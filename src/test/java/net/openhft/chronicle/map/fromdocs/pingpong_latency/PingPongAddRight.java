/*
 * Copyright 2013-2026 chronicle.software; SPDX-License-Identifier: Apache-2.0
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
