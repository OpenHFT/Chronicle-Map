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

package net.openhft.chronicle.map.fromdocs.pingpong_latency;

import net.openhft.affinity.AffinitySupport;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import net.openhft.chronicle.map.fromdocs.BondVOInterface;

import java.io.IOException;
import java.util.Arrays;

import static net.openhft.chronicle.values.Values.newNativeReference;

/* on an i7-4500 laptop.

PingPongLEFT: Timing 1 x off-heap operations on /dev/chm/RDR_DIM_Mock
#0:  compareAndSwapCoupon() 50/90/99%tile was 41 / 52 / 132
#1:  compareAndSwapCoupon() 50/90/99%tile was 40 / 56 / 119
#2:  compareAndSwapCoupon() 50/90/99%tile was 39 / 48 / 54
#3:  compareAndSwapCoupon() 50/90/99%tile was 39 / 48 / 56
#4:  compareAndSwapCoupon() 50/90/99%tile was 39 / 48 / 54
#5:  compareAndSwapCoupon() 50/90/99%tile was 39 / 48 / 54
#6:  compareAndSwapCoupon() 50/90/99%tile was 39 / 48 / 55
#7:  compareAndSwapCoupon() 50/90/99%tile was 39 / 48 / 55
#8:  compareAndSwapCoupon() 50/90/99%tile was 39 / 48 / 54
#9:  compareAndSwapCoupon() 50/90/99%tile was 39 / 48 / 54

 */
public class PingPongCASLeft {
    public static void main(String... ignored) throws IOException {
        ChronicleMap<String, BondVOInterface> chm = PingPongCASLeft.acquireCHM();

        playPingPong(chm, 4, 5, true, "PingPongCASLEFT");
    }

    static void playPingPong(ChronicleMap<String, BondVOInterface> chm, double _coupon,
                             double _coupon2, boolean setFirst, final String desc) {
        BondVOInterface bond1 = newNativeReference(BondVOInterface.class);
        BondVOInterface bond2 = newNativeReference(BondVOInterface.class);
        BondVOInterface bond3 = newNativeReference(BondVOInterface.class);
        BondVOInterface bond4 = newNativeReference(BondVOInterface.class);

        chm.acquireUsing("369604101", bond1);
        chm.acquireUsing("369604102", bond2);
        chm.acquireUsing("369604103", bond3);
        chm.acquireUsing("369604104", bond4);
        System.out.printf("\n\n" + desc + ": Timing 1 x off-heap operations on /dev/chm/RDR_DIM_Mock\n");
        if (setFirst) {
            bond1.setCoupon(_coupon);
            bond2.setCoupon(_coupon);
            bond3.setCoupon(_coupon);
            bond4.setCoupon(_coupon);
        }
        int timeToCallNanoTime = 30;
        int runs = 1000000;
        long[] timings = new long[runs];
        for (int j = 0; j < 10; j++) {
            for (int i = 0; i < runs; i++) {
                long _start = System.nanoTime(); //
                while (!bond1.compareAndSwapCoupon(_coupon, _coupon2)) ;
                while (!bond2.compareAndSwapCoupon(_coupon, _coupon2)) ;
                while (!bond3.compareAndSwapCoupon(_coupon, _coupon2)) ;
                while (!bond4.compareAndSwapCoupon(_coupon, _coupon2)) ;

                timings[i] = (System.nanoTime() - _start - timeToCallNanoTime) / 4;
            }
            Arrays.sort(timings);
            System.out.printf("#%d:  compareAndSwapCoupon() 50/90/99%%tile was %,d / %,d / %,d%n",
                    j, timings[runs / 2], timings[runs * 9 / 10], timings[runs * 99 / 100]);
        }
    }

    static ChronicleMap<String, BondVOInterface> acquireCHM() throws IOException {
        // ensure thread ids are globally unique.
        AffinitySupport.setThreadId();
        return ChronicleMapBuilder.of(String.class, BondVOInterface.class)
                .entries(16)
                .averageKeySize("369604101".length()).create();
    }
}