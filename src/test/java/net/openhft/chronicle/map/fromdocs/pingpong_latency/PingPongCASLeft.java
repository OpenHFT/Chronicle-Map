/*
 *      Copyright (C) 2015  higherfrequencytrading.com
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

package net.openhft.chronicle.map.fromdocs.pingpong_latency;

import net.openhft.affinity.AffinitySupport;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import net.openhft.chronicle.map.fromdocs.BondVOInterface;

import java.io.IOException;
import java.util.Arrays;

import static net.openhft.lang.model.DataValueClasses.newDirectReference;

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
        BondVOInterface bond1 = newDirectReference(BondVOInterface.class);
        BondVOInterface bond2 = newDirectReference(BondVOInterface.class);
        BondVOInterface bond3 = newDirectReference(BondVOInterface.class);
        BondVOInterface bond4 = newDirectReference(BondVOInterface.class);

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