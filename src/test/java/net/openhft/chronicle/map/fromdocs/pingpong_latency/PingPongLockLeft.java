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

import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.fromdocs.BondVOInterface;

import java.io.IOException;
import java.util.Arrays;

import static net.openhft.lang.model.DataValueClasses.newDirectReference;

public class PingPongLockLeft {
    public static void main(String... ignored) throws IOException, InterruptedException {
        ChronicleMap<String, BondVOInterface> chm = PingPongCASLeft.acquireCHM();

        playPingPong(chm, 4, 5, true, "PingPongLockLEFT");
    }

    static void playPingPong(ChronicleMap<String, BondVOInterface> chm, double _coupon, double _coupon2, boolean setFirst, final String desc) throws InterruptedException {
        BondVOInterface bond1 = newDirectReference(BondVOInterface.class);
        BondVOInterface bond2 = newDirectReference(BondVOInterface.class);
        BondVOInterface bond3 = newDirectReference(BondVOInterface.class);
        BondVOInterface bond4 = newDirectReference(BondVOInterface.class);

        chm.acquireUsing("369604101", bond1);
        chm.acquireUsing("369604102", bond2);
        chm.acquireUsing("369604103", bond3);
        chm.acquireUsing("369604104", bond4);
        System.out.printf("\n\n" + desc + ": Timing 1 x off-heap operations on " + chm.file() + "\n");
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
                long _start = System.nanoTime();
                toggleCoupon(bond1, _coupon, _coupon2);
                toggleCoupon(bond2, _coupon, _coupon2);
                toggleCoupon(bond3, _coupon, _coupon2);
                toggleCoupon(bond4, _coupon, _coupon2);

                timings[i] = (System.nanoTime() - _start - timeToCallNanoTime) / 4;
            }
            Arrays.sort(timings);
            System.out.printf("#%d:  lock,compare,set,unlock 50/90/99%%tile was %,d / %,d / %,d%n",
                    j, timings[runs / 2], timings[runs * 9 / 10], timings[runs * 99 / 100]);
        }
    }

    private static void toggleCoupon(BondVOInterface bond, double _coupon, double _coupon2) throws InterruptedException {
        for (int i = 0; ; i++) {
            bond.busyLockEntry();
            try {
                if (bond.getCoupon() == _coupon) {
                    bond.setCoupon(_coupon2);
                    return;
                }
                if (i > 1000)
                    Thread.yield();
            } finally {
                bond.unlockEntry();
            }
        }
    }
}