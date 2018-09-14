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

import net.openhft.chronicle.algo.locks.AcquisitionStrategies;
import net.openhft.chronicle.algo.locks.ReadWriteLockingStrategy;
import net.openhft.chronicle.algo.locks.TryAcquireOperations;
import net.openhft.chronicle.algo.locks.VanillaReadWriteWithWaitsLockingStrategy;
import net.openhft.chronicle.bytes.Byteable;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.fromdocs.BondVOInterface;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static net.openhft.chronicle.algo.bytes.Access.checkedBytesStoreAccess;
import static net.openhft.chronicle.values.Values.newNativeReference;

public class PingPongLockLeft {
    public static void main(String... ignored) throws IOException, InterruptedException {
        ChronicleMap<String, BondVOInterface> chm = PingPongCASLeft.acquireCHM();

        playPingPong(chm, 4, 5, true, "PingPongLockLEFT");
    }

    static void playPingPong(ChronicleMap<String, BondVOInterface> chm, double _coupon, double _coupon2, boolean setFirst, final String desc) throws InterruptedException {
        BondVOInterface bond1 = newNativeReference(BondVOInterface.class);
        BondVOInterface bond2 = newNativeReference(BondVOInterface.class);
        BondVOInterface bond3 = newNativeReference(BondVOInterface.class);
        BondVOInterface bond4 = newNativeReference(BondVOInterface.class);

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

    private static void toggleCoupon(BondVOInterface bond, double _coupon, double _coupon2)
            throws InterruptedException {
        for (int i = 0; ; i++) {
            AcquisitionStrategies.<ReadWriteLockingStrategy>spinLoopOrFail(5, TimeUnit.SECONDS)
                    .acquire(TryAcquireOperations.writeLock(),
                            VanillaReadWriteWithWaitsLockingStrategy.instance(),
                            checkedBytesStoreAccess(),
                            ((Byteable) bond).bytesStore(), ((Byteable) bond).offset());
            try {
                if (bond.getCoupon() == _coupon) {
                    bond.setCoupon(_coupon2);
                    return;
                }
                if (i > 1000)
                    Thread.yield();
            } finally {
                VanillaReadWriteWithWaitsLockingStrategy.instance()
                        .writeUnlock(checkedBytesStoreAccess(),
                                ((Byteable) bond).bytesStore(), ((Byteable) bond).offset());
            }
        }
    }
}