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

package net.openhft.chronicle.map;

import net.openhft.chronicle.algo.locks.AcquisitionStrategies;
import net.openhft.chronicle.algo.locks.ReadWriteLockingStrategy;
import net.openhft.chronicle.algo.locks.TryAcquireOperations;
import net.openhft.chronicle.algo.locks.VanillaReadWriteWithWaitsLockingStrategy;
import net.openhft.chronicle.bytes.Byteable;
import net.openhft.chronicle.map.utility.ProcessInstanceLimiter;
import net.openhft.chronicle.values.Array;
import net.openhft.chronicle.values.Group;
import net.openhft.chronicle.values.Values;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static net.openhft.chronicle.algo.bytes.Access.checkedBytesStoreAccess;
import static org.junit.Assert.assertEquals;

/**
 * @author jack shirazi
 */
public class CHMTest5 {
    public static final String TEST_KEY = "whatever";
    public static int NUMBER_OF_PROCESSES_ALLOWED = 2;

    public static void main(String[] args) throws IOException {
        //First create (or access if already created) the shared map
        ChronicleMapBuilder<String, CHMTest5Data> builder =
                ChronicleMapBuilder.of(String.class, CHMTest5Data.class)
                        .entries(1000);

        //// don't include this, just to check it is as expected.
        assertEquals(8, builder.minSegments());
        //// end of test

        String chmPath = System.getProperty("java.io.tmpdir") + System.getProperty("file.separator") + "CHMTest5";

        ChronicleMap<String, CHMTest5Data> theSharedMap =
                builder.createPersistedTo(new File(chmPath));

        //Now get the shared data object, auto-creating if it's not there
        CHMTest5Data data = Values.newNativeReference(CHMTest5Data.class);
        theSharedMap.acquireUsing(TEST_KEY, data);
        //if this was newly created, we need to set the max allowed
        //Note, the object is pointing at shared memory and writes
        //directly to it, so no need to put() it back into the map
        if (data.getMaxNumberOfProcessesAllowed() != NUMBER_OF_PROCESSES_ALLOWED) {
            //it's either a new object, set to 0, or
            //another process set it to an invalid value
            if (data.compareAndSwapMaxNumberOfProcessesAllowed(0, NUMBER_OF_PROCESSES_ALLOWED)) {
                //What we expected, everything's good
            } else {
                //something else set a value, if it's not 2 we've got a conflict
                if (data.getMaxNumberOfProcessesAllowed() != NUMBER_OF_PROCESSES_ALLOWED) {
                    System.out.println("Incorrect configuration found, expected " +
                            NUMBER_OF_PROCESSES_ALLOWED + " slots, instead found " +
                            data.getMaxNumberOfProcessesAllowed() + " - exiting");
                    System.exit(0);
                }
            }
        }

        //Now, we look for an empty slot in the "time" array.
        //Because this initial implementation of the data object
        //is directly accessing the shared memory, we just operate
        //on it as a shared object

        // There would be no need to lock the MaxNumberOfProcessesAllowed field
        // as we used CAS to update and it's read-only after that
        // but we need to lock access to the Time array
        long[] times1 = new long[NUMBER_OF_PROCESSES_ALLOWED];
        getTimes(data, times1);
        pause(300L);
        long[] times2 = new long[NUMBER_OF_PROCESSES_ALLOWED];
        getTimes(data, times2);
        //look for a slot that hasn't changed in that 300ms pause
        int slotindex = 0;
        long lastUpdateTime = -1;
        for (; slotindex < times1.length; slotindex++) {
            if (times2[slotindex] == times1[slotindex]) {
                //we have an index which has not been updated by anything else
                //in the 300ms pause, so we have a spare slot - we use this slot
                long timenow = System.currentTimeMillis();
                tryLock1Sec(data);
                try {
                    data.setTimeAt(slotindex, timenow);
                } finally {
                    //and release the lock
                    releaseLock(data);
                }

                //Now we have successfully acquired a slot
                lastUpdateTime = timenow;
                System.out.println("We have started on slot " + slotindex);
                break;
            }
        }

        if (lastUpdateTime == -1) {
            System.out.println("We failed to find a free slot, so terminating now");
            System.exit(0);
        }

        //Now let's run for 60 seconds, updating the slot every 100ms
        for (int count = 0; count < 600; count++) {
            pause(100L);
            long timenow = System.currentTimeMillis();
            tryLock1Sec(data);
            try {
                if (lastUpdateTime == data.getTimeAt(slotindex)) {
                    //That's what we expect so just update the slot
                    data.setTimeAt(slotindex, timenow);
                    lastUpdateTime = timenow;
                } else {
                    //Some other process has hijacked our slot - so
                    //the only thing we can do is terminate
                    System.out.println("Another process has hijacked our slot " + slotindex + ", so terminating now");
                    System.exit(0);
                }
            } finally {
                releaseLock(data);
            }
        }
        System.out.println("Exiting slot " + slotindex + " after completing the full test.");
    }

    private static void releaseLock(CHMTest5Data data) {
        //and release the lock
        try {
            data.unlockEntry();
        } catch (IllegalMonitorStateException e) {
            //odd, but we'll be unlocked either way
            System.out.println("Unexpected state: " + e);
            e.printStackTrace();
        }
    }

    private static void tryLock1Sec(CHMTest5Data data) {
        boolean locked = false;
        for (int i = 0; i < 1000000; i++) {
            //try up to 1 second
            if (data.tryLockNanosEntry(1000L)) {
                locked = true;
                break;
            }
        }
        if (!locked) {
            System.out.println("Unable to acquire a lock on the time array - exiting");
            System.exit(0);
        }
    }

    private static void getTimes(CHMTest5Data data, long[] times1) {
        tryLock1Sec(data);
        try {
            //we've got the lock, now copy the array
            for (int i = 0; i < times1.length; i++) {
                times1[i] = data.getTimeAt(i);
            }
        } finally {
            //and release the lock
            releaseLock(data);
        }
    }

    public static void pause(long pause) {
        ProcessInstanceLimiter.pause(pause);
    }

    public interface CHMTest5Data {

        @Group(0)
        long getEntryLockState();

        void setEntryLockState(long entryLockState);

        @Group(1)
        int getMaxNumberOfProcessesAllowed();

        void setMaxNumberOfProcessesAllowed(int max);

        boolean compareAndSwapMaxNumberOfProcessesAllowed(int expected, int value);

        @Group(1)
        @Array(length = 4)
        void setTimeAt(int index, long time);

        long getTimeAt(int index);

        @Deprecated()
        default boolean tryLockNanosEntry(long nanos) {
            return AcquisitionStrategies
                    .<ReadWriteLockingStrategy>spinLoop(nanos, TimeUnit.NANOSECONDS).acquire(
                            TryAcquireOperations.writeLock(),
                            VanillaReadWriteWithWaitsLockingStrategy.instance(),
                            checkedBytesStoreAccess(), ((Byteable) this).bytesStore(),
                            ((Byteable) this).offset());
        }

        @Deprecated()
        default void unlockEntry() throws IllegalMonitorStateException {
            VanillaReadWriteWithWaitsLockingStrategy.instance()
                    .writeUnlock(checkedBytesStoreAccess(),
                            ((Byteable) this).bytesStore(), ((Byteable) this).offset());
        }
    }
}