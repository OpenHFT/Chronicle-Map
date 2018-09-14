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

package net.openhft.chronicle.map.utility;

import net.openhft.affinity.AffinitySupport;
import net.openhft.chronicle.algo.locks.AcquisitionStrategies;
import net.openhft.chronicle.algo.locks.ReadWriteLockingStrategy;
import net.openhft.chronicle.algo.locks.TryAcquireOperations;
import net.openhft.chronicle.algo.locks.VanillaReadWriteWithWaitsLockingStrategy;
import net.openhft.chronicle.bytes.Byteable;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import net.openhft.chronicle.values.Array;
import net.openhft.chronicle.values.Group;
import net.openhft.chronicle.values.Values;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static net.openhft.chronicle.algo.bytes.Access.checkedBytesStoreAccess;

/**
 * ProcessInstanceLimiter limits the number of JVM processes of a particular
 * type that can be started on a particular machine. It does this be using a
 * shared map (using ChronicleMap) to maintain shared data across any processes
 * which use a ProcessInstanceLimiter, and checking on startup and regularly
 * after whether it is allowed to run.
 * <p/>
 * <p>Typically, you need to specify two things to create an instance of
 * ProcessInstanceLimiter: a path to a file that will hold the shared map; and a
 * callback object (an instance implementing ProcessInstanceLimiter.Callback) to
 * handle the various possible callback messages that the ProcessInstanceLimiter
 * can generate.
 * <p/>
 * <p>Once you have a ProcessInstanceLimiter instance, you specify a type of
 * process (any string) which will be limited to up to N processes running at
 * the same time by calling the setMaxNumberOfProcessesOfType() method. Finally
 * you tell the instance you are starting your process of type X by calling
 * startingProcessOfType(X). This last is deliberately not done automatically as
 * you may wish for one type of process to define limitations on other types of
 * processes.
 * <p/>
 * <p>The are some convenience methods which allow you to quickly specify a limit
 * without consideration of the above. For example, if during your application
 * startup you call ProcessInstanceLimiter.limitTo(2), then you need not call
 * anything else and you have limited your application to running at most 2 JVM
 * instances of your application. Under the covers, this call is identical to
 * the sequence:
 * <p/>
 * <p>ProcessInstanceLimiter limiter = new ProcessInstanceLimiter();
 * limiter.setMaxNumberOfProcessesOfType(processType,numProcesses);
 * limiter.startingProcessOfType(processType);
 * <p/>
 * <p>This:
 * 1. Creates a shared file called ProcessInstanceLimiter_DEFAULT_SHARED_MAP_ in
 * the temp directory to hold an instance of ChronicleMap
 * 2. Creates an instance of ProcessInstanceLimiter.DefaultCallback to handle
 * all callbacks in a reasonable way - all callbacks will emit a message on stdout
 * (using System.out) and those that indicate a conflict will exit the process
 * (using System.exit)
 * 3. Call setMaxNumberOfProcessesOfType("_DEFAULT_",2) to specify that at most only
 * 2 processes designated as type _DEFAULT_ will be allowed to run
 * 4. Calls startingProcessOfType("_DEFAULT_") to indicate that this process is an
 * instance of a _DEFAULT_ type process, and so should be limited appropriately
 */
public class ProcessInstanceLimiter implements Runnable {
    private static final long DEFAULT_TIME_UPDATE_INTERVAL_MS = 100L;
    private static final String DEFAULT_SHARED_MAP_NAME = "ProcessInstanceLimiter_DEFAULT_SHARED_MAP_";
    private static final String DEFAULT_SHARED_MAP_DIRECTORY = System.getProperty("java.io.tmpdir");
    private static final String DEFAULT_PROCESS_NAME = "_DEFAULT_";

    static {
        AffinitySupport.setThreadId();
    }

    private final String sharedMapPath;
    private final ChronicleMap<String, Data> theSharedMap;
    private final Callback callback;
    private final Map<String, Integer> localUpdates = new ConcurrentHashMap<String, Integer>();
    private final Map<String, String> processTypeToStartTimeType = new ConcurrentHashMap<String, String>();
    private long timeUpdateInterval = DEFAULT_TIME_UPDATE_INTERVAL_MS;
    private long startTime;
    private long[] lastStartTimes;
    private Map<String, Data> timedata = new ConcurrentHashMap<String, Data>();
    private Map<String, Data> starttimedata = new ConcurrentHashMap<String, Data>();

    /**
     * Create a ProcessInstanceLimiter instance with a default callback, an
     * instance of "DefaultCallback", and using the default shared file named
     * ProcessInstanceLimiter_DEFAULT_SHARED_MAP_ in the temp directory.
     *
     * @throws IOException - if the default shared file cannot be created
     */
    public ProcessInstanceLimiter() throws IOException {
        this(new DefaultCallback());
        ((DefaultCallback) this.getCallback()).setLimiter(this);
    }

    /**
     * Create a ProcessInstanceLimiter instance using the default shared file
     * named ProcessInstanceLimiter_DEFAULT_SHARED_MAP_ in the temp directory.
     *
     * @param callback - An instance of the Callback interface, which will receive
     *                 callbacks
     * @throws IOException - if the default shared file cannot be created
     */
    public ProcessInstanceLimiter(Callback callback) throws IOException {
        this(DEFAULT_SHARED_MAP_DIRECTORY + System.getProperty("file.separator") + DEFAULT_SHARED_MAP_NAME, callback);
    }

    /**
     * Create a ProcessInstanceLimiter instance using the default shared file
     * named ProcessInstanceLimiter_DEFAULT_SHARED_MAP_ in the tmp directory.
     *
     * @param sharedMapPath - The path to a file which will be used to store the shared
     *                      map (the file need not pre-exist)
     * @param callback      - An instance of the Callback interface, which will receive
     *                      callbacks
     * @throws IOException - if the default shared file cannot be created
     */
    public ProcessInstanceLimiter(String sharedMapPath, Callback callback) throws IOException {
        this.sharedMapPath = sharedMapPath;
        this.callback = callback;
        ChronicleMapBuilder<String, Data> builder =
                ChronicleMapBuilder.of(String.class, Data.class);
        builder.entries(1000);
        builder.averageKeySize((DEFAULT_PROCESS_NAME + "#").length());
        this.theSharedMap = builder.createPersistedTo(new File(sharedMapPath));
        Thread t = new Thread(this, "ProcessInstanceLimiter updater");
        t.setDaemon(true);
        t.start();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        ProcessInstanceLimiter.limitTo(2);
        Jvm.pause(60L * 1000L);
    }

    /**
     * Convenience method.
     * <p/>
     * <p>Create a ProcessInstanceLimiter instance which is limited to one OS
     * process instance of the DEFAULT type. This will enforce that any JVM on
     * the same box which runs the code
     * "ProcessInstanceLimiter.limitToOneProcess()" will only have at most one
     * JVM instance running at a time.
     *
     * @return - the ProcessInstanceLimiter instance
     * @throws IOException - if the default shared file cannot be created
     */
    public static ProcessInstanceLimiter limitToOneProcess() throws IOException {
        return limitTo(1);
    }

    /**
     * Convenience method.
     * <p/>
     * <p>Create a ProcessInstanceLimiter instance which is limited to
     * "numProcesses" OS process instances of the DEFAULT type. This will
     * enforce that any JVM on the same box which runs the code
     * "ProcessInstanceLimiter.limitTo(numProcesses)" will only have at most
     * numProcesses JVM instances running at a time. All the JVMs must use the
     * same "numProcesses" value or the process will immediately exit with a
     * configuration error.
     *
     * @param numProcesses - the number of JVM processes that can run at any one time
     * @return - the ProcessInstanceLimiter instance
     * @throws IOException - if the default shared file cannot be created
     */
    public static ProcessInstanceLimiter limitTo(int numProcesses) throws IOException {
        return limitTo(numProcesses, DEFAULT_PROCESS_NAME);
    }

    /**
     * Convenience method.
     * <p/>
     * <p>Create a ProcessInstanceLimiter instance which is limited to
     * "numProcesses" OS process instances of the "processType" type. This will
     * enforce that any JVM on the same box which runs the code
     * "ProcessInstanceLimiter.limitTo(numProcesses, processType)" will only
     * have at most numProcesses JVM instances running at a time. All the JVMs
     * must use the same "numProcesses" value for a "processType" or the process
     * will immediately exit with a configuration error.
     *
     * @param numProcesses - the number of JVM processes that can run at any one time
     * @param processType  - any string, specifies the type of process that is limited to
     *                     numProcesses processes
     * @return - the ProcessInstanceLimiter instance
     * @throws IOException - if the default shared file cannot be created
     */
    public static ProcessInstanceLimiter limitTo(int numProcesses, String processType) throws IOException {
        ProcessInstanceLimiter limiter = new ProcessInstanceLimiter();
        limiter.setMaxNumberOfProcessesOfType(processType, numProcesses);
        limiter.startingProcessOfType(processType);
        return limiter;
    }

    /**
     * Sleeps the thread for the specified number of milliseconds, ignoring
     * interruptions.
     *
     * @param pause - time in milliseconds to sleep
     */
    public static void pause(long pause) {
        long start = System.currentTimeMillis();
        long elapsedTime;
        while ((elapsedTime = System.currentTimeMillis() - start) < pause) {
            Thread.interrupted();// clear interruptions.
            Jvm.pause(pause - elapsedTime);
        }
    }

    /**
     * The path to the shared file which stored the shared map.
     */
    public String getSharedMapPath() {
        return sharedMapPath;
    }

    /**
     * The instance of the Callback interface held by the instance, which will
     * receive callbacks
     */
    public Callback getCallback() {
        return this.callback;
    }

    /**
     * Returns the MaxNumberOfProcesses allowed for processes of type
     * "processType", as specified in the shared map. If that type hasn't been
     * set, then this returns -1 (which is an invalid value, as it must be a
     * positive value)
     */
    public int getMaxNumberOfProcessesAllowedFor(String processType) {
        Data data = this.starttimedata.get(processType);
        if (data == null) {
            return -1;
        } else {
            return data.getMaxNumberOfProcessesAllowed();
        }
    }

    /**
     * The interval between updates to the shared map timestamps - i.e. this is
     * the interval between notifications of other processes starting
     */
    public long getTimeUpdateInterval() {
        return timeUpdateInterval;
    }

    /**
     * Set the interval between updates to the shared map timestamps - i.e. this
     * is the interval between notifications of other processes starting
     */
    public void setTimeUpdateInterval(long timeUpdateInterval) {
        this.timeUpdateInterval = timeUpdateInterval;
    }

    /**
     * run() method for the ProcessInstanceLimiter which it starts in a thread
     * called "ProcessInstanceLimiter updater"
     */
    public void run() {
        //every timeUpdateInterval milliseconds, update the time
        while (true) {
            try {
                pause(timeUpdateInterval);
                String processType;
                Set<Entry<String, Integer>> entrySet = this.localUpdates.entrySet();
                for (Entry<String, Integer> entry : entrySet) {
                    processType = entry.getKey();
                    int index = entry.getValue().intValue();
                    Data data = this.timedata.get(processType);
                    if (data == null) {
                        entrySet.remove(entry);
                    } else {
                        if (!lock(data, 100000)) {
                            entrySet.remove(entry);
                            this.callback.lockConflictDetected(processType, index);
                        } else {
                            try {
                                if (!updateTheSharedMap(processType, index, data)) {
                                    entrySet.remove(entry);
                                }
                            } finally {
                                //and release the lock
                                unlock(data);
                            }
                        }
                    }
                }
            } catch (Exception e) {
                // TODO
                e.printStackTrace();
            }
        }
    }

    /**
     * Call this near the start of the process - if the process can acquire a
     * slot, it will callback thisProcessOfTypeHasStartedAtSlot(), otherwise one
     * of the other callback interface methods will be called.
     *
     * @param processType
     */
    public void startingProcessOfType(String processType) {
        Data data = this.timedata.get(processType);
        if (data == null) {
            this.callback.noDefinitionForProcessesOfType(processType);
            this.callback.tooManyProcessesOfType(processType);
            return;
        }
        // We need to lock access to the Time array, try up to 1 second
        long[] times1 = new long[data.getMaxNumberOfProcessesAllowed()];
        if (!lock(data, 1000000)) {
            this.callback.tooManyProcessesOfType(processType);
            return;
        }
        //try {Jvm.pause(60L*1000L);} catch (InterruptedException e) {}
        //we've got the lock, now copy the array
        try {
            for (int i = 0; i < times1.length; i++) {
                times1[i] = data.getTimeAt(i);
            }
        } finally {
            //and release the lock
            unlock(data);
        }
        pause(3L * timeUpdateInterval);
        if (!lock(data, 1000000)) {
            this.callback.tooManyProcessesOfType(processType);
            return;
        }
        boolean alreadyUnlocked = false;
        try {
            for (int i = 0; i < times1.length; i++) {
                if (data.getTimeAt(i) == times1[i]) {
                    //we have an index which has not been updated in 3x the
                    //time interval, so we have a spare slot - use this slot
                    this.startTime = System.currentTimeMillis();
                    this.starttimedata.get(processType).setTimeAt(i, this.startTime);
                    if (updateTheSharedMap(processType, i, data)) {
                        this.localUpdates.put(processType, new Integer(i));
                        unlock(data);
                        alreadyUnlocked = true;
                        this.callback.thisProcessOfTypeHasStartedAtSlot(processType, i);
                        return;
                    }
                }
            }
        } finally {
            //and release the lock
            if (!alreadyUnlocked) {
                unlock(data);
            }
        }
        this.callback.tooManyProcessesOfType(processType);
    }

    /**
     * Set the maximum number of processes of type processType that can run
     * concurrently on the same machine
     *
     * @param processType                 - any string, specifies the type of process that is limited to
     *                                    maxNumberOfProcessesAllowed processes
     * @param maxNumberOfProcessesAllowed - any positive number, specifies the maximum number of
     *                                    processes of this type that can run concurrently on the same
     *                                    machine
     */
    public void setMaxNumberOfProcessesOfType(String processType, int maxNumberOfProcessesAllowed) {
        if (maxNumberOfProcessesAllowed <= 0) {
            throw new IllegalArgumentException("maxNumberOfProcessesAllowed must be a positive number, not " + maxNumberOfProcessesAllowed);
        }
        Data data = Values.newNativeReference(Data.class);
        this.timedata.put(processType, data);
        this.theSharedMap.acquireUsing(processType, data);
        if (data.getMaxNumberOfProcessesAllowed() != maxNumberOfProcessesAllowed) {
            //it's either a new object, set to 0, or
            //another process set it to an invalid value
            if (data.compareAndSwapMaxNumberOfProcessesAllowed(0, maxNumberOfProcessesAllowed)) {
                //What we expected, everything's good
            } else {
                //something else set a value, if it's not 2 we've got a conflict
                if (data.getMaxNumberOfProcessesAllowed() != maxNumberOfProcessesAllowed) {
                    throw new IllegalArgumentException("The existing shared map already specifies that the maximum number of processes allowed is " + data.getMaxNumberOfProcessesAllowed() + " and changing that to " + maxNumberOfProcessesAllowed + " is not supported");
                }
            }
        }
        String name = processType + '#';
        data = Values.newNativeReference(Data.class);
        this.starttimedata.put(processType, data);
        this.processTypeToStartTimeType.put(processType, name);
        this.theSharedMap.acquireUsing(name, data);
        //this time just set it, we've done the guarding with the other value
        if (data.getMaxNumberOfProcessesAllowed() == 0) {
            data.setMaxNumberOfProcessesAllowed(maxNumberOfProcessesAllowed);
        }
    }

    /**
     * Assumes that the data object is non-null and already locked
     * If true is returned, the update has been applied, otherwise
     * this slot is conflicted
     */
    private boolean updateTheSharedMap(String processType, int index, Data data) {
        long timenow = System.currentTimeMillis();
        data.setTimeAt(index, timenow);
        Data startTimesData = this.starttimedata.get(processType);
        if (this.startTime != startTimesData.getTimeAt(index)) {
            //something else is updating this index, so we assume we're
            //conflicted and give up - with a callback
            this.callback.anotherProcessHasHijackedThisSlot(processType, index);
            return false;
        }
        if (this.lastStartTimes != null) {
            for (int i = 0; i < this.lastStartTimes.length; i++) {
                if ((i != index) && (this.lastStartTimes[i] != startTimesData.getTimeAt(i))) {
                    this.callback.anotherProcessHasStartedOnSlot(processType, i, startTimesData.getTimeAt(i));
                }
            }
        } else {
            this.lastStartTimes = new long[startTimesData.getMaxNumberOfProcessesAllowed()];
        }
        for (int i = 0; i < this.lastStartTimes.length; i++) {
            this.lastStartTimes[i] = startTimesData.getTimeAt(i);
        }
        return true;
    }

    private boolean lock(Data data, int microsecondsToTry) {
        return AcquisitionStrategies
                .<ReadWriteLockingStrategy>spinLoop(microsecondsToTry, TimeUnit.MICROSECONDS)
                .acquire(TryAcquireOperations.writeLock(),
                        VanillaReadWriteWithWaitsLockingStrategy.instance(),
                        checkedBytesStoreAccess(),
                        ((Byteable) data).bytesStore(),
                        ((Byteable) data).offset());
    }

    private void unlock(Data data) {
        try {
            VanillaReadWriteWithWaitsLockingStrategy.instance()
                    .writeUnlock(checkedBytesStoreAccess(),
                            ((Byteable) data).bytesStore(), ((Byteable) data).offset());
        } catch (IllegalMonitorStateException e) {
            //odd, but we'll be unlocked either way
            System.out.println("Unexpected state: " + e);
            e.printStackTrace();
        }
    }

    /**
     * The Callback interface holds all the calls that can be made by the
     * process instance limiter.
     */
    public interface Callback {
        /**
         * Called when there are already the specified number of processes of
         * the given type running, and this process is one too many.
         *
         * @param processType - the name of the type of process being limited
         */
        void tooManyProcessesOfType(String processType);

        /**
         * Called when there is a lock conflict in the limiter
         * which probably means the process must exit
         *
         * @param processType - the name of the type of process being limited
         * @param index       - the slot number held by the other process
         */
        void lockConflictDetected(String processType, int index);

        /**
         * Called when another process has started and successfully acquired a
         * slot that allows it to continue running.
         *
         * @param processType - the name of the type of process being limited
         * @param slot        - the slot number held by the other process
         * @param startTime   - the start timestamp of the other process
         */
        void anotherProcessHasStartedOnSlot(String processType, int slot, long startTime);

        /**
         * Called when this process has started and successfully acquired a slot
         * that allows it to continue running.
         *
         * @param processType - the name of the type of process being limited
         * @param slot        - the slot number held by this process
         */
        void thisProcessOfTypeHasStartedAtSlot(String processType, int slot);

        /**
         * Called if the process was started but there was no data defined in
         * the shared map that would limit processes of this type.
         *
         * @param processType - the name of the type of process being limited
         */
        void noDefinitionForProcessesOfType(String processType);

        /**
         * Called when another process somehow managed to steal the slot that
         * this process had acquired.
         *
         * @param processType - the name of the type of process being limited
         * @param slot        - the slot number held by this process
         */
        void anotherProcessHasHijackedThisSlot(String processType, int slot);
    }

    /**
     * The Data object holds an array of timestamps and a maximum number of
     * processes allowed to be running concurrently
     * <p/>
     * <p>The Timelock field is just for locking the time field
     */
    public interface Data {

        /**
         * Ensure lock goes first in flyweight layout, to apply external locking
         */
        @Group(0)
        long getTimeLock();

        void setTimeLock(long timeLock);

        @Group(1)
        @Array(length = 50)
        void setTimeAt(int index, long time);

        long getTimeAt(int index);

        @Group(1)
        int getMaxNumberOfProcessesAllowed();

        void setMaxNumberOfProcessesAllowed(int num);

        boolean compareAndSwapMaxNumberOfProcessesAllowed(int expected, int value);
    }

    /**
     * A default implementation of the Callback interface, which prints an
     * information line to System.out for each callback, and calls
     * System.exit(0) for those methods which don't leave the current process
     * owning a slot.
     */
    public static class DefaultCallback implements Callback {
        ProcessInstanceLimiter limiter;

        public DefaultCallback(ProcessInstanceLimiter limiter) {
            this.limiter = limiter;
        }

        public DefaultCallback() {
        }

        public ProcessInstanceLimiter getLimiter() {
            return limiter;
        }

        public void setLimiter(ProcessInstanceLimiter limiter) {
            this.limiter = limiter;
        }

        public void tooManyProcessesOfType(String processType) {
            System.out.println("Sufficient processes (" + this.limiter.getMaxNumberOfProcessesAllowedFor(processType) + ") of type " + processType + " have already been started, so exiting this process");
            System.exit(0);
        }

        public void noDefinitionForProcessesOfType(String processType) {
            System.out.println("No definition for processes of type " + processType + " has been set, so exiting this process");
            System.exit(0);
        }

        public void anotherProcessHasHijackedThisSlot(String processType, int slot) {
            System.out.println("Another process of type " + processType + " has hijacked the slot (" + slot + "/" + this.limiter.getMaxNumberOfProcessesAllowedFor(processType) + ") allocated to this process, so exiting this process");
            System.exit(0);
        }

        public void thisProcessOfTypeHasStartedAtSlot(String processType, int slot) {
            System.out.println("This process of type " + processType + " has started at slot " + slot + "/" + this.limiter.getMaxNumberOfProcessesAllowedFor(processType));
        }

        public void anotherProcessHasStartedOnSlot(String processType, int slot, long startTime) {
            System.out.println("Another process of type " + processType + " has started at slot " + slot + "/" + this.limiter.getMaxNumberOfProcessesAllowedFor(processType) + " at time " + new Date(startTime));
        }

        public void lockConflictDetected(String processType, int slot) {
            System.out.println("The limiter lock has become conflicted for type " + processType + " on slot (" + slot + "/" + this.limiter.getMaxNumberOfProcessesAllowedFor(processType) + ") allocated to this process, so exiting this process");
            System.exit(0);
        }
    }

}
