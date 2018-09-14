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

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.map.utility.ProcessInstanceLimiter;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

// todo make this example more efficient e.g. not using Serializable.
public class ProcessInstanceLimiterMain implements Runnable {
    private static final long TIME_UPDATE_INTERVAL = 100L;
    private final String sharedMapName;
    private final Map<String, Data> theSharedMap;
    private final Callback callback;
    private final Map<String, IntWrapper> localUpdates = new ConcurrentHashMap<String, IntWrapper>();

    public ProcessInstanceLimiterMain(String sharedMapName, Callback callback) throws IOException {
        this.sharedMapName = sharedMapName;
        this.callback = callback;
        ChronicleMapBuilder<String, Data> builder =
                ChronicleMapBuilder.of(String.class, Data.class);
        builder.entries(10000);
        builder.minSegments(2);
        File file = new File(System.getProperty("java.io.tmpdir") + "/" + sharedMapName);
        this.theSharedMap = builder.create();
        Thread t = new Thread(this, "ProcessInstanceLimiterMain updater");
        t.setDaemon(true);
        t.start();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        Callback callback = new Callback() {
            public void tooManyProcessesOfType(String processType) {
                System.out.println("Too many processes of type " + processType + " have been started, so exiting this process");
                System.exit(0);
            }

            public void noDefinitionForProcessesOfType(String processType) {
                System.out.println("No definition for processes of type " + processType + " has been set, so exiting this process");
                System.exit(0);
            }
        };
        ProcessInstanceLimiterMain limiter = new ProcessInstanceLimiterMain("test", callback);
        limiter.setMaxNumberOfProcessesOfType("x", 2);
        limiter.startingProcessOfType("x");
        Jvm.pause(60L * 1000L);
    }

    public static void pause(long pause) {
        ProcessInstanceLimiter.pause(pause);
    }

    public void run() {
        //every TIME_UPDATE_INTERVAL milliseconds, update the time
        while (true) {
            try {
                pause(TIME_UPDATE_INTERVAL);
                String processType;
                for (Entry<String, IntWrapper> entry : this.localUpdates.entrySet()) {
                    processType = entry.getKey();
                    int index = entry.getValue().intValue;
                    Data data = this.theSharedMap.get(processType);
                    if (data != null) {
                        long timenow = System.currentTimeMillis();
                        data.time[index] = timenow;
                        this.theSharedMap.put(processType, data);
                        while ((data = this.theSharedMap.get(processType)).time[index] != timenow) {
                            data.time[index] = timenow;
                            this.theSharedMap.put(processType, data);
                        }
                    }
                }
            } catch (Exception e) {
                // TODO
                e.printStackTrace();
            }
        }
    }

    public void startingProcessOfType(String processType) {
        Data data = this.theSharedMap.get(processType);
        if (data == null) {
            this.callback.noDefinitionForProcessesOfType(processType);
            this.callback.tooManyProcessesOfType(processType);
            return;
        }
        long[] times1 = data.time;
        pause(2L * TIME_UPDATE_INTERVAL);
        long[] times2 = this.theSharedMap.get(processType).time;
        for (int i = 0; i < times1.length; i++) {
            if (times2[i] == times1[i]) {
                //we have an index which has not been updated in 200ms
                //so we have a spare slot - use this slot
                this.localUpdates.put(processType, new IntWrapper(i));
                return;
            }
        }
        this.callback.tooManyProcessesOfType(processType);
    }

    public void setMaxNumberOfProcessesOfType(String processType, int maxNumberOfProcessesAllowed) {
        this.theSharedMap.put(processType, new Data(processType, maxNumberOfProcessesAllowed));
    }

    public interface Callback {
        public void tooManyProcessesOfType(String processType);

        public void noDefinitionForProcessesOfType(String processType);
    }

    public static class Data implements Serializable {
        /**
         * generated serialVersionUID
         */
        private static final long serialVersionUID = 9163018396438735118L;
        String processType;
        int maxNumberOfProcessesAllowed;
        long[] time;

        public Data(String processType, int maxNumberOfProcessesAllowed) {
            this.processType = processType;
            this.maxNumberOfProcessesAllowed = maxNumberOfProcessesAllowed;
            this.time = new long[maxNumberOfProcessesAllowed];
        }
    }

    public static class IntWrapper {
        int intValue;

        public IntWrapper(int intValue) {
            this.intValue = intValue;
        }
    }
}
