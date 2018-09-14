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

package net.openhft.chronicle.map.ipc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class StateMachineProcessor implements Runnable {
    private final StateMachineData smd;
    private final StateMachineState from;
    private final StateMachineState transition;
    private final StateMachineState to;
    private final Logger logger;

    /**
     * @param smd
     * @param from
     * @param to
     */
    public StateMachineProcessor(final StateMachineData smd, StateMachineState from, StateMachineState transition, StateMachineState to) {
        this.smd = smd;
        this.from = from;
        this.transition = transition;
        this.to = to;

        this.logger = LoggerFactory.getLogger(from + " => " + transition + " => " + to);
    }

    /**
     * @param smd
     * @param from
     * @param transition
     * @param to
     */
    public static void runProcessor(final StateMachineData smd, StateMachineState from, StateMachineState transition, StateMachineState to) {
        new StateMachineProcessor(smd, from, transition, to).run();
    }

    @Override
    public void run() {
        while (!smd.done()) {
            if (smd.stateIn(transition)) {
                doProcess();
            }

            logger.info("Wait for {}", from);
            smd.waitForState(from, transition);

            doProcess();
        }
    }

    private void doProcess() {
        smd.incStateData();

        logger.info("Status {}, Next {}, Data {}",
                from,
                to,
                smd.getStateData()
        );

        smd.setState(transition, to);
    }
}
