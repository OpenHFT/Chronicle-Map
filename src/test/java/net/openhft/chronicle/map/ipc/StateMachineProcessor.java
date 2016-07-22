/*
 *      Copyright (C) 2012, 2016  higherfrequencytrading.com
 *      Copyright (C) 2016 Roman Leventov
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
