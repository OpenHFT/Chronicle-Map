/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map.ipc;

import net.openhft.chronicle.core.Jvm;

/**
 *
 */
public class StateMachineProcessor implements Runnable {
    private final StateMachineData smd;
    private final StateMachineState from;
    private final StateMachineState transition;
    private final StateMachineState to;

    /**
     */
    public StateMachineProcessor(final StateMachineData smd, StateMachineState from, StateMachineState transition, StateMachineState to) {
        this.smd = smd;
        this.from = from;
        this.transition = transition;
        this.to = to;

    }

    /**
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

            Jvm.debug().on(getClass(), "Wait for " + from);
            smd.waitForState(from, transition);

            doProcess();
        }
    }

    private void doProcess() {
        smd.incStateData();

        Jvm.debug().on(getClass(),
                "Status " + from + ", " +
                        "Next " + to + ", " +
                        "Data " + smd.getStateData());

        smd.setState(transition, to);
    }
}
