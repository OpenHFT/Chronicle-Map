/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash.impl.stage.hash;

import net.openhft.sg.Staged;

/**
 * Placeholder stage for logging-related state.
 *
 * <p>Currently empty but retained as a separate stage so that logging concerns can
 * be injected into compiled contexts without disturbing the core stage graph.
 */
@Staged
public class LogHolder {
}
