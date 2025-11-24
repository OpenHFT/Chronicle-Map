/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map.internal;

import net.openhft.chronicle.core.analytics.AnalyticsFacade;
import net.openhft.chronicle.core.pom.PomProperties;

/**
 * Singleton holder for the Chronicle Map analytics reporter.
 * <p>
 * Centralises construction of the shared {@link AnalyticsFacade} so callers reuse the same
 * measurement id, secret and app version metadata derived from the POM, while still honouring the
 * global {@code chronicle.analytics.disable} toggle.
 */
public enum AnalyticsHolder {
    ; // none

    // Todo: VERSION is "unknown" for some reason
    private static final String VERSION = PomProperties.version("net.openhft", "chronicle-map");

    private static final AnalyticsFacade ANALYTICS = AnalyticsFacade.standardBuilder("G-TDTJG5CT6G", "J8qsWGHgQP6CLs43mQ10KQ", VERSION)
            //.withReportDespiteJUnit()
            .withDebugLogger(System.out::println)
            //.withUrl("https://www.google-analytics.com/debug/mp/collect")
            .build();

    public static AnalyticsFacade instance() {
        return ANALYTICS;
    }
}
