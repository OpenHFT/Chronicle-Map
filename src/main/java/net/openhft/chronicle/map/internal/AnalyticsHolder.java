package net.openhft.chronicle.map.internal;

import net.openhft.chronicle.core.analytics.AnalyticsFacade;
import net.openhft.chronicle.core.pom.PomProperties;

public enum AnalyticsHolder {;

    // Todo: VERSION is "unknown" for some reason
    private static final String VERSION = PomProperties.version("net.openhft", "chronicle-map");

    private static final AnalyticsFacade ANALYTICS = AnalyticsFacade.standardBuilder("G-TDTJG5CT6G", "J8qsWGHgQP6CLs43mQ10KQ", VERSION)
            //.withReportDespiteJUnit()
            .build();

    public static AnalyticsFacade instance() {
        return ANALYTICS;
    }

}