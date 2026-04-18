/*
 * Copyright 2013-2026 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map.fromdocs;

public final class Point {

    double x, y;

    public static Point of(double x, double y) {
        Point p = new Point();
        p.x = x;
        p.y = y;
        return p;
    }
}
