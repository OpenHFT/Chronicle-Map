/*
 *      Copyright (C) 2015  higherfrequencytrading.com
 *      Copyright 2001-2015 The Apache Software Foundation
 *      Copyright 2010-2012 CS Systèmes d'Information
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

package net.openhft.chronicle.hash.impl.util.math;

class Precision {
    
    private Precision() {
        
    }

    /** Offset to order signed double numbers lexicographically. */
    private static final long SGN_MASK = 0x8000000000000000L;
    /** Positive zero bits. */
    private static final long POSITIVE_ZERO_DOUBLE_BITS = Double.doubleToRawLongBits(+0.0);
    /** Negative zero bits. */
    private static final long NEGATIVE_ZERO_DOUBLE_BITS = Double.doubleToRawLongBits(-0.0);

    /**
     * Returns {@code true} if there is no double value strictly between the
     * arguments or the difference between them is within the range of allowed
     * error (inclusive).
     *
     * @param x First value.
     * @param y Second value.
     * @param eps Amount of allowed absolute error.
     * @return {@code true} if the values are two adjacent floating point
     * numbers or they are within range of each other.
     */
    public static boolean isEquals(double x, double y, double eps) {
        return isEquals(x, y, 1) || Math.abs(y - x) <= eps;
    }

    /**
     * Returns true if both arguments are equal or within the range of allowed
     * error (inclusive).
     * <p>
     * Two float numbers are considered equal if there are {@code (maxUlps - 1)}
     * (or fewer) floating point numbers between them, i.e. two adjacent
     * floating point numbers are considered equal.
     * </p>
     * <p>
     * Adapted from <a
     * href="http://randomascii.wordpress.com/2012/02/25/comparing-floating-point-numbers-2012-edition/">
     * Bruce Dawson</a>
     * </p>
     *
     * @param x first value
     * @param y second value
     * @param maxUlps {@code (maxUlps - 1)} is the number of floating point
     * values between {@code x} and {@code y}.
     * @return {@code true} if there are fewer than {@code maxUlps} floating
     * point values between {@code x} and {@code y}.
     */
    public static boolean isEquals(final double x, final double y, final int maxUlps) {

        final long xInt = Double.doubleToRawLongBits(x);
        final long yInt = Double.doubleToRawLongBits(y);

        final boolean isEqual;
        if (((xInt ^ yInt) & SGN_MASK) == 0L) {
            // number have same sign, there is no risk of overflow
            isEqual = Math.abs(xInt - yInt) <= maxUlps;
        } else {
            // number have opposite signs, take care of overflow
            final long deltaPlus;
            final long deltaMinus;
            if (xInt < yInt) {
                deltaPlus  = yInt - POSITIVE_ZERO_DOUBLE_BITS;
                deltaMinus = xInt - NEGATIVE_ZERO_DOUBLE_BITS;
            } else {
                deltaPlus  = xInt - POSITIVE_ZERO_DOUBLE_BITS;
                deltaMinus = yInt - NEGATIVE_ZERO_DOUBLE_BITS;
            }

            if (deltaPlus > maxUlps) {
                isEqual = false;
            } else {
                isEqual = deltaMinus <= (maxUlps - deltaPlus);
            }
}

        return isEqual && !Double.isNaN(x) && !Double.isNaN(y);

    }
}
