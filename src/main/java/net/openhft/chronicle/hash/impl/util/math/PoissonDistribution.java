/*
 * Copyright 2012-2018 Chronicle Map Contributors
 * Copyright 2001-2015 The Apache Software Foundation
 * Copyright 2010-2012 CS Systèmes d'Information
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

package net.openhft.chronicle.hash.impl.util.math;

public class PoissonDistribution {

    private static final double EPSILON = 1e-12;
    /**
     * Poisson distribution is used to estimate segment fillings. Segments are not bound with
     * Integer.MAX_VALUE, but it's not clear if algorithms from Commons Math could work with such
     * big values as Long.MAX_VALUES
     */
    private static final long UPPER_BOUND = 1L << 36;
    private static final int MAX_ITERATIONS = 10000000;

    public static double cumulativeProbability(double mean, long x) {
        if (x < 0) {
            return 0;
        }
        if (x >= UPPER_BOUND) {
            return 1;
        }
        return Gamma.regularizedGammaQ((double) x + 1, mean, EPSILON, MAX_ITERATIONS);
    }

    public static long inverseCumulativeProbability(double mean, double p) {
        checkProbability(p);

        long lower = 0;
        if (p == 0.0) {
            return lower;
        }
        lower -= 1; // this ensures cumulativeProbability(lower) < p, which
        // is important for the solving step

        long upper = UPPER_BOUND;
        if (p == 1.0) {
            return upper;
        }

        // use the one-sided Chebyshev inequality to narrow the bracket
        // cf. AbstractRealDistribution.inverseCumulativeProbability(double)
        final double mu = mean;
        // in Poisson distribution, variance == mean
        double variance = mean;
        final double sigma = Math.sqrt(variance);
        final boolean chebyshevApplies = !(Double.isInfinite(mu) || Double.isNaN(mu) ||
                Double.isInfinite(sigma) || Double.isNaN(sigma) || sigma == 0.0);
        if (chebyshevApplies) {
            double k = Math.sqrt((1.0 - p) / p);
            double tmp = mu - k * sigma;
            if (tmp > lower) {
                lower = ((int) Math.ceil(tmp)) - 1;
            }
            k = 1.0 / k;
            tmp = mu + k * sigma;
            if (tmp < upper) {
                upper = ((int) Math.ceil(tmp)) - 1;
            }
        }

        return solveInverseCumulativeProbability(mean, p, lower, upper);
    }

    private static void checkProbability(double p) {
        if (p < 0.0 || p > 1.0) {
            throw new IllegalArgumentException("probability should be in [0.0, 1.0] bounds, " + p +
                    " given");
        }
    }

    /**
     * This is a utility function used by {@link
     * #inverseCumulativeProbability(double, double)}. It assumes {@code 0 < p < 1} and
     * that the inverse cumulative probability lies in the bracket {@code
     * (lower, upper]}. The implementation does simple bisection to find the
     * smallest {@code p}-quantile <code>inf{x in Z | P(X<=x) >= p}</code>.
     *
     * @param p     the cumulative probability
     * @param lower a value satisfying {@code cumulativeProbability(lower) < p}
     * @param upper a value satisfying {@code p <= cumulativeProbability(upper)}
     * @return the smallest {@code p}-quantile of this distribution
     */
    private static long solveInverseCumulativeProbability(double mean, final double p,
                                                          long lower, long upper) {

        while (lower + 1 < upper) {
            long xm = (lower + upper) / 2;
            if (xm < lower || xm > upper) {
                /*
                 * Overflow.
                 * There will never be an overflow in both calculation methods
                 * for xm at the same time
                 */
                xm = lower + (upper - lower) / 2;
            }

            double pm = checkedCumulativeProbability(mean, xm);
            if (pm >= p) {
                upper = xm;
            } else {
                lower = xm;
            }
        }
        return upper;
    }

    public static double meanByCumulativeProbabilityAndValue(double p, long x, double precision) {
        checkProbability(p);
        assert x > 0 && x < UPPER_BOUND;

        double lower = 0;
        double upper = UPPER_BOUND;
        while (lower + precision < upper) {
            double m = (lower + upper) / 2;
            double pm = checkedCumulativeProbability(m, x);
            if (pm < p) {
                upper = m;
            } else {
                lower = m;
            }
        }
        return lower;
    }

    /**
     * Computes the cumulative probability function and checks for {@code NaN}
     * values returned. Throws {@code MathInternalError} if the value is
     * {@code NaN}. Rethrows any exception encountered evaluating the cumulative
     * probability function. Throws {@code MathInternalError} if the cumulative
     * probability function returns {@code NaN}.
     *
     * @param argument input value
     * @return the cumulative probability
     * @throws AssertionError if the cumulative probability is {@code NaN}
     */
    private static double checkedCumulativeProbability(double mean, long argument) {
        double result = cumulativeProbability(mean, argument);
        if (Double.isNaN(result)) {
            throw new AssertionError("Discrete cumulative probability function returned NaN " +
                    "for argument " + argument);
        }
        return result;
    }
}
