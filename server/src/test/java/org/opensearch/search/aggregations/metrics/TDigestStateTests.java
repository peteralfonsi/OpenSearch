/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.search.aggregations.metrics;

import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import static org.opensearch.search.aggregations.metrics.PercentilesConfig.TDigest.DEFAULT_COMPRESSION;

public class TDigestStateTests extends OpenSearchTestCase {

    public void testMoreThan4BValues() {
        // Regression test for #19528
        // See https://github.com/tdunning/t-digest/pull/70/files#diff-4487072cee29b939694825647928f742R439
        TDigestState digest = new TDigestState(100);
        for (int i = 0; i < 1000; ++i) {
            digest.add(randomDouble());
        }
        final int count = 1 << 29;
        for (int i = 0; i < 10; ++i) {
            digest.add(randomDouble(), count);
        }
        assertEquals(1000 + 10L * (1 << 29), digest.size());
        assertTrue(digest.size() > 2 * Integer.MAX_VALUE);
        final double[] quantiles = new double[] { 0, 0.1, 0.5, 0.9, 1, randomDouble() };
        Arrays.sort(quantiles);
        double prev = Double.NEGATIVE_INFINITY;
        for (double q : quantiles) {
            final double v = digest.quantile(q);
            logger.trace("q=" + q + ", v=" + v);
            assertTrue(v >= prev);
            assertTrue("Unexpectedly low value: " + v, v >= 0.0);
            assertTrue("Unexpectedly high value: " + v, v <= 1.0);
            prev = v;
        }
    }

    public void testError() {
        // Check if offsetting the values fed into the digest - for example as in a timestamp - affects accuracy.
        //long offset = 0; //1_000_000_000_000L;
        int numDocs = 100_000_000;
        int rangeOfVariation = 10_000_000;
        int numIters = 10;


        List<Double> values = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            values.add(( double) i * rangeOfVariation / numDocs);
        }
        Collections.shuffle(values);

        // Get errors without offset
        System.out.println("truePercentile,percentileApproximation,abs(error)");
        List<Long> elapsedTimes = new ArrayList<>();
        for (long offset : new long[]{0, 1_000_000_000_000L}) {
            for (int iter = 0; iter < numIters; iter++) {
                TDigestState state = new TDigestState(DEFAULT_COMPRESSION);
                long nanosStart = System.nanoTime();
                for (double value : values) {
                    state.add(value + offset);
                }
                long elapsed = System.nanoTime() - nanosStart;
                elapsedTimes.add(elapsed);
                for (double quantile = 0.01; quantile < 1.0; quantile += 0.01) {
                    double percentileApproximation = state.quantile(quantile);
                    double truePercentile = quantile * rangeOfVariation + offset;
                    double error = Math.abs(percentileApproximation - truePercentile);
                    String formatted = String.format(Locale.US, "%f,%f,%f", truePercentile, percentileApproximation, error);
                    System.out.println(formatted);
                }
            }
        }
        System.out.println("\n\nElapsed times adding doc values for each run:");
        for (long time : elapsedTimes) {
            System.out.println(time);
        }
    }
}
