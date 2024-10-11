/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.countminsketch;

import org.opensearch.common.Randomness;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class CountMinSketchTests extends OpenSearchTestCase {
    public void testEstimationAccuracy() {
        CountMinSketch cms = new CountMinSketch(25, 1024, -1);
        int numKeys = 2000;
        int maxFreq = 30;
        Random rand = Randomness.get();
        int[] trueFrequencies = new int[numKeys];
        List<Integer> toIncrement = new ArrayList<>();
        for (int i = 0; i < numKeys; i++) {
            trueFrequencies[i] = rand.nextInt(maxFreq);
            for (int j = 0; j < trueFrequencies[i]; j++) {
                toIncrement.add(i);
            }
        }
        // Now shuffle the list of pending values to increment and actually increment them
        Collections.shuffle(toIncrement);
        for (int x : toIncrement) {
            cms.increment(x);
        }

        // Now check the estimated frequency is >= the actual frequency and print the error (debug only)
        for (int i = 0; i < numKeys; i++) {
            int estimate = cms.estimate(i);
            assertTrue(estimate >= trueFrequencies[i]);
            if (estimate >= trueFrequencies[i]) {
                System.out.println("True frequency = " + trueFrequencies[i] + ", estimated frequency = " + estimate + ", error = " + ((float) estimate / trueFrequencies[i] - 1));
            }
        }
    }
}
