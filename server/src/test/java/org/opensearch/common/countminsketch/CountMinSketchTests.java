/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.countminsketch;

import org.opensearch.common.Randomness;
import org.opensearch.common.cache.ICacheKey;
import org.opensearch.common.cache.LoadAwareCacheLoader;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Phaser;

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
        int numWithError = 0;
        double totError = 0.0;
        for (int i = 0; i < numKeys; i++) {
            long estimate = cms.estimate(i);
            assertTrue(estimate >= trueFrequencies[i]);
            if (estimate > trueFrequencies[i]) {
                numWithError++;
                if (trueFrequencies[i] > 0) { // Skip infinity error ... eh
                    double error = ((double) estimate / trueFrequencies[i] - 1);
                    //System.out.println("True frequency = " + trueFrequencies[i] + ", estimated frequency = " + estimate + ", error = " + error);
                    totError += error;
                }
            }
        }
        System.out.println("Fraction with error = " + (double) numWithError / numKeys);
        System.out.println("Average error = " + totError / numKeys);
    }

    public void testDecay() {
        int decayPeriod = 4;
        CountMinSketch cms = new CountMinSketch(5, 128, decayPeriod);
        int key = 7;
        for (int i = 0; i < decayPeriod; i++) {
            cms.increment(key);
            if (i < decayPeriod - 1) {
                assertEquals(i + 1, cms.estimate(key));
            }
        }
        // After the last estimation call, decay should have happened
        assertEquals(decayPeriod / 2, cms.estimate(key));
    }

    public void testMultithreadedDecay() throws Exception {
        int decayPeriod = 8;
        CountMinSketch cms = new CountMinSketch(5, 128, decayPeriod);
        int key = 7;
        for (int i = 0; i < decayPeriod - 1; i++) {
            cms.increment(key);
        }
        // Set up 2 threads to increment at once; only the first one should actually trigger decay,
        // so we expect the estimate to be (decayPeriod + 1) / 2 + 1 at the end (where / is floor int division)
        Thread[] threads = new Thread[2];
        Phaser phaser = new Phaser(3);
        CountDownLatch countDownLatch = new CountDownLatch(2); // To wait for all threads to finish.

        List<LoadAwareCacheLoader<ICacheKey<String>, String>> loadAwareCacheLoaderList = new CopyOnWriteArrayList<>();

        for (int i = 0; i < 2; i++) {
            threads[i] = new Thread(() -> {
                phaser.arriveAndAwaitAdvance();
                cms.increment(key);
                countDownLatch.countDown();
            });
            threads[i].start();
        }
        phaser.arriveAndAwaitAdvance();
        countDownLatch.await();
        assertEquals(((decayPeriod + 1) >>> 1), cms.estimate(key));
        // I assume the +1 from the second thread happens before the decay is complete - but it could happen after,
        // so this may be flaky. instead check decay count. but fine for proof of concept
        assertEquals(1, cms.decayCount);
    }

}
