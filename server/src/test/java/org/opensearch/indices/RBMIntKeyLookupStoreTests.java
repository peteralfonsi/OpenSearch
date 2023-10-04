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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.indices;

import org.opensearch.common.Randomness;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

public class RBMIntKeyLookupStoreTests extends OpenSearchTestCase {
    // Tests mostly based on HybridIntKeyStoreTests.java
    public void testInit() {
        long memCap = 100 * RBMSizeEstimator.BYTES_IN_MB;
        RBMIntKeyLookupStore kls = new RBMIntKeyLookupStore((int) Math.pow(2, 29), memCap);
        assertEquals(0, kls.getSize());
        assertEquals(memCap, kls.getMemorySizeCapInBytes());
    }
    public void testTransformationLogic() throws Exception {
        int modulo = (int) Math.pow(2, 29);
        RBMIntKeyLookupStore kls = new RBMIntKeyLookupStore((int) Math.pow(2, 29), 0L);
        int offset = 3;
        for (int i = 0; i < 4; i++) { // after this we run into max value, but thats not a flaw with the class design
            int posValue = i * modulo + offset;
            kls.add(posValue);
            int negValue = -(i * modulo + offset);
            kls.add(negValue);
        }
        assertEquals(2, kls.getSize());
        int[] testVals = new int[]{0, 1, -1, -23495, 23058, modulo, -modulo, Integer.MAX_VALUE, Integer.MIN_VALUE};
        for (int value : testVals) {
            assertTrue(kls.getInternalRepresentation(value) < modulo);
            assertTrue(kls.getInternalRepresentation(value) > -modulo);
        }
    }

    public void testContainsAndForceRemove() throws Exception {
        RBMIntKeyLookupStore kls = new RBMIntKeyLookupStore((int) Math.pow(2, 29), 0L);
        for (int i = 0; i < 2000; i++) {
            kls.add(i);
            assertTrue(kls.contains(i));
        }
        assertFalse(kls.canHaveFalseNegatives());
        for (int i = 1900; i < 2000; i++) {
            kls.forceRemove(i);
            assertFalse(kls.contains(i));
        }
        assertEquals(1900, kls.getSize());
    }

    public void testAddingStatsGetters() throws Exception {
        int modulo = (int) Math.pow(2, 15);
        RBMIntKeyLookupStore kls = new RBMIntKeyLookupStore(modulo, 0L);
        kls.add(15);
        kls.add(-15);
        assertEquals(2, kls.getTotalAdds());
        assertEquals(0, kls.getCollisions());

        int offset = 1;
        for (int i = 0; i < 10; i++) {
            kls.add(i * modulo + offset);
        }
        assertEquals(12, kls.getTotalAdds());
        assertEquals(9, kls.getCollisions());
    }

    public void testRegenerateStore() throws Exception {
        int numToAdd = 10000000;
        Random rand = Randomness.get();
        RBMIntKeyLookupStore kls = new RBMIntKeyLookupStore((int) Math.pow(2, 29), 0L);
        for (int i = 0; i < numToAdd; i++) {
            kls.add(i);
        }
        assertEquals(numToAdd, kls.getSize());
        Integer[] newVals = new Integer[1000]; // margin accounts for collisions
        for (int j = 0; j < newVals.length; j++) {
            newVals[j] = rand.nextInt();
        }
        kls.regenerateStore(newVals);
        assertTrue(Math.abs(kls.getSize() - newVals.length) < 3); // inexact due to collisions

        // test clear()
        kls.clear();
        assertEquals(0, kls.getSize());
    }

    public void testAddingDuplicates() throws Exception {
        RBMIntKeyLookupStore kls = new RBMIntKeyLookupStore((int) Math.pow(2, 29), 0L);
        int numToAdd = 4820411;
        for (int i = 0; i < numToAdd; i++) {
            kls.add(i);
            kls.add(i);
        }
        for (int j = 0; j < 1000; j++) {
            kls.add(577);
        }
        assertEquals(numToAdd, kls.getSize());
    }

    public void testMemoryCapBlocksAdd() throws Exception {
        int modulo = (int) Math.pow(2, 29);
        for (int maxEntries: new int[]{2342000, 1000, 100000}) {
            long memSizeCapInBytes = RBMSizeEstimator.getSizeInBytesWithModulo(maxEntries, modulo);
            RBMIntKeyLookupStore kls = new RBMIntKeyLookupStore(modulo, memSizeCapInBytes);
            for (int j = 0; j < maxEntries + 1000; j++) {
                kls.add(j);
            }
            assertTrue(Math.abs(maxEntries - kls.getSize()) < (double) maxEntries / 25);
            // exact cap varies a small amount bc of floating point, especially when we use bytes instead of MB for calculations
            // precision gets much worse when we compose the two functions, as we do here, but this wouldn't happen in an actual use case
        }
    }

    public void testConcurrency() throws Exception {
        Random rand = Randomness.get();
        int modulo = (int) Math.pow(2, 29);
        long memCap = 100 * RBMSizeEstimator.BYTES_IN_MB;
        for (int j = 0; j < 5; j++) { // test with different numbers of threads
            RBMIntKeyLookupStore kls = new RBMIntKeyLookupStore((int) Math.pow(2, 29), 0L);
            int numThreads = rand.nextInt(50) + 1;
            ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(numThreads);
            // In this test we want to add the first 200K numbers and check they're all correctly there.
            // We do some duplicates too to ensure those aren't incorrectly added.
            int amountToAdd = 200000;
            ArrayList<Future<Boolean>> wasAdded = new ArrayList<>(amountToAdd);
            ArrayList<Future<Boolean>> duplicatesWasAdded = new ArrayList<>();
            for (int i = 0; i < amountToAdd; i++) {
                wasAdded.add(null);
            }
            for (int i = 0; i < amountToAdd; i++) {
                final int val = i;
                Future<Boolean> fut = executor.submit(() -> {
                    boolean didAdd;
                    try {
                        didAdd = kls.add(val);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    return didAdd;
                });
                wasAdded.set(val, fut);
                if (val % 1000 == 0) {
                    // do a duplicate add
                    Future<Boolean> duplicateFut = executor.submit(() -> {
                        boolean didAdd;
                        try {
                            didAdd = kls.add(val);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                        return didAdd;
                    });
                    duplicatesWasAdded.add(duplicateFut);
                }
            }
            int originalAdds = 0;
            int duplicateAdds = 0;
            for (Future<Boolean> fut : wasAdded) {
                if (fut.get()) {
                    originalAdds++;
                }
            }
            for (Future<Boolean> duplicateFut : duplicatesWasAdded) {
                if (duplicateFut.get()) {
                    duplicateAdds++;
                }
            }
            for (int i = 0; i < amountToAdd; i++) {
                assertTrue(kls.contains(i));
            }
            assertEquals(amountToAdd, originalAdds + duplicateAdds);
            assertEquals(amountToAdd, kls.getSize());
            assertEquals(amountToAdd / 1000, kls.getCollisions());
            executor.shutdown();
        }
    }

    public void testRemoveNoCollisions() throws Exception {
        long memCap = 100L * RBMSizeEstimator.BYTES_IN_MB;
        int numToAdd = 195000;
        RBMIntKeyLookupStore kls = new RBMIntKeyLookupStore(0, memCap);
        // there should be no collisions for sequential positive numbers up to modulo
        for (int i = 0; i < numToAdd; i++) {
            kls.add(i);
        }
        for (int i = 0; i < 1000; i++) {
            assertTrue(kls.remove(i));
            assertFalse(kls.contains(i));
            assertFalse(kls.valueHasHadCollision(i));
        }
        assertEquals(numToAdd - 1000, kls.getSize());
    }

    public void testRemoveWithCollisions() throws Exception {
        int modulo = (int) Math.pow(2, 26);
        long memCap = 100L * RBMSizeEstimator.BYTES_IN_MB;
        RBMIntKeyLookupStore kls = new RBMIntKeyLookupStore(modulo, memCap);
        for (int i = 0; i < 10; i++) {
            kls.add(i);
            if (i % 2 == 1) {
                kls.add(-i);
                assertFalse(kls.valueHasHadCollision(i));
                kls.add(i + modulo);
                assertTrue(kls.valueHasHadCollision(i));
            } else {
                assertFalse(kls.valueHasHadCollision(i));
            }
        }
        assertEquals(15, kls.getSize());
        for (int i = 0; i < 10; i++) {
            boolean didRemove = kls.remove(i);
            if (i % 2 == 1) {
                // we expect a collision with i + modulo, so we can't remove
                assertFalse(didRemove);
                assertTrue(kls.contains(i));
                // but we should be able to remove -i
                boolean didRemoveNegative = kls.remove(-i);
                assertTrue(didRemoveNegative);
                assertFalse(kls.contains(-i));
            } else {
                // we expect no collision
                assertTrue(didRemove);
                assertFalse(kls.contains(i));
                assertFalse(kls.valueHasHadCollision(i));
            }
        }
        assertEquals(5, kls.getSize());
        int offset = 12;
        kls.add(offset);
        for (int j = 1; j < 5; j++) {
            kls.add(offset + j * modulo);
        }
        assertEquals(6, kls.getSize());
        assertFalse(kls.remove(offset + modulo));
        assertTrue(kls.valueHasHadCollision(offset + 15 * modulo));
        assertTrue(kls.contains(offset + 17 * modulo));
    }

    public void testNullInputs() throws Exception {
        RBMIntKeyLookupStore kls = new RBMIntKeyLookupStore((int) Math.pow(2, 29), 0L);
        assertFalse(kls.add(null));
        assertFalse(kls.contains(null));
        assertEquals(0, (int) kls.getInternalRepresentation(null));
        assertFalse(kls.remove(null));
        kls.forceRemove(null);
        assertFalse(kls.canHaveFalseNegatives());
        assertFalse(kls.isCollision(null, null));
        assertEquals(0, kls.getTotalAdds());
        Integer[] newVals = new Integer[]{1, 17, -2, null, -4, null};
        kls.regenerateStore(newVals);
        assertEquals(4, kls.getSize());
    }

    public void testMemoryCapValueInitialization() {
        double[] logModulos = new double[] { 0.0, 31.2, 30, 29, 28, 13 };
        double[] expectedMultipliers = new double[] { 1.2, 1.2, 1.2, 1, 1, 1 };
        double[] expectedSlopes = new double[] { 0.637, 0.637, 0.637, 0.619, 0.614, 0.629 };
        double[] expectedIntercepts = new double[] { 3.091, 3.091, 3.091, 2.993, 2.905, 2.603 };
        long memSizeCapInBytes = (long) 100.0 * RBMSizeEstimator.BYTES_IN_MB;
        double delta = 0.01;
        for (int i = 0; i < logModulos.length; i++) {
            int modulo = 0;
            if (logModulos[i] != 0) {
                modulo = (int) Math.pow(2, logModulos[i]);
            }
            RBMIntKeyLookupStore kls = new RBMIntKeyLookupStore(modulo, memSizeCapInBytes);
            assertEquals(kls.stats.memSizeCapInBytes, kls.getMemorySizeCapInBytes(), 1.0);
            assertEquals(expectedMultipliers[i], kls.sizeEstimator.bufferMultiplier, delta);
            assertEquals(expectedSlopes[i], kls.sizeEstimator.slope, delta);
            assertEquals(expectedIntercepts[i], kls.sizeEstimator.intercept, delta);
            System.out.println("log modulo: " + logModulos[i]);
            System.out.println("Estimated size at 10^6: " + kls.sizeEstimator.getSizeInBytes(1000000));
        }

    }
}
