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
        RBMIntKeyLookupStore base_kls = new RBMIntKeyLookupStore((int) Math.pow(2, 29), memCap);
        RemovableRBMIntKeyLookupStore rkls = new RemovableRBMIntKeyLookupStore((int) Math.pow(2, 29), memCap);
        for (RBMIntKeyLookupStore kls : new RBMIntKeyLookupStore[] { base_kls, rkls }) {
            assertEquals(0, kls.getSize());
            assertEquals(memCap, kls.getMemorySizeCapInBytes());
        }
    }
    public void testTransformationLogic() throws Exception {
        int modulo = (int) Math.pow(2, 29);
        RBMIntKeyLookupStore base_kls = new RBMIntKeyLookupStore((int) Math.pow(2, 29), 0L);
        RemovableRBMIntKeyLookupStore rkls = new RemovableRBMIntKeyLookupStore((int) Math.pow(2, 29), 0L);
        for (RBMIntKeyLookupStore kls : new RBMIntKeyLookupStore[] { base_kls, rkls }) {
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
    }

    public void testContainsAndForceRemove() throws Exception {
        RBMIntKeyLookupStore base_kls = new RBMIntKeyLookupStore((int) Math.pow(2, 29), 0L);
        RemovableRBMIntKeyLookupStore rkls = new RemovableRBMIntKeyLookupStore((int) Math.pow(2, 29), 0L);
        for (RBMIntKeyLookupStore kls : new RBMIntKeyLookupStore[] { base_kls, rkls }) {
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
    }

    public void testAddingStatsGetters() throws Exception {
        int modulo = (int) Math.pow(2, 15);
        RBMIntKeyLookupStore base_kls = new RBMIntKeyLookupStore(modulo, 0L);
        RemovableRBMIntKeyLookupStore rkls = new RemovableRBMIntKeyLookupStore(modulo, 0L);
        for (RBMIntKeyLookupStore kls : new RBMIntKeyLookupStore[] { base_kls, rkls }) {
            kls.add(15);
            kls.add(-15);
            assertEquals(2, kls.getNumAddAttempts());
            assertEquals(0, kls.getNumCollisions());

            int offset = 1;
            for (int i = 0; i < 10; i++) {
                kls.add(i * modulo + offset);
            }
            assertEquals(12, kls.getNumAddAttempts());
            assertEquals(9, kls.getNumCollisions());
        }

    }

    public void testRegenerateStore() throws Exception {
        int numToAdd = 10000000;
        Random rand = Randomness.get();
        RBMIntKeyLookupStore base_kls = new RBMIntKeyLookupStore((int) Math.pow(2, 29), 0L);
        RemovableRBMIntKeyLookupStore rkls = new RemovableRBMIntKeyLookupStore((int) Math.pow(2, 29), 0L);
        for (RBMIntKeyLookupStore kls : new RBMIntKeyLookupStore[] { base_kls, rkls }) {
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
    }

    public void testAddingDuplicates() throws Exception {
        RBMIntKeyLookupStore base_kls = new RBMIntKeyLookupStore((int) Math.pow(2, 29), 0L);
        RemovableRBMIntKeyLookupStore rkls = new RemovableRBMIntKeyLookupStore((int) Math.pow(2, 29), 0L);
        for (RBMIntKeyLookupStore kls : new RBMIntKeyLookupStore[] { base_kls, rkls }) {
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
    }

    public void testMemoryCapValueInitialization() {
        double[] logModulos = new double[] { 0.0, 31.2, 30, 29, 28, 13 }; // these will NOT decrement by 1
        double[] expectedMultipliers = new double[] { 1.35, 1.35, 1.35, 1.6, 1.6, 1.6 };
        double[] expectedSlopes = new double[] { 0.69, 0.69, 0.69, 0.75, 0.75, 0.88 };
        double[] expectedIntercepts = new double[] { -3, -3, -3, -3.5, -3.5, -4.5 };
        long memSizeCapInBytes = (long) 100.0 * RBMSizeEstimator.BYTES_IN_MB;
        double delta = 0.01;
        for (int i = 0; i < logModulos.length; i++) {
            int modulo = 0;
            if (logModulos[i] != 0) {
                modulo = (int) Math.pow(2, logModulos[i]);
            }
            RBMIntKeyLookupStore base_kls = new RBMIntKeyLookupStore(modulo, memSizeCapInBytes);
            RemovableRBMIntKeyLookupStore rkls = new RemovableRBMIntKeyLookupStore(modulo, memSizeCapInBytes);
            for (RBMIntKeyLookupStore kls : new RBMIntKeyLookupStore[] { base_kls, rkls }) {
                assertEquals(kls.memSizeCapInBytes, kls.getMemorySizeCapInBytes(), 1.0);
                assertEquals(expectedMultipliers[i], kls.getRBMMemBufferMultiplier(), delta);
                assertEquals(expectedSlopes[i], kls.getRBMMemSlope(), delta);
                assertEquals(expectedIntercepts[i], kls.getRBMMemIntercept(), delta);
            }
        }
    }

    public void testMemoryCapBlocksAdd() throws Exception {
        int modulo = (int) Math.pow(2, 29);
        for (int maxEntries: new int[]{2342000, 1000, 100000}) {
            long memSizeCapInBytes = HybridIntKeyLookupStore.getRBMMemSizeWithModuloInBytes(maxEntries, modulo);
            RBMIntKeyLookupStore base_kls = new RBMIntKeyLookupStore((int) Math.pow(2, 29), memSizeCapInBytes);
            RemovableRBMIntKeyLookupStore rkls = new RemovableRBMIntKeyLookupStore((int) Math.pow(2, 29), memSizeCapInBytes);
            for (RBMIntKeyLookupStore kls : new RBMIntKeyLookupStore[] { base_kls, rkls }) {
                for (int j = 0; j < maxEntries + 1000; j++) {
                    kls.add(j);
                }
                assertTrue(Math.abs(maxEntries - kls.getSize()) < 5); // exact cap varies a small amount bc of floating point
            }
        }
    }

    public void testConcurrency() throws Exception {
        Random rand = Randomness.get();
        int modulo = (int) Math.pow(2, 29);
        long memCap = 100 * RBMSizeEstimator.BYTES_IN_MB;
        for (int j = 0; j < 5; j++) { // test with different numbers of threads
            RBMIntKeyLookupStore base_kls = new RBMIntKeyLookupStore((int) Math.pow(2, 29), 0L);
            RemovableRBMIntKeyLookupStore rkls = new RemovableRBMIntKeyLookupStore((int) Math.pow(2, 29), 0L);
            for (RBMIntKeyLookupStore kls : new RBMIntKeyLookupStore[] { base_kls, rkls }) {
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
                assertEquals(amountToAdd / 1000, kls.getNumCollisions());
                executor.shutdown();
            }
        }
    }

    public void testRemoveNoCollisions() throws Exception {
        // only for RemovableRBMIntKeyLookupStore
        long memCap = 100L * RBMSizeEstimator.BYTES_IN_MB;
        int numToAdd = 195000;
        RemovableRBMIntKeyLookupStore rkls = new RemovableRBMIntKeyLookupStore(0, memCap);
        // there should be no collisions for sequential positive numbers up to modulo
        assertTrue(rkls.supportsRemoval());
        for (int i = 0; i < numToAdd; i++) {
            rkls.add(i);
        }
        for (int i = 0; i < 1000; i++) {
            assertTrue(rkls.remove(i));
            assertFalse(rkls.contains(i));
            assertFalse(rkls.valueHasHadCollision(i));
        }
        assertEquals(numToAdd - 1000, rkls.getSize());
    }

    public void testRemoveWithCollisions() throws Exception {
        int modulo = (int) Math.pow(2, 26);
        long memCap = 100L * RBMSizeEstimator.BYTES_IN_MB;
        RemovableRBMIntKeyLookupStore rkls = new RemovableRBMIntKeyLookupStore(modulo, memCap);
        for (int i = 0; i < 10; i++) {
            rkls.add(i);
            if (i % 2 == 1) {
                rkls.add(-i);
                assertFalse(rkls.valueHasHadCollision(i));
                rkls.add(i + modulo);
                assertTrue(rkls.valueHasHadCollision(i));
            } else {
                assertFalse(rkls.valueHasHadCollision(i));
            }
        }
        assertEquals(15, rkls.getSize());
        for (int i = 0; i < 10; i++) {
            boolean didRemove = rkls.remove(i);
            if (i % 2 == 1) {
                // we expect a collision with i + modulo, so we can't remove
                assertFalse(didRemove);
                assertTrue(rkls.contains(i));
                // but we should be able to remove -i
                boolean didRemoveNegative = rkls.remove(-i);
                assertTrue(didRemoveNegative);
                assertFalse(rkls.contains(-i));
            } else {
                // we expect no collision
                assertTrue(didRemove);
                assertFalse(rkls.contains(i));
                assertFalse(rkls.valueHasHadCollision(i));
            }
        }
        assertEquals(5, rkls.getSize());
        int offset = 12;
        rkls.add(offset);
        for (int j = 1; j < 5; j++) {
            rkls.add(offset + j * modulo);
        }
        assertEquals(6, rkls.getSize());
        assertFalse(rkls.remove(offset + modulo));
        assertTrue(rkls.valueHasHadCollision(offset + 15 * modulo));
        assertTrue(rkls.contains(offset + 17 * modulo));
    }

    public void testNullInputs() throws Exception {
        RBMIntKeyLookupStore base_kls = new RBMIntKeyLookupStore((int) Math.pow(2, 29), 0L);
        RemovableRBMIntKeyLookupStore rkls = new RemovableRBMIntKeyLookupStore((int) Math.pow(2, 29), 0L);
        for (RBMIntKeyLookupStore kls : new RBMIntKeyLookupStore[] { base_kls, rkls }) {
            assertFalse(kls.add(null));
            assertFalse(kls.contains(null));
            assertEquals(0, (int) kls.getInternalRepresentation(null));
            assertFalse(kls.remove(null));
            kls.forceRemove(null);
            assertFalse(kls.canHaveFalseNegatives());
            assertFalse(kls.isCollision(null, null));
            assertEquals(0, kls.getNumAddAttempts());
            Integer[] newVals = new Integer[]{1, 17, -2, null, -4, null};
            kls.regenerateStore(newVals);
            assertEquals(4, kls.getSize());
        }
    }
}
