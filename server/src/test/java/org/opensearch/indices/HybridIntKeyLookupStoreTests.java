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

// Tests base functionality of HybridIntKeyLookupStore for both that class and the inheriting
// RemovableHybridIntKeyLookupStore.

public class HybridIntKeyLookupStoreTests extends OpenSearchTestCase {
    public void testInit() throws Exception {
        HybridIntKeyLookupStore base_kls = new HybridIntKeyLookupStore((int) Math.pow(2, 29), 0L);
        RemovableHybridIntKeyLookupStore rkls = new RemovableHybridIntKeyLookupStore((int) Math.pow(2, 29), 0L);
        for (HybridIntKeyLookupStore kls : new HybridIntKeyLookupStore[] { base_kls, rkls }) {
            assertEquals("HashSet", kls.getCurrentStructure());
            assertEquals(0, kls.getSize());
        }
    }

    public void testStructureTransitions() throws Exception {
        HybridIntKeyLookupStore base_kls = new HybridIntKeyLookupStore((int) Math.pow(2, 29), 0L);
        RemovableHybridIntKeyLookupStore rkls = new RemovableHybridIntKeyLookupStore((int) Math.pow(2, 29), 0L);
        for (HybridIntKeyLookupStore kls : new HybridIntKeyLookupStore[] { base_kls, rkls }) {
            for (int i = 0; i < HybridIntKeyLookupStore.HASHSET_TO_INTARR_THRESHOLD; i++) {
                kls.add(i);
            }
            assertEquals("intArr", kls.getCurrentStructure());
            assertEquals(HybridIntKeyLookupStore.HASHSET_TO_INTARR_THRESHOLD, kls.getSize());
            for (int i = HybridIntKeyLookupStore.HASHSET_TO_INTARR_THRESHOLD; i < HybridIntKeyLookupStore.INTARR_TO_RBM_THRESHOLD; i++) {
                kls.add(i);
            }
            assertEquals("RBM", kls.getCurrentStructure());
            assertEquals(HybridIntKeyLookupStore.INTARR_TO_RBM_THRESHOLD, kls.getSize());
        }
    }

    public void testArrayLogic() throws Exception {
        HybridIntKeyLookupStore base_kls = new HybridIntKeyLookupStore((int) Math.pow(2, 29), 0L);
        RemovableHybridIntKeyLookupStore rkls = new RemovableHybridIntKeyLookupStore((int) Math.pow(2, 29), 0L);
        for (HybridIntKeyLookupStore kls : new HybridIntKeyLookupStore[] { base_kls, rkls }) {
            Random rand = Randomness.get();
            int numToAdd = 50000;
            int[] addedValues = new int[numToAdd];
            for (int i = 0; i < numToAdd; i++) {
                int val = rand.nextInt();
                kls.add(val);
                addedValues[i] = val;
            }
            assertTrue(kls.arrayCorrectlySorted()); // Not sure if this is really good as a public method - but idk how else to do it?
            assertTrue(numToAdd - kls.getSize() < 20); // size should not be too different from numToAdd - exact number varies due to
            // collisions
            int numToRemove = 20000;
            for (int j = 0; j < numToRemove; j++) {
                kls.forceRemove(addedValues[j]);
            }
            assertTrue(numToAdd - numToRemove - kls.getSize() < 20);
            assertTrue(kls.arrayCorrectlySorted());
            assertTrue(kls.canHaveFalseNegatives());
        }
    }

    public void testTransformationLogic() throws Exception {
        int modulo = (int) Math.pow(2, 29);
        HybridIntKeyLookupStore base_kls = new HybridIntKeyLookupStore(modulo, 0L);
        RemovableHybridIntKeyLookupStore rkls = new RemovableHybridIntKeyLookupStore(modulo, 0L);
        for (HybridIntKeyLookupStore kls : new HybridIntKeyLookupStore[] { base_kls, rkls }) {
            int offset = 3;
            for (int i = 0; i < 4; i++) { // after this we run into max value, but thats not a flaw with the class design
                int posValue = i * modulo + offset;
                kls.add(posValue);
                int negValue = -(i * modulo + offset);
                kls.add(negValue);
            }
            assertEquals(1, kls.getSize());

            // test output is always in expected range
            int[] testVals = new int[] { 0, 1, -1, -23495, 23058, modulo, -modulo, Integer.MAX_VALUE, Integer.MIN_VALUE };
            for (int value : testVals) {
                assertTrue(kls.getInternalRepresentation(value) <= 0);
                assertTrue(kls.getInternalRepresentation(value) > -modulo);
            }
        }
    }

    public void testContainsAndForceRemove() throws Exception {
        HybridIntKeyLookupStore base_kls = new HybridIntKeyLookupStore((int) Math.pow(2, 29), 0L);
        RemovableHybridIntKeyLookupStore rkls = new RemovableHybridIntKeyLookupStore((int) Math.pow(2, 29), 0L);
        for (HybridIntKeyLookupStore kls : new HybridIntKeyLookupStore[] { base_kls, rkls }) {
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
            int lastSize = kls.getSize();
            for (int i = kls.getSize(); i < HybridIntKeyLookupStore.HASHSET_TO_INTARR_THRESHOLD; i++) {
                assertFalse(kls.contains(i));
                kls.add(i);
                assertTrue(kls.contains(i)); // intArr removal logic already tested in testArrayLogic()
                assertEquals(1, kls.getSize() - lastSize);
                lastSize = kls.getSize();
            }
            assertEquals("intArr", kls.getCurrentStructure());
            assertEquals(HybridIntKeyLookupStore.HASHSET_TO_INTARR_THRESHOLD, kls.getSize());
            for (int i = kls.getSize(); i < HybridIntKeyLookupStore.INTARR_TO_RBM_THRESHOLD + 1000; i++) {
                kls.add(i);
                assertTrue(kls.contains(i));
            }
            assertEquals("RBM", kls.getCurrentStructure());
            assertEquals(HybridIntKeyLookupStore.INTARR_TO_RBM_THRESHOLD + 1000, kls.getSize());
            for (int i = 5000; i < 10000; i++) {
                kls.forceRemove(i);
                assertFalse(kls.contains(i));
            }
            assertTrue(kls.canHaveFalseNegatives());
        }
    }

    public void testAddingStatsGetters() throws Exception {
        int modulo = (int) Math.pow(2, 15);
        HybridIntKeyLookupStore base_kls = new HybridIntKeyLookupStore(modulo, 0L);
        RemovableHybridIntKeyLookupStore rkls = new RemovableHybridIntKeyLookupStore(modulo, 0L);
        for (HybridIntKeyLookupStore kls : new HybridIntKeyLookupStore[] { base_kls, rkls }) {
            kls.add(15);
            kls.add(-15);
            assertEquals(2, kls.getNumAddAttempts());
            assertEquals(1, kls.getNumCollisions());

            int offset = 1;
            for (int i = 0; i < 10; i++) {
                kls.add(i * modulo + offset);
            }
            assertEquals(12, kls.getNumAddAttempts());
            assertEquals(10, kls.getNumCollisions());
        }
    }

    public void testRegenerateStore() throws Exception {
        HybridIntKeyLookupStore base_kls = new HybridIntKeyLookupStore((int) Math.pow(2, 29), 0L);
        RemovableHybridIntKeyLookupStore rkls = new RemovableHybridIntKeyLookupStore((int) Math.pow(2, 29), 0L);
        for (HybridIntKeyLookupStore kls : new HybridIntKeyLookupStore[] { base_kls, rkls }) {
            Random rand = Randomness.get();
            int[] resetNumbers = new int[] {
                HybridIntKeyLookupStore.HASHSET_TO_INTARR_THRESHOLD,
                HybridIntKeyLookupStore.INTARR_TO_RBM_THRESHOLD,
                HybridIntKeyLookupStore.INTARR_TO_RBM_THRESHOLD + 10000 };
            // test reset starting from each of the 3 internal structure types
            for (int resetNum : resetNumbers) {
                for (int i = 0; i < resetNum; i++) {
                    kls.add(i);
                }
                int[] newVals = new int[(int) (resetNum * 1.1)]; // margin accounts for collisions
                for (int j = 0; j < newVals.length; j++) {
                    newVals[j] = rand.nextInt();
                }
                kls.regenerateStore(newVals);
                assertTrue(kls.getSize() >= resetNum);
                assertTrue(kls.getSize() <= newVals.length);
            }
            // test clear()
            kls.clear();
            assertEquals("HashSet", kls.getCurrentStructure());
            assertEquals(0, kls.getSize());
        }
    }

    public void testAddingDuplicates() throws Exception {
        HybridIntKeyLookupStore base_kls = new HybridIntKeyLookupStore((int) Math.pow(2, 29), 0L);
        RemovableHybridIntKeyLookupStore rkls = new RemovableHybridIntKeyLookupStore((int) Math.pow(2, 29), 0L);
        for (HybridIntKeyLookupStore kls : new HybridIntKeyLookupStore[] { base_kls, rkls }) {
            for (int i = 0; i < HybridIntKeyLookupStore.HASHSET_TO_INTARR_THRESHOLD - 1; i++) {
                kls.add(i);
                kls.add(i);
            }
            for (int j = 0; j < 1000; j++) {
                kls.add(577);
            }
            assertEquals(HybridIntKeyLookupStore.HASHSET_TO_INTARR_THRESHOLD - 1, kls.getSize());
            for (int i = HybridIntKeyLookupStore.HASHSET_TO_INTARR_THRESHOLD - 1; i < HybridIntKeyLookupStore.INTARR_TO_RBM_THRESHOLD
                - 1; i++) {
                kls.add(i);
                kls.add(i);
            }
            for (int j = 0; j < 1000; j++) {
                kls.add(12342);
            }
            assertEquals(HybridIntKeyLookupStore.INTARR_TO_RBM_THRESHOLD - 1, kls.getSize());
            for (int i = HybridIntKeyLookupStore.INTARR_TO_RBM_THRESHOLD - 1; i < HybridIntKeyLookupStore.INTARR_TO_RBM_THRESHOLD
                + 5000; i++) {
                kls.add(i);
                kls.add(i);
            }
            for (int j = 0; j < 1000; j++) {
                kls.add(-10004);
            }
            assertEquals(HybridIntKeyLookupStore.INTARR_TO_RBM_THRESHOLD + 5000, kls.getSize());
        }
    }

    public void testMemoryCapValueInitialization() {
        double[] logModulos = new double[] { 0.0, 31.2, 30, 29, 28, 13 }; // these will decrement by 1
        double[] expectedMultipliers = new double[] { 1.35, 1.35, 1.6, 1.6, 1.6, 1.6 };
        double[] expectedSlopes = new double[] { 0.69, 0.69, 0.75, 0.75, 0.88, 0.88 };
        double[] expectedIntercepts = new double[] { -3, -3, -3.5, -3.5, -4.5, -4.5 };
        long memSizeCapInBytes = (long) 100.0 * HybridIntKeyLookupStore.BYTES_IN_MB;
        double delta = 0.01;
        for (int i = 0; i < logModulos.length; i++) {
            int modulo = 0;
            if (logModulos[i] != 0) {
                modulo = (int) Math.pow(2, logModulos[i]);
            }
            HybridIntKeyLookupStore rbm = new HybridIntKeyLookupStore(modulo, memSizeCapInBytes);
            assertEquals(rbm.memSizeCapInBytes, rbm.getMemorySizeCapInBytes(), 1.0);
            assertEquals(expectedMultipliers[i], rbm.getRBMMemBufferMultiplier(), delta);
            assertEquals(expectedSlopes[i], rbm.getRBMMemSlope(), delta);
            assertEquals(expectedIntercepts[i], rbm.getRBMMemIntercept(), delta);
        }
    }

    public void testMemoryCapBlocksTransitions() throws Exception {
        double[] testModulos = new double[] { 0, Math.pow(2, 31), Math.pow(2, 29), Math.pow(2, 28), Math.pow(2, 26) };
        for (int i = 0; i < testModulos.length; i++) {
            int modulo = (int) testModulos[i];
            long maxHashsetMemSize = HybridIntKeyLookupStore.getHashsetMemSizeInBytes(HybridIntKeyLookupStore.HASHSET_TO_INTARR_THRESHOLD - 1);
            long intArrMemSize = HybridIntKeyLookupStore.getIntArrMemSizeInBytes();
            long minRBMMemSize = HybridIntKeyLookupStore.getRBMMemSizeWithModuloInBytes(HybridIntKeyLookupStore.INTARR_TO_RBM_THRESHOLD, modulo);

            // test that transitions in data structure do indeed monotonically increase predicted memory size
            assertTrue(maxHashsetMemSize < intArrMemSize);
            assertTrue(intArrMemSize < minRBMMemSize);

            HybridIntKeyLookupStore kls = new HybridIntKeyLookupStore(modulo, intArrMemSize - 1000);
            for (int j = 0; j < HybridIntKeyLookupStore.HASHSET_TO_INTARR_THRESHOLD - 1; j++) {
                boolean didAdd = kls.add(j);
                assertTrue(didAdd);
            }
            // now try to add one more, which would cause a transition and push us past the memory cap
            assertFalse(kls.isAtCapacity());
            assertEquals("HashSet", kls.getCurrentStructure());
            boolean didAdd = kls.add(HybridIntKeyLookupStore.HASHSET_TO_INTARR_THRESHOLD - 1);
            assertFalse(didAdd);
            assertTrue(kls.isAtCapacity());
            assertEquals("HashSet", kls.getCurrentStructure());

            kls = new HybridIntKeyLookupStore(modulo, minRBMMemSize);
            for (int j = 0; j < HybridIntKeyLookupStore.INTARR_TO_RBM_THRESHOLD - 1; j++) {
                didAdd = kls.add(j);
                assertTrue(didAdd);
            }
            assertFalse(kls.isAtCapacity());
            didAdd = kls.add(HybridIntKeyLookupStore.INTARR_TO_RBM_THRESHOLD);
            assertFalse(didAdd);
            assertTrue(kls.isAtCapacity());
            assertEquals("intArr", kls.getCurrentStructure());
        }
    }

    public void testMemoryCapBlocksAdd() throws Exception {
        double[] testModulos = new double[] { 0, Math.pow(2, 31), Math.pow(2, 29), Math.pow(2, 28), Math.pow(2, 26) };
        for (int i = 0; i < testModulos.length; i++) {
            int modulo = (int) testModulos[i];

            // test where max number of entries should be 3000
            long memSizeCapInBytes = (long) (HybridIntKeyLookupStore.HASHSET_MEM_SLOPE * 3000 * HybridIntKeyLookupStore.BYTES_IN_MB);
            HybridIntKeyLookupStore kls = new HybridIntKeyLookupStore(modulo, memSizeCapInBytes);
            for (int j = 0; j < 3500; j++) {
                kls.add(j);
            }
            assertTrue(Math.abs(3000 - kls.getSize()) < 2); // double --> long conversion adds a bit of lossiness
            assertEquals("HashSet", kls.getCurrentStructure());

            // test where max number of entries should be 999,999 (bounded at intArr size)
            memSizeCapInBytes = HybridIntKeyLookupStore.getIntArrMemSizeInBytes();
            kls = new HybridIntKeyLookupStore(modulo, memSizeCapInBytes);
            for (int j = 0; j < 105000; j++) {
                kls.add(j);
            }
            assertEquals(HybridIntKeyLookupStore.INTARR_TO_RBM_THRESHOLD - 1, kls.getSize());
            assertEquals("intArr", kls.getCurrentStructure());

            int maxEntries = 2342000;
            memSizeCapInBytes = HybridIntKeyLookupStore.getRBMMemSizeWithModuloInBytes(maxEntries, modulo);
            kls = new HybridIntKeyLookupStore(modulo, memSizeCapInBytes);
            for (int j = 0; j < maxEntries + 1000; j++) {
                kls.add(j);
            }
            assertTrue(Math.abs(maxEntries - kls.getSize()) < 5); // exact cap varies a small amount bc of floating point
        }
    }

    public void testConcurrency() throws Exception {
        Random rand = Randomness.get();
        for (int j = 0; j < 5; j++) { // test with different numbers of threads
            HybridIntKeyLookupStore base_kls = new HybridIntKeyLookupStore((int) Math.pow(2, 29), 0L);
            RemovableHybridIntKeyLookupStore rkls = new RemovableHybridIntKeyLookupStore((int) Math.pow(2, 29), 0L);
            for (HybridIntKeyLookupStore kls : new HybridIntKeyLookupStore[] { base_kls, rkls }) {
                int numThreads = rand.nextInt(50) + 1;
                ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(numThreads);
                // In this test we want to add the first 200K numbers and check they're all correctly there.
                // We do some duplicates too to ensure those aren't incorrectly added.
                int amountToAdd = 200000;
                ArrayList<Future<Boolean>> wasAdded = new ArrayList<>(amountToAdd); // idk why i cant make an array???
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
}
