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

import java.util.Random;

public class HybridIntKeyLookupStoreTests extends org.apache.lucene.util.LuceneTestCase {
    public void testInit() throws Exception {
        HybridIntKeyLookupStore kls = new HybridIntKeyLookupStore((int) Math.pow(2, 29));
        assertEquals("HashSet", kls.getCurrentStructure());
        assertEquals(0, kls.getSize());
    }

    public void testStructureTransitions() throws Exception {
        HybridIntKeyLookupStore kls = new HybridIntKeyLookupStore((int) Math.pow(2, 29));
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

    public void testArrayLogic() throws Exception {
        HybridIntKeyLookupStore kls = new HybridIntKeyLookupStore((int) Math.pow(2, 29));
        Random rand = new Random();
        int numToAdd = 50000;
        int[] addedValues = new int[numToAdd];
        for (int i = 0; i < numToAdd; i++) {
            int val = rand.nextInt();
            kls.add(val);
            addedValues[i] = val;
        }
        System.out.println(kls.getSize());
        assertTrue(kls.arrayCorrectlySorted()); // Not sure if this is really good as a public method - but idk how else to do it?
        assertTrue(numToAdd - kls.getSize() < 20); // size should not be too different from numToAdd - exact number varies due to collisions
        int numToRemove = 20000;
        for (int j = 0; j < numToRemove; j++) {
            kls.forceRemove(addedValues[j]);
        }
        System.out.println(kls.getSize());
        assertTrue(numToAdd - numToRemove - kls.getSize() < 20);
        assertTrue(kls.arrayCorrectlySorted());
        assertTrue(kls.canHaveFalseNegatives());
    }

    public void testTransformationLogic() throws Exception {
        int modulo = (int) Math.pow(2, 29);
        HybridIntKeyLookupStore kls = new HybridIntKeyLookupStore((int) modulo);
        int offset = 3;
        for (int i = 0; i < 4; i++) { // after this we run into max value, but thats not a flaw with the class design
            int posValue = i * modulo + offset;
            kls.add(posValue);
            int negValue = -(i * modulo + offset);
            kls.add(negValue);
        }
        assertEquals(1, kls.getSize());

        // test output is always in expected range
        int[] testVals = new int[]{0, 1, -1, -23495, 23058, modulo, -modulo, Integer.MAX_VALUE, Integer.MIN_VALUE};
        for (int value: testVals) {
            assertTrue(kls.getInternalRepresentation(value) <= 0);
            assertTrue(kls.getInternalRepresentation(value) > -modulo);
        }
    }

    public void testContainsAndForceRemove() throws Exception {
        HybridIntKeyLookupStore kls = new HybridIntKeyLookupStore((int) Math.pow(2, 29));
        for (int i = 0; i < 2000; i++) {
            kls.add(i);
            assertTrue(kls.contains(i));
        }
        assertFalse(kls.canHaveFalseNegatives());
        for (int i = 1900; i < 2000; i++) {
            kls.forceRemove(i);
            assertFalse(kls.contains(i));
        }
        System.out.println(kls.contains(1901));
        assertEquals(1900, kls.getSize());
        int lastSize = kls.getSize();
        for (int i = kls.getSize(); i < HybridIntKeyLookupStore.HASHSET_TO_INTARR_THRESHOLD; i++) {
            assertFalse(kls.contains(i));
            kls.add(i);
            assertTrue(kls.contains(i)); // intArr removal logic already tested in testArrayLogic()
            assertEquals(1, kls.getSize() - lastSize);
            lastSize = kls.getSize();
        }
        System.out.println(kls.getSize());
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

    public void testAddingStatsGetters() throws Exception {
        int modulo = (int) Math.pow(2, 15);
        HybridIntKeyLookupStore kls = new HybridIntKeyLookupStore(modulo);
        assertEquals(modulo, kls.getModulo());

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

    public void testRegenerateStore() throws Exception {
        HybridIntKeyLookupStore kls = new HybridIntKeyLookupStore((int) Math.pow(2, 29));
        Random rand = new Random();
        int[] resetNumbers =  new int[]{HybridIntKeyLookupStore.HASHSET_TO_INTARR_THRESHOLD, HybridIntKeyLookupStore.INTARR_TO_RBM_THRESHOLD, HybridIntKeyLookupStore.INTARR_TO_RBM_THRESHOLD+10000};
        // test reset starting from each of the 3 internal structure types
        for (int resetNum: resetNumbers) {
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
    }

    public void testAddingDuplicates() throws Exception {
        HybridIntKeyLookupStore kls = new HybridIntKeyLookupStore((int) Math.pow(2, 29));
        for (int i = 0; i < HybridIntKeyLookupStore.HASHSET_TO_INTARR_THRESHOLD - 1; i++) {
            kls.add(i);
            kls.add(i);
        }
        for (int j = 0; j < 1000; j++) {
            kls.add(577);
        }
        assertEquals(HybridIntKeyLookupStore.HASHSET_TO_INTARR_THRESHOLD - 1, kls.getSize());
        for (int i = HybridIntKeyLookupStore.HASHSET_TO_INTARR_THRESHOLD - 1; i < HybridIntKeyLookupStore.INTARR_TO_RBM_THRESHOLD - 1; i++) {
            kls.add(i);
            kls.add(i);
        }
        for (int j = 0; j < 1000; j++) {
            kls.add(12342);
        }
        assertEquals(HybridIntKeyLookupStore.INTARR_TO_RBM_THRESHOLD - 1, kls.getSize());
        for (int i = HybridIntKeyLookupStore.INTARR_TO_RBM_THRESHOLD - 1; i < HybridIntKeyLookupStore.INTARR_TO_RBM_THRESHOLD + 5000; i++) {
            kls.add(i);
            kls.add(i);
        }
        for (int j = 0; j < 1000; j++) {
            kls.add(-10004);
        }
        assertEquals(HybridIntKeyLookupStore.INTARR_TO_RBM_THRESHOLD + 5000, kls.getSize());
    }
}
