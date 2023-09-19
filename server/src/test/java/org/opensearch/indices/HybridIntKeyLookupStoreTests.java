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
        assert kls.getCurrentStructure().equals("HashSet");
        assert kls.getSize() == 0;
    }

    public void testStructureTransitions() throws Exception {
        HybridIntKeyLookupStore kls = new HybridIntKeyLookupStore((int) Math.pow(2, 29));
        for (int i = 0; i < HybridIntKeyLookupStore.HASHSET_TO_INTARR_THRESHOLD; i++) {
        //for (int i = 0; i < 5001; i++) {
            kls.add(i);
        }
        System.out.println("Size: " + kls.getSize());
        assert (kls.getCurrentStructure().equals("intArr"));
        assert kls.getSize() == HybridIntKeyLookupStore.HASHSET_TO_INTARR_THRESHOLD;
        for (int i = HybridIntKeyLookupStore.HASHSET_TO_INTARR_THRESHOLD; i < HybridIntKeyLookupStore.INTARR_TO_RBM_THRESHOLD; i++) {
            kls.add(i);
        }
        assert (kls.getCurrentStructure().equals("RBM"));
        System.out.println(kls.getSize());
        assert kls.getSize() == HybridIntKeyLookupStore.INTARR_TO_RBM_THRESHOLD;
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
        assert kls.arrayCorrectlySorted(); // Not sure if this is really good as a public method - but idk how else to do it?
        assert (numToAdd - kls.getSize() < 20); // size should not be too different from numToAdd - exact number varies due to collisions
        int numToRemove = 20000;
        for (int j = 0; j < numToRemove; j++) {
            kls.forceRemove(addedValues[j]);
        }
        System.out.println(kls.getSize());
        assert (numToAdd - numToRemove - kls.getSize() < 20);
        assert kls.arrayCorrectlySorted();
        assert kls.canHaveFalseNegatives();
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
        assert kls.getSize() == 1;

        // test output is always in expected range
        int[] testVals = new int[]{0, 1, -1, -23495, 23058, modulo, -modulo, Integer.MAX_VALUE, Integer.MIN_VALUE};
        for (int value: testVals) {
            System.out.println(kls.getInternalRepresentation(value));
            assert kls.getInternalRepresentation(value) <= 0;
            assert kls.getInternalRepresentation(value) > -modulo;
        }
    }

    public void testContainsAndForceRemove() throws Exception {
        HybridIntKeyLookupStore kls = new HybridIntKeyLookupStore((int) Math.pow(2, 29));
        for (int i = 0; i < 2000; i++) {
            kls.add(i);
            assert kls.contains(i);
        }
        assert !kls.canHaveFalseNegatives();
        for (int i = 1900; i < 2000; i++) {
            kls.forceRemove(i);
            //System.out.println("in loop " + kls.contains(i));
            assert !kls.contains(i);
        }
        System.out.println(kls.contains(1901));
        assert kls.getSize() == 1900;
        int lastSize = kls.getSize();
        for (int i = kls.getSize(); i < HybridIntKeyLookupStore.HASHSET_TO_INTARR_THRESHOLD; i++) {
            assert !kls.contains(i);
            kls.add(i);
            assert kls.contains(i); // intArr removal logic already tested in testArrayLogic()
            assert kls.getSize() - lastSize == 1;
            lastSize = kls.getSize();
        }
        System.out.println(kls.getSize());
        assert kls.getCurrentStructure().equals("intArr");
        assert kls.getSize() == HybridIntKeyLookupStore.HASHSET_TO_INTARR_THRESHOLD;
        for (int i = kls.getSize(); i < HybridIntKeyLookupStore.INTARR_TO_RBM_THRESHOLD + 1000; i++) {
            kls.add(i);
            assert kls.contains(i);
        }
        assert kls.getCurrentStructure().equals("RBM");
        assert kls.getSize() == HybridIntKeyLookupStore.INTARR_TO_RBM_THRESHOLD + 1000;
        for (int i = 5000; i < 10000; i++) {
            kls.forceRemove(i);
            assert !kls.contains(i);
        }
        assert kls.canHaveFalseNegatives();
    }

    public void testAddingStatsGetters() throws Exception {
        int modulo = (int) Math.pow(2, 15);
        HybridIntKeyLookupStore kls = new HybridIntKeyLookupStore(modulo);
        assert kls.getModulo() == modulo;

        kls.add(15);
        kls.add(-15);
        assert kls.getNumAddAttempts() == 2;
        assert kls.getNumCollisions() == 1;

        int offset = 1;
        for (int i = 0; i < 10; i++) {
            kls.add(i * modulo + offset);
        }
        assert kls.getNumAddAttempts() == 12;
        assert kls.getNumCollisions() == 10;
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
            assert kls.getSize() >= resetNum;
            assert kls.getSize() <= newVals.length;
        }
    }
}
