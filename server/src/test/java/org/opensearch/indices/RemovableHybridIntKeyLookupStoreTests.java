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

package org.opensearch.indices;

import org.opensearch.test.OpenSearchTestCase;

// NOTE: Only new functionality is tested here.
// Inherited functionality is tested for both this class and the superclass in HybridIntKeyLookupStoreTests.java.

public class RemovableHybridIntKeyLookupStoreTests extends OpenSearchTestCase {
    public void testRemoveNoCollisions() throws Exception {
        long memCap = 100L * HybridIntKeyLookupStore.BYTES_IN_MB;
        RemovableHybridIntKeyLookupStore rkls = new RemovableHybridIntKeyLookupStore(0, memCap);
        // there should be no collisions for sequential positive numbers up to modulo
        assertTrue(rkls.supportsRemoval());
        for (int i = 0; i < HybridIntKeyLookupStore.HASHSET_TO_INTARR_THRESHOLD - 1; i++) {
            rkls.add(i);
        }
        assertEquals("HashSet", rkls.getCurrentStructure());
        for (int i = 0; i < 1000; i++) {
            assertTrue(rkls.remove(i));
            assertFalse(rkls.contains(i));
            assertFalse(rkls.valueHasHadCollision(i));
        }
        assertEquals(HybridIntKeyLookupStore.HASHSET_TO_INTARR_THRESHOLD - 1001, rkls.getSize());
        for (int i = 0; i < 1000; i++) {
            rkls.add(i);
            assertFalse(rkls.valueHasHadCollision(i));
        }

        assertEquals(HybridIntKeyLookupStore.HASHSET_TO_INTARR_THRESHOLD - 1, rkls.getSize());
        for (int i = HybridIntKeyLookupStore.HASHSET_TO_INTARR_THRESHOLD - 1; i < HybridIntKeyLookupStore.INTARR_TO_RBM_THRESHOLD
            - 1; i++) {
            rkls.add(i);
        }
        assertEquals("intArr", rkls.getCurrentStructure());
        assertEquals(HybridIntKeyLookupStore.INTARR_TO_RBM_THRESHOLD - 1, rkls.getSize());
        for (int i = 0; i < 1000; i++) {
            assertTrue(rkls.remove(i));
            assertFalse(rkls.contains(i));
            assertFalse(rkls.valueHasHadCollision(i));
        }
        for (int i = 0; i < 1000; i++) {
            rkls.add(i);
            assertFalse(rkls.valueHasHadCollision(i));
        }

        for (int i = HybridIntKeyLookupStore.INTARR_TO_RBM_THRESHOLD - 1; i < HybridIntKeyLookupStore.INTARR_TO_RBM_THRESHOLD + 1000; i++) {
            rkls.add(i);
        }
        assertEquals("RBM", rkls.getCurrentStructure());
        assertEquals(HybridIntKeyLookupStore.INTARR_TO_RBM_THRESHOLD + 1000, rkls.getSize());
        for (int i = 0; i < HybridIntKeyLookupStore.INTARR_TO_RBM_THRESHOLD + 1000; i++) {
            assertTrue(rkls.remove(i));
            assertFalse(rkls.contains(i));
            assertFalse(rkls.valueHasHadCollision(i));
        }
        assertEquals("RBM", rkls.getCurrentStructure());
        assertEquals(0, rkls.getSize());
    }

    public void testRemoveWithCollisions() throws Exception {
        int modulo = (int) Math.pow(2, 26);
        long memCap = 100L * HybridIntKeyLookupStore.BYTES_IN_MB;
        RemovableHybridIntKeyLookupStore rkls = new RemovableHybridIntKeyLookupStore(modulo, memCap);
        for (int i = 0; i < 10; i++) {
            rkls.add(i);
            if (i % 2 == 0) {
                rkls.add(-i);
                assertTrue(rkls.valueHasHadCollision(i));
            } else {
                assertFalse(rkls.valueHasHadCollision(i));
            }
        }
        assertEquals(10, rkls.getSize());
        for (int i = 0; i < 10; i++) {
            boolean didRemove = rkls.remove(i);
            if (i % 2 == 0) {
                // we expect a collision with -i, so we can't remove
                assertFalse(didRemove);
                assertTrue(rkls.contains(i));
            } else {
                // we expect no collision
                assertTrue(didRemove);
                assertFalse(rkls.contains(i));
                assertFalse(rkls.valueHasHadCollision(i));
            }
        }
        assertEquals(5, rkls.getSize());
        rkls.add(1);
        for (int j = 1; j < 5; j++) {
            rkls.add(1 + j * modulo);
        }
        assertEquals(6, rkls.getSize());
        assertFalse(rkls.remove(1 + modulo));
        assertTrue(rkls.valueHasHadCollision(1 + 15 * modulo));
        assertTrue(rkls.contains(1 + 17 * modulo));
    }
}
