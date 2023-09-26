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

import org.roaringbitmap.RoaringBitmap;
import java.util.HashSet;

public class RemovableRBMIntKeyLookupStore extends RBMIntKeyLookupStore implements KeyLookupStore<Integer> {
    // The code for this class is almost the same as RemovableHybridIntKeyLookupStore,
    // just with different superclasses.
    // I considered changing the separate Removable classes into a CollisionHandler object
    // which could be reused as a field of the KeyLookupStore objects, but since we will ultimately
    // only use one of these four possible classes after doing performance testing,
    // I don't think it's worth it to make the logic more complex just to avoid reusing code that might be deleted.

    private HashSet<Integer> collidedInts;
    private int numRemovalAttempts;
    private int numSuccessfulRemovals;

    RemovableRBMIntKeyLookupStore(int modulo, long memSizeCapInBytes) {
        super(modulo, memSizeCapInBytes);
        collidedInts = new HashSet<>();
        numRemovalAttempts = 0;
        numSuccessfulRemovals = 0;
    }

    @Override
    protected void handleCollisions(int transformedValue) {
        numCollisions++;
        collidedInts.add(transformedValue);
    }

    @Override
    public boolean supportsRemoval() {
        return true;
    }

    // Check if the value to remove has had a collision, and if not, remove it
    @Override
    public boolean remove(Integer value) throws Exception {
        if (value == null) {
            return false;
        }
        int transformedValue = transform(value);
        readLock.lock();
        try {
            if (!contains(value)) {
                return false;
            }
            numRemovalAttempts++;
            if (collidedInts.contains(transformedValue)) {
                return false;
            }
        } finally {
            readLock.unlock();
        }
        writeLock.lock();
        try {
            rbm.remove(transformedValue);
            size--;
            numSuccessfulRemovals++;
            return true;
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public long getMemorySizeInBytes() {
        return super.getMemorySizeInBytes() + RBMSizeEstimator.getHashsetMemSizeInBytes(collidedInts.size());
    }

    @Override
    public void regenerateStore(Integer[] newValues) throws Exception {
        collidedInts = new HashSet<>();
        super.regenerateStore(newValues);
    }

    public int getNumRemovalAttempts() {
        return numRemovalAttempts;
    }

    public int getNumSuccessfulRemovals() {
        return numSuccessfulRemovals;
    }

    public boolean valueHasHadCollision(Integer value) {
        if (value == null) {
            return false;
        }
        return collidedInts.contains(transform(value));
    }
}
