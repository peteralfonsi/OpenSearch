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

import java.util.HashSet;

/**
 * A store which supports safe removal of keys by maintaining a hashset of values that have had collisions.
 */
public class RemovableHybridIntKeyLookupStore extends HybridIntKeyLookupStore implements KeyLookupStore<Integer> {
    private HashSet<Integer> collidedInts;

    RemovableHybridIntKeyLookupStore(int modulo, long memSizeCapInBytes) {
        super(modulo, memSizeCapInBytes);
        collidedInts = new HashSet<>();
    }

    @Override
    protected void handleCollisions(int transformedValue) {
        stats.numCollisions.inc();
        collidedInts.add(transformedValue);
    }


    // Check if the value to remove has had a collision, and if not, remove it
    @Override
    public boolean remove(Integer value) throws IllegalStateException {
        if (value == null) {
            return false;
        }
        int transformedValue = transform(value);
        readLock.lock();
        try {
            if (!contains(value)) {
                return false;
            }
            stats.numRemovalAttempts.inc();
            if (collidedInts.contains(transformedValue)) {
                return false;
            }
        } finally {
            readLock.unlock();
        }
        writeLock.lock();
        try {
            removeHelperFunction(transformedValue);
            stats.numSuccessfulRemovals.inc();
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
    public void regenerateStore(Integer[] newValues) throws IllegalStateException {
        collidedInts = new HashSet<>();
        super.regenerateStore(newValues);
    }

    public int getNumRemovalAttempts() {
        return (int) stats.numRemovalAttempts.count();
    }

    public int getNumSuccessfulRemovals() {
        return (int) stats.numSuccessfulRemovals.count();
    }

    public boolean valueHasHadCollision(Integer value) {
        if (value == null) {
            return false;
        }
        return collidedInts.contains(transform(value));
    }

}
