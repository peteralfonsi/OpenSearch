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

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class RBMIntKeyLookupStore implements KeyLookupStore<Integer> {
    // This class shares a lot of the same fields with HybridIntKeyLookupStore, but basically none of the logic
    // besides getters, so I decided against making it the superclass to HybridIntKeyLookupStore
    protected final int modulo;
    protected int size;
    protected long memSizeCapInBytes;
    protected int numAddAttempts;
    protected int numCollisions;
    protected boolean guaranteesNoFalseNegatives;
    protected RoaringBitmap rbm;
    protected final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    protected final Lock readLock = lock.readLock();
    protected final Lock writeLock = lock.writeLock();
    protected int maxNumEntries;
    protected boolean atCapacity;

    RBMIntKeyLookupStore(int modulo, long memSizeCapInBytes) {
        this.modulo = modulo;
        this.size = 0;
        this.numAddAttempts = 0;
        this.numCollisions = 0;
        this.guaranteesNoFalseNegatives = true;
        this.memSizeCapInBytes = memSizeCapInBytes; // A cap of 0 means no cap
        this.maxNumEntries = calculateMaxNumEntries();
        this.rbm = new RoaringBitmap();
    }

    protected int calculateMaxNumEntries() {
        if (memSizeCapInBytes == 0) {
            return Integer.MAX_VALUE;
        }
        return RBMSizeEstimator.getNumEntriesFromSizeInBytes(memSizeCapInBytes);
    }

    protected final int transform(int value) {
        return modulo == 0 ? value : value % modulo;
    }

    protected void handleCollisions(int transformedValue) {
        numCollisions++;
    }

    @Override
    public boolean add(Integer value) throws Exception {
        if (value == null) {
            return false;
        }
        writeLock.lock();
        numAddAttempts++;
        try {
            if (size == maxNumEntries) {
                atCapacity = true;
                return false;
            }
            int transformedValue = transform(value);
            boolean alreadyContained = contains(transformedValue);
            if (!alreadyContained) {
                rbm.add(transformedValue);
                size++;
                return true;
            }
            handleCollisions(transformedValue);
            return false;
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public boolean contains(Integer value) throws Exception {
        if (value == null) {
            return false;
        }
        int transformedValue = transform(value);
        readLock.lock();
        try {
            return rbm.contains(transformedValue);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public Integer getInternalRepresentation(Integer value) {
        if (value == null) {
            return 0;
        }
        return Integer.valueOf(transform(value));
    }

    @Override
    public boolean remove(Integer value) throws Exception {
        return false;
    }

    @Override
    public boolean supportsRemoval() {
        return false;
    }

    @Override
    public void forceRemove(Integer value) throws Exception {
        if (value == null) {
            return;
        }
        writeLock.lock();
        guaranteesNoFalseNegatives = false;
        try {
            int transformedValue = transform(value);
            rbm.remove(transformedValue);
            size--;
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public boolean canHaveFalseNegatives() {
        return !guaranteesNoFalseNegatives;
    }

    @Override
    public int getSize() {
        readLock.lock();
        try {
            return size;
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public int getNumAddAttempts() {
        return numAddAttempts;
    }

    @Override
    public int getNumCollisions() {
        return numCollisions;
    }

    @Override
    public String getCurrentStructure() throws Exception {
        return "RBM";
    }

    @Override
    public boolean isCollision(Integer value1, Integer value2) {
        if (value1 == null || value2 == null) {
            return false;
        }
        return transform(value1) == transform(value2);
    }

    @Override
    public long getMemorySizeInBytes() {
        return RBMSizeEstimator.getSizeInBytes(size);
    }

    @Override
    public long getMemorySizeCapInBytes() {
        return memSizeCapInBytes;
    }

    @Override
    public boolean isAtCapacity() {
        return atCapacity;
    }

    @Override
    public void regenerateStore(Integer[] newValues) throws Exception {
        rbm.clear();
        size = 0;
        this.numAddAttempts = 0;
        this.numCollisions = 0;
        this.guaranteesNoFalseNegatives = true;
        for (int i = 0; i < newValues.length; i++) {
            if (newValues[i] != null) {
                add(newValues[i]);
            }
        }
    }

    @Override
    public void clear() throws Exception {
        regenerateStore(new Integer[]{});
    }
}
