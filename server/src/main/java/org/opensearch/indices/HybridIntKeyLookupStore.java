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

import java.util.Arrays;
import java.util.HashSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.opensearch.common.metrics.CounterMetric;
import org.roaringbitmap.RoaringBitmap;
import org.w3c.dom.css.Counter;

/**
 * A store which dynamically switches its internal data structure from hash set to sorted int array
 * to roaring bitmap.
 */
public class HybridIntKeyLookupStore implements KeyLookupStore<Integer> {
    public static final int HASHSET_TO_INTARR_THRESHOLD = 5000;
    public static final int INTARR_SIZE = 100000;
    public static final int INTARR_TO_RBM_THRESHOLD = INTARR_SIZE;

    /**
     * Used to keep track of which structure is being used to store values.
     */
    public enum StructureTypes {
        HASHSET,
        INTARR,
        RBM
    }

    protected class KeyStoreStats {
        protected int size;
        protected long memSizeCapInBytes;
        protected CounterMetric numAddAttempts;
        protected CounterMetric numCollisions;
        protected boolean guaranteesNoFalseNegatives;
        protected int maxNumEntries;
        protected boolean atCapacity;
        protected CounterMetric numRemovalAttempts; // used in removable classes
        protected CounterMetric numSuccessfulRemovals;
        protected KeyStoreStats(long memSizeCapInBytes, int maxNumEntries) {
            this.size = 0;
            this.numAddAttempts = new CounterMetric();
            this.numCollisions = new CounterMetric();
            this.guaranteesNoFalseNegatives = true;
            this.memSizeCapInBytes = memSizeCapInBytes;
            this.maxNumEntries = maxNumEntries;
            this.atCapacity = false;
            this.numRemovalAttempts = new CounterMetric();
            this.numSuccessfulRemovals = new CounterMetric();
        }
    }

    protected KeyStoreStats stats;
    protected StructureTypes currentStructure;
    protected final int modulo;


    protected HashSet<Integer> hashset;
    protected int[] intArr;
    protected RoaringBitmap rbm;
    protected final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    protected final Lock readLock = lock.readLock();
    protected final Lock writeLock = lock.writeLock();

    public HybridIntKeyLookupStore(int modulo, long memSizeCapInBytes) {
        this.modulo = modulo; // A modulo of 0 means no modulo
        this.hashset = new HashSet<Integer>();
        this.currentStructure = StructureTypes.HASHSET;
        this.stats = new KeyStoreStats(memSizeCapInBytes, calculateMaxNumEntries(memSizeCapInBytes));
    }

    protected final int customAbs(int value) {
        if (value < 0 && value > Integer.MIN_VALUE) {
            return -value;
        } else if (value >= 0) {
            return value;
        }
        return Integer.MAX_VALUE;
    }

    protected final int transform(int value) {
        // We only use negative numbers to simplify sorting the int array
        return modulo == 0 ? -customAbs(value) : -customAbs(value % modulo);
    }

    // Helper function for intArr operations
    protected void intArrChecks(int value) throws IllegalStateException {
        if (currentStructure != StructureTypes.INTARR) {
            throw new IllegalStateException("Cannot run isInIntArr when currentStructure is not INTARR!!");
        }
        if (value > 0) {
            throw new IllegalStateException("Cannot use positive value " + Integer.toString(value) + " in isInIntArr");
        }
    }

    /** Checks for presence of value in intArr. If doAdd is true and the value is not already there, adds it.
     * Returns true if the value was already contained (and therefore not added again), false otherwise
     */
    protected final boolean isInIntArr(int value, int arrSize, boolean doAdd) throws IllegalStateException {
        Lock lock = doAdd ? writeLock : readLock;
        lock.lock();
        try {
            intArrChecks(value);
            int index = Arrays.binarySearch(intArr, 0, arrSize, value); // only search in initialized part of array
            if (index < 0) {
                if (doAdd) {
                    int insertionPoint = -index - 1;
                    System.arraycopy(intArr, insertionPoint, intArr, insertionPoint + 1, arrSize - insertionPoint);
                    intArr[insertionPoint] = value;
                }
                return false;
            }
            return true;
        } finally {
            lock.unlock();
        }
    }

    protected final void switchHashsetToIntArr() throws IllegalStateException {
        writeLock.lock();
        try {
            if (currentStructure == StructureTypes.HASHSET) {
                stats.size = 0;
                intArr = new int[INTARR_SIZE];
                currentStructure = StructureTypes.INTARR;
                for (int value : hashset) {
                    boolean alreadyContained = isInIntArr(value, stats.size, true);
                    // should never be already contained, but just to be safe
                    if (!alreadyContained) {
                        stats.size++;
                    }
                }
                hashset = null;
            }
        } finally {
            writeLock.unlock();
        }
    }

    protected final void switchIntArrToRBM() {
        writeLock.lock();
        try {
            if (currentStructure == StructureTypes.INTARR) {
                currentStructure = StructureTypes.RBM;
                rbm = new RoaringBitmap();
                for (int i = 0; i < stats.size; i++) {
                    rbm.add(intArr[i]);
                }
                intArr = null;
            }
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Checks if adding an additional value would require us to change data structures.
     * If so, start that change.
     */
    protected final void handleStructureSwitch() throws IllegalStateException { // write lock?
        writeLock.lock();
        try {
            if (stats.size == HASHSET_TO_INTARR_THRESHOLD - 1) {
                if (stats.maxNumEntries <= HASHSET_TO_INTARR_THRESHOLD) {
                    stats.atCapacity = true;
                    return;
                }
                switchHashsetToIntArr();
            } else if (stats.size == INTARR_TO_RBM_THRESHOLD - 1) {
                if (stats.maxNumEntries <= INTARR_TO_RBM_THRESHOLD) {
                    stats.atCapacity = true;
                    return;
                }
                switchIntArrToRBM();
            }
        } finally {
            writeLock.unlock();
        }
    }

    protected final void removeFromIntArr(int value) throws IllegalStateException {
        writeLock.lock();
        try {
            intArrChecks(value);
            int index = Arrays.binarySearch(intArr, 0, stats.size, value);
            if (index >= 0) {
                System.arraycopy(intArr, index + 1, intArr, index, stats.size - index - 1);
                intArr[stats.size - 1] = 0;
                stats.size--;
            }
        } finally {
            writeLock.unlock();
        }
    }

    protected void handleCollisions(int transformedValue) {
        stats.numCollisions.inc();
    }

    @Override
    public boolean add(Integer value) throws IllegalStateException {
        if (value == null) {
            return false;
        }
        writeLock.lock();
        try {
            if (stats.size == stats.maxNumEntries) {
                stats.atCapacity = true;
            }
            handleStructureSwitch(); // also might set atCapacity
            if (!stats.atCapacity) {

                stats.numAddAttempts.inc();
                int transformedValue = transform(value);
                boolean alreadyContained;

                switch (currentStructure) {
                    case HASHSET:
                        alreadyContained = !(hashset.add(transformedValue));
                        break;
                    case INTARR:
                        alreadyContained = isInIntArr(transformedValue, stats.size, true);
                        break;
                    case RBM:
                        alreadyContained = containsTransformed(transformedValue);
                        if (!alreadyContained) {
                            rbm.add(transformedValue);
                        }
                        break;
                    default:
                        throw new IllegalStateException("currentStructure is none of possible values");
                }
                if (alreadyContained) {
                    handleCollisions(transformedValue);
                    return false;
                }
                stats.size++;
                return true;
            }
            return false;
        } finally {
            writeLock.unlock();
        }
    }

    protected boolean containsTransformed(int transformedValue) throws IllegalStateException {
        readLock.lock();
        try {
            switch (currentStructure) {
                case HASHSET:
                    return hashset.contains(transformedValue);
                case INTARR:
                    return isInIntArr(transformedValue, stats.size, false);
                case RBM:
                    return rbm.contains(transformedValue);
                default:
                    throw new IllegalStateException("currentStructure is none of possible values");
            }
        } finally {
            readLock.unlock();
        }
    }

    // Check the array is sorted with no duplicate elements (except 0)
    protected boolean arrayCorrectlySorted() {
        readLock.lock();
        try {
            if (currentStructure == StructureTypes.INTARR) {
                for (int j = 0; j < intArr.length - 1; j++) {
                    if (!((intArr[j] < intArr[j + 1]) || (intArr[j] == intArr[j + 1] && intArr[j + 1] == 0))) {
                        // left clause: check that array is sorted, right clause: check that values are unique unless they're zero
                        // (uninitialized)
                        return false;
                    }
                }
            }
            return true;
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public boolean contains(Integer value) throws IllegalStateException {
        if (value == null) {
            return false;
        }
        int transformedValue = transform(value);
        return containsTransformed(transformedValue);
    }

    @Override
    public Integer getInternalRepresentation(Integer value) {
        if (value == null) {
            return 0;
        }
        return Integer.valueOf(transform(value));
    }

    @Override
    public boolean remove(Integer value) throws IllegalStateException {
        return false;
    }


    protected void removeHelperFunction(int transformedValue) throws IllegalStateException {
        // allows code to be reused in forceRemove() of this class and remove() of inheriting class
        // shouldn't be called on its own, or on a value that's not already inside the structure
        switch (currentStructure) {
            case HASHSET:
                hashset.remove(transformedValue);
                stats.size--;
                return;
            case INTARR:
                removeFromIntArr(transformedValue); // size is decreased in this function already
                return;
            case RBM:
                rbm.remove(transformedValue);
                stats.size--;
        }
    }

    @Override
    public void forceRemove(Integer value) throws IllegalStateException {
        if (value == null) {
            return;
        }
        writeLock.lock();
        stats.guaranteesNoFalseNegatives = false;
        try {
            int transformedValue = transform(value);
            boolean alreadyContained = contains(transformedValue);
            if (alreadyContained) {
                removeHelperFunction(transformedValue);
            }
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public boolean canHaveFalseNegatives() {
        return !stats.guaranteesNoFalseNegatives;
    }

    @Override
    public int getSize() {
        readLock.lock(); // needed because size is changed during switchHashsetToIntarr()
        try {
            return stats.size;
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public int getTotalAdds() {
        return (int) stats.numAddAttempts.count();
    }

    @Override
    public int getCollisions() {
        return (int) stats.numCollisions.count();
    }


    public StructureTypes getCurrentStructure() throws IllegalStateException {
        return currentStructure;
    }

    @Override
    public boolean isCollision(Integer value1, Integer value2) {
        if (value1 == null || value2 == null) {
            return false;
        }
        return transform(value1) == transform(value2);
    }

    protected int calculateMaxNumEntries(long memSizeCapInBytes) {
        double maxHashsetMemSize = RBMSizeEstimator.getHashsetMemSizeInBytes(HASHSET_TO_INTARR_THRESHOLD - 1);
        double intArrMemSize = getIntArrMemSizeInBytes();
        double minRBMMemSize = RBMSizeEstimator.getSizeInBytes(INTARR_TO_RBM_THRESHOLD);

        if (memSizeCapInBytes == 0) {
            return Integer.MAX_VALUE;
        }
        if (memSizeCapInBytes >= minRBMMemSize) {
            // max number of elements will be when we have an RBM
            return Math.max(RBMSizeEstimator.getNumEntriesFromSizeInBytes(memSizeCapInBytes), INTARR_TO_RBM_THRESHOLD); // there's some floating point weirdness, so we need the min to ensure we dont get values slightly below 100k
        }
        if (memSizeCapInBytes < intArrMemSize) {
            // max number of elements will be when we have a hash set
            return Math.min((int) (RBMSizeEstimator.convertBytesToMB(memSizeCapInBytes) / RBMSizeEstimator.HASHSET_MEM_SLOPE), HASHSET_TO_INTARR_THRESHOLD - 1);
        }
        // max number of elements will be when we have an intArr
        return INTARR_TO_RBM_THRESHOLD - 1;
    }

    protected static long getIntArrMemSizeInBytes() {
        return (long) (4 * INTARR_SIZE + 24);
    }

    @Override
    public long getMemorySizeInBytes() {
        switch (currentStructure) {
            case HASHSET:
                return RBMSizeEstimator.getHashsetMemSizeInBytes(stats.size);
            case INTARR:
                return getIntArrMemSizeInBytes();
            case RBM:
                return RBMSizeEstimator.getSizeInBytes(stats.size);
        }
        return 0;
    }

    @Override
    public long getMemorySizeCapInBytes() {
        return stats.memSizeCapInBytes;
    }

    public int getMaxNumEntries() {
        return stats.maxNumEntries;
    }

    @Override
    public boolean isFull() {
        return stats.atCapacity;
    }

    @Override
    public void regenerateStore(Integer[] newValues) throws IllegalStateException {
        intArr = null;
        rbm = null;
        stats.size = 0;
        stats.numCollisions = new CounterMetric();
        stats.numAddAttempts = new CounterMetric();
        stats.guaranteesNoFalseNegatives = true;
        stats.numRemovalAttempts = new CounterMetric();
        stats.numSuccessfulRemovals = new CounterMetric();
        currentStructure = StructureTypes.HASHSET;
        hashset = new HashSet<>();

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
