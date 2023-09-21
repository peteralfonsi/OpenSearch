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
import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class HybridIntKeyLookupStore implements IntKeyLookupStore {
    public static final int HASHSET_TO_INTARR_THRESHOLD = 5000;
    public static final int INTARR_SIZE = 100000;
    public static final int INTARR_TO_RBM_THRESHOLD = INTARR_SIZE;
    public static final double HASHSET_MEM_SLOPE = 6.46 * Math.pow(10, -6); // used to calculate memory usage

    public enum StructureTypes {
        HASHSET,
        INTARR,
        RBM
    };

    protected StructureTypes currentStructure;
    protected final int modulo;
    protected int size;
    protected double memSizeCap; // in MB
    protected int numAddAttempts;
    protected int numCollisions;
    protected boolean guaranteesNoFalseNegatives;

    protected HashSet<Integer> hashset;
    protected int[] intArr;
    protected RoaringBitmap rbm;
    // set these to null once they're replaced - then GC will pick it up allegedly
    protected final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    protected final Lock readLock = lock.readLock();
    protected final Lock writeLock = lock.writeLock();

    // These are used to estimate RBM memory usage
    protected double RBMMemSlope;
    protected double RBMMemBufferMultiplier;
    protected double RBMMemIntercept;
    protected int maxNumEntries;
    protected boolean isAtCapacity;


    public HybridIntKeyLookupStore(int modulo, double memSizeCap) {
        this.modulo = modulo; // A modulo of 0 means no modulo
        this.hashset = new HashSet<Integer>();
        this.currentStructure = StructureTypes.HASHSET;
        this.size = 0;
        this.numAddAttempts = 0;
        this.numCollisions = 0;
        this.guaranteesNoFalseNegatives = true;
        this.memSizeCap = memSizeCap; // A cap of 0 means no cap
        memSizeInitFunction(); // Initialize values for RBM memory size estimates
        this.maxNumEntries = calculateMaxNumEntries();
    }

    protected final int customAbs(int value) { // this seems really extra of the forbidden-apis-config to enforce...
        if (value < 0 && value > Integer.MIN_VALUE) {
            return -value;
        } else if (value >= 0) {
            return value;
        }
        return Integer.MAX_VALUE;
    }

    protected final int transform(int value) {
        return modulo == 0 ? -customAbs(value) : -customAbs(value % modulo);
    }

    protected void intArrChecks(int value) throws Exception {
        if (currentStructure != StructureTypes.INTARR) {
            throw new Exception("Cannot run isInIntArr when currentStructure is not INTARR!!");
        }
        if (value > 0) {
            throw new Exception("Cannot use positive value " + Integer.toString(value) + " in isInIntArr");
        }
    }

    protected final boolean isInIntArr(int value, int arrSize, boolean doAdd) throws Exception { // write lock?
        // Checks for presence of value in intArr. If doAdd is true and the value is not already there, adds it.
        // Returns true if the value was already contained (and therefore not added again), false otherwise
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

    protected final void switchHashsetToIntArr() throws Exception { // write lock?
        // during the transitions, especially intarr->RBM, the memory usage will spike. Not sure how to handle this, or if it's really a
        // problem?
        writeLock.lock();
        try {
            if (currentStructure == StructureTypes.HASHSET) {
                size = 0;
                intArr = new int[INTARR_SIZE];
                currentStructure = StructureTypes.INTARR;
                for (int value : hashset) {
                    boolean alreadyContained = isInIntArr(value, size, true);
                    // should never be already contained, but just to be safe
                    if (!alreadyContained) {
                        size++;
                    }
                }
                hashset = null;
            }
        } finally {
            writeLock.unlock();
        }
    }

    protected final void switchIntArrToRBM() { // write lock?
        writeLock.lock();
        try {
            if (currentStructure == StructureTypes.INTARR) {
                currentStructure = StructureTypes.RBM;
                rbm = new RoaringBitmap();
                for (int i = 0; i < size; i++) {
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
    protected final void handleStructureSwitch() throws Exception { // write lock?
        writeLock.lock();
        try {
            if (size == HASHSET_TO_INTARR_THRESHOLD - 1) {
                //if (getIntArrMemSize() >= memSizeCap && memSizeCap != 0) {
                if (maxNumEntries <= HASHSET_TO_INTARR_THRESHOLD) {
                    isAtCapacity = true;
                    return;
                }
                switchHashsetToIntArr();
            } else if (size == INTARR_TO_RBM_THRESHOLD - 1) {
                //if (getRBMMemSize(INTARR_TO_RBM_THRESHOLD) >= memSizeCap && memSizeCap != 0) {
                if (maxNumEntries <= INTARR_TO_RBM_THRESHOLD) {
                    isAtCapacity = true;
                    return;
                }
                switchIntArrToRBM();
            }
        } finally {
            writeLock.unlock();
        }
    }

    protected final void removeFromIntArr(int value) throws Exception {
        writeLock.lock();
        try {
            intArrChecks(value);
            int index = Arrays.binarySearch(intArr, 0, size, value);
            if (index >= 0) {
                System.arraycopy(intArr, index + 1, intArr, index, size - index - 1);
                intArr[size - 1] = 0;
                size--;
            }
        } finally {
            writeLock.unlock();
        }
    }

    protected void handleCollisions(int transformedValue) {
        numCollisions++;
    }

    @Override
    public boolean add(int value) throws Exception {
        writeLock.lock();
        try {
            if (size == maxNumEntries) {
                isAtCapacity = true;
            }
            handleStructureSwitch(); // also might set isAtCapacity
            if (!isAtCapacity) {

                numAddAttempts++;
                int transformedValue = transform(value);
                boolean alreadyContained;

                switch (currentStructure) {
                    case HASHSET:
                        alreadyContained = !(hashset.add(transformedValue));
                        break;
                    case INTARR:
                        alreadyContained = isInIntArr(transformedValue, size, true);
                        break;
                    case RBM:
                        alreadyContained = containsTransformed(transformedValue);
                        if (!alreadyContained) {
                            rbm.add(transformedValue);
                        }
                        break;
                    default:
                        throw new Exception("currentStructure is none of possible values");
                }
                if (alreadyContained) {
                    handleCollisions(transformedValue);
                    return false;
                }
                size++;
                return true;
            }
            return false;
        } finally {
            writeLock.unlock();
        }
    }

    protected boolean containsTransformed(int transformedValue) throws Exception {
        // fill in with switch statement
        readLock.lock();
        try {
            switch (currentStructure) {
                case HASHSET:
                    return hashset.contains(transformedValue);
                case INTARR:
                    return isInIntArr(transformedValue, size, false);
                case RBM:
                    return rbm.contains(transformedValue);
                default:
                    throw new Exception("currentStructure is none of possible values");
            }
        } finally {
            readLock.unlock();
        }
    }

    protected boolean arrayCorrectlySorted() {
        // I think this is necessary as a protected method for unit testing, but shouldn't be used otherwise
        // How should I improve this?
        readLock.lock();
        try {
            if (currentStructure == StructureTypes.INTARR) {
                for (int j = 0; j < intArr.length - 1; j++) {
                    if (!((intArr[j] < intArr[j + 1]) || (intArr[j] == intArr[j + 1] && intArr[j + 1] == 0))) {
                        // left clause: check that array is sorted, right clause: check that values are unique unless they're zero (uninitialized)
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
    public boolean contains(int value) throws Exception {
        int transformedValue = transform(value);
        return containsTransformed(transformedValue);
    }

    @Override
    public int getInternalRepresentation(int value) {
        return transform(value);
    }

    @Override
    public boolean remove(int value) throws Exception {
        return false;
    }

    @Override
    public boolean supportsRemoval() {
        return false;
    }

    protected void removeHelperFunction(int transformedValue) throws Exception {
        // allows code to be reused in forceRemove() of this class and remove() of inheriting class
        // shouldn't be called on its own, or on a value that's not already inside the structure
        switch (currentStructure) {
            case HASHSET:
                hashset.remove(transformedValue);
                size--;
                return;
            case INTARR:
                removeFromIntArr(transformedValue); // size is decreased in this function already
                return;
            case RBM:
                rbm.remove(transformedValue);
                size--;
        }
    }
    @Override
    public void forceRemove(int value) throws Exception {
        writeLock.lock();
        guaranteesNoFalseNegatives = false;
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
        return !guaranteesNoFalseNegatives;
    }

    @Override
    public int getSize() {
        readLock.lock(); // needed because size is changed during switchHashsetToIntarr()
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
        switch (currentStructure) {
            case HASHSET:
                return "HashSet";
            case INTARR:
                return "intArr";
            case RBM:
                return "RBM";
            default:
                throw new Exception("currentStructure is none of possible values");
        }
    }

    @Override
    public int getModulo() {
        return modulo;
    }

    @Override
    public boolean isUsingNegativeOnly() {
        return true;
    }

    @Override
    public boolean isCollision(int value1, int value2) {
        return transform(value1) == transform(value2);
    }

    protected static double[] memSizeHelperFunction(int modulo) {
        // Run to set up values to help estimate RBM size given a modulo
        // Returns an array of {bufferMultiplier, slope, intercept}
        // See https://quip-amazon.com/9Vl3A3kBq2bR/IntKeyLookupStore-Size-Estimates
        // for an explanation of where these numbers came from

        double modifiedModulo;
        if (modulo == 0) {
            modifiedModulo = 31.0;
        } else {
            modifiedModulo = Math.log(0.5 * modulo) / Math.log(2);
        }
        // Note the effective modulos are 0.5x compared to tests, since we also use only negative numbers due to the intArr
        double highCutoff = 29.001; // Floating point makes 29 not work
        double lowCutoff = 28.0;
        double bufferMultiplier = 1.35;
        if (modifiedModulo <= highCutoff) {
            bufferMultiplier = 1.6;
        }

        double slope;
        double intercept;
        if (modifiedModulo > highCutoff) {
            slope = 0.69;
            intercept = -3;
        } else if (modifiedModulo >= lowCutoff) {
            slope = 0.75;
            intercept = -3.5;
        } else {
            slope = 0.88;
            intercept = -4.5;
        }
        return new double[]{bufferMultiplier, slope, intercept};
    }

    protected void memSizeInitFunction() {
        double[] memSizeValues = memSizeHelperFunction(modulo);
        this.RBMMemBufferMultiplier = memSizeValues[0];
        this.RBMMemSlope = memSizeValues[1];
        this.RBMMemIntercept = memSizeValues[2];
    }

    protected int calculateMaxNumEntries() {
        double maxHashsetMemSize = HybridIntKeyLookupStore.getHashsetMemSize(HybridIntKeyLookupStore.HASHSET_TO_INTARR_THRESHOLD-1);
        double intArrMemSize = HybridIntKeyLookupStore.getIntArrMemSize();
        double minRBMMemSize = HybridIntKeyLookupStore.getRBMMemSizeWithModulo(HybridIntKeyLookupStore.INTARR_TO_RBM_THRESHOLD, modulo);

        if (memSizeCap == 0) {
            return Integer.MAX_VALUE;
        }
        if (memSizeCap >= minRBMMemSize) {
            // max number of elements will be when we have an RBM

            return (int) Math.pow(memSizeCap / (this.RBMMemBufferMultiplier * Math.pow(10, this.RBMMemIntercept)), 1 / this.RBMMemSlope);
        }
        if (memSizeCap < intArrMemSize) {
            // max number of elements will be when we have a hash set
            return Math.min((int) (memSizeCap / HASHSET_MEM_SLOPE), HASHSET_TO_INTARR_THRESHOLD-1);
        }
        // max number of elements will be when we have an intArr
        return HybridIntKeyLookupStore.INTARR_TO_RBM_THRESHOLD - 1;
    }

    protected static double getHashsetMemSize(int numEntries) {
        // See https://quip-amazon.com/9Vl3A3kBq2bR/IntKeyLookupStore-Size-Estimates
        // for an explanation of where these numbers came from
        return HASHSET_MEM_SLOPE * numEntries;
    }

    protected static double getIntArrMemSize() {
        return (4 * INTARR_SIZE + 24) / (Math.pow(2, 20));
    }

    protected double getRBMMemSize(int numEntries) {
        // See https://quip-amazon.com/9Vl3A3kBq2bR/IntKeyLookupStore-Size-Estimates
        // for an explanation of where these numbers came from
        return Math.pow(numEntries, RBMMemSlope) * Math.pow(10, RBMMemIntercept) * RBMMemBufferMultiplier;
    }

    protected static double getRBMMemSizeWithModulo(int numEntries, int modulo) {
        double[] memSizeValues = memSizeHelperFunction(modulo);
        return Math.pow(numEntries, memSizeValues[1]) * Math.pow(10, memSizeValues[2]) * memSizeValues[0];
    }

    @Override
    public double getMemorySize() {
        switch (currentStructure) {
            case HASHSET:
                return getHashsetMemSize(size);
            case INTARR:
                return getIntArrMemSize();
            case RBM:
                return getRBMMemSize(size);
        }
        return 0;
    }

    @Override
    public double getMemorySizeCap() {
        return memSizeCap;
    }

    public double getRBMMemSlope(){
        return RBMMemSlope;
    }
    public double getRBMMemBufferMultiplier() {
        return RBMMemBufferMultiplier;
    }
    public double getRBMMemIntercept() {
        return RBMMemIntercept;
    }

    public int getMaxNumEntries() {
        return maxNumEntries;
    }

    @Override
    public boolean getIsAtCapacity() {
        return isAtCapacity;
    }

    @Override
    public void regenerateStore(int[] newValues) throws Exception {
        intArr = null;
        rbm = null;
        size = 0;
        numCollisions = 0;
        numAddAttempts = 0;
        guaranteesNoFalseNegatives = true;
        currentStructure = StructureTypes.HASHSET;
        hashset = new HashSet<>();

        for (int value: newValues) {
            add(value);
        }
    }
}
