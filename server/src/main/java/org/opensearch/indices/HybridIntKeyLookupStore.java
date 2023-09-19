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
//import java.lang.Math;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class HybridIntKeyLookupStore implements IntKeyLookupStore {
    public static final int HASHSET_TO_INTARR_THRESHOLD = 5000;
    public static final int INTARR_SIZE = 100000;
    public static final int INTARR_TO_RBM_THRESHOLD = INTARR_SIZE;

    public enum StructureTypes {
        HASHSET,
        INTARR,
        RBM
    };

    private StructureTypes currentStructure;
    private final int modulo;
    private int size;
    private int numAddAttempts;
    private int numCollisions;
    private boolean guaranteesNoFalseNegatives;

    private HashSet<Integer> hashset;
    private int[] intArr;
    private RoaringBitmap rbm;
    // set these to null once they're replaced - then GC will pick it up allegedly
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final Lock readLock = lock.readLock();
    private final Lock writeLock = lock.writeLock();

    public HybridIntKeyLookupStore(int modulo) {
        this.modulo = modulo; // A modulo of 0 means no modulo
        this.hashset = new HashSet<Integer>();
        this.currentStructure = StructureTypes.HASHSET;
        this.size = 0;
        this.numAddAttempts = 0;
        this.numCollisions = 0;
        this.guaranteesNoFalseNegatives = true;
    }

    private int customAbs(int value) { // this seems really extra of the forbidden-apis-config to enforce...
        if (value < 0 && value > Integer.MIN_VALUE) {
            return -value;
        } else if (value >= 0) {
            return value;
        }
        return Integer.MAX_VALUE;
    }

    private int transform(int value) {
        return modulo == 0 ? -customAbs(value) : -customAbs(value % modulo);
    }

    private void intArrChecks(int value) throws Exception {
        if (currentStructure != StructureTypes.INTARR) {
            throw new Exception("Cannot run isInIntArr when currentStructure is not INTARR!!");
        }
        if (value > 0) {
            throw new Exception("Cannot use positive value " + Integer.toString(value) + " in isInIntArr");
        }
    }

    private boolean isInIntArr(int value, int arrSize, boolean doAdd) throws Exception { // write lock?
        // Checks for presence of value in intArr. If doAdd is true and the value is not already there, adds it.
        // Returns true if the value was already contained (and therefore not added again), false otherwise
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
    }

    private void switchHashsetToIntArr() throws Exception { // write lock?
        // during the transitions, especially intarr->RBM, the memory usage will spike. Not sure how to handle this, or if it's really a
        // problem?
        size = 0;
        intArr = new int[INTARR_SIZE];
        currentStructure = StructureTypes.INTARR;
        //int i = 0;
        for (int value : hashset) {
            boolean alreadyContained = isInIntArr(value, size, true);
            // should never be already contained, but just to be safe
            if (!alreadyContained) {
                size++;
            }
            //i++;
        }
        //System.out.println("i = " + Integer.toString(i)); // debug
        hashset = null;
    }

    private void switchIntArrToRBM() { // write lock?
        currentStructure = StructureTypes.RBM;
        rbm = new RoaringBitmap();
        for (int i = 0; i < size; i++) {
            rbm.add(intArr[i]);
        }
        intArr = null;
        // comment!
    }

    /**
     * Checks if adding an additional value would require us to change data structures.
     * If so, start that change.
     */
    private void handleStructureSwitch() throws Exception { // write lock?
        if (size == HASHSET_TO_INTARR_THRESHOLD - 1) {
            switchHashsetToIntArr();
        } else if (size == INTARR_TO_RBM_THRESHOLD - 1) {
            switchIntArrToRBM();
        }
    }

    private void removeFromIntArr(int value) throws Exception {
        intArrChecks(value);
        int index = Arrays.binarySearch(intArr, 0, size, value);
        if (index >= 0) {
            System.arraycopy(intArr, index + 1, intArr, index, size - index - 1);
            intArr[size - 1] = 0;
            size--;
        }
    }

    @Override
    public boolean add(int value) throws Exception {
        writeLock.lock();
        try {
            numAddAttempts++;
            int transformedValue = transform(value);
            handleStructureSwitch();
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
                numCollisions++;
                return false;
            }
            size++;
            return true;
        } finally {
            writeLock.unlock();
        }
    }

    private boolean containsTransformed(int transformedValue) throws Exception {
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
        if (currentStructure == StructureTypes.INTARR) {
            for (int j = 0; j < intArr.length - 1; j++) {
                if (!((intArr[j] < intArr[j + 1]) || (intArr[j] == intArr[j + 1] && intArr[j + 1] == 0))) {
                    // left clause: check that array is sorted, right clause: check that values are unique unless they're zero (uninitialized)
                    return false;
                }
            }
        }
        return true;
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
    public boolean remove(int value) {
        return false;
    }

    @Override
    public boolean supportsRemoval() {
        return false;
    }

    @Override
    public void forceRemove(int value) throws Exception {
        writeLock.lock();
        guaranteesNoFalseNegatives = false;

        try {
            int transformedValue = transform(value);
            boolean alreadyContained = contains(transformedValue);
            if (alreadyContained) {
                switch (currentStructure) {
                    case HASHSET:
                        hashset.remove(transformedValue);
                        break;
                    case INTARR:
                        removeFromIntArr(transformedValue);
                        break;
                    case RBM:
                        rbm.remove(transformedValue);
                        break;
                }
                size--;
            }
        } catch(Exception e) {
            System.out.println("exception!! " + e);
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

    @Override
    public long getMemorySize() {
        return 0; // :(
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
