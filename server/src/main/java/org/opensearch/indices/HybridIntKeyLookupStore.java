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
import java.lang.Math;
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

    private int transform(int value) {
        return modulo == 0 ? -Math.abs(value) : -Math.abs(value % modulo);
    }

    private void intArrChecks(int value) throws Exception {
        if (currentStructure != StructureTypes.INTARR) {
            throw new Exception("Cannot run isInIntArr when currentStructure is not INTARR!!");
        }
        if (value > 0) {
            throw new Exception(String.format("Cannot use positive value %d in isInIntArr", value));
        }
    }

    private boolean isInIntArr(int value, boolean doAdd) throws Exception { // write lock?
        // Checks for presence of value in intArr. If doAdd is true and the value is not already there, adds it.
        // Returns true if the value was already contained (and therefore not added again), false otherwise
        intArrChecks(value);
        int index = Arrays.binarySearch(intArr, 0, size, value); // only search in initialized part of array
        if (index < 0) {
            if (doAdd) {
                int insertionPoint = -index - 1;
                System.arraycopy(intArr, insertionPoint, intArr, insertionPoint + 1, size - insertionPoint);
                intArr[insertionPoint] = value;
                size++;
            }
            return false;
        }
        return true;
    }


    private void switchHashsetToIntArr() throws Exception { // write lock?
        // during the transitions, especially intarr->RBM, the memory usage will spike. Not sure how to handle this, or if it's really a problem?
        size = 0;
        intArr = new int[INTARR_SIZE];
        currentStructure = StructureTypes.INTARR;
        for (int value : hashset) {
            isInIntArr(value, true);
        }
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
    private void handleStructureSwitch() throws Exception{ // write lock?
        if (size == HASHSET_TO_INTARR_THRESHOLD - 1) {
            switchHashsetToIntArr();
        } else if (size == INTARR_TO_RBM_THRESHOLD - 1) {
            switchIntArrToRBM();
        }
    }

    private void removeFromIntArr(int value) throws Exception {
        intArrChecks(value);
        int index = Arrays.binarySearch(intArr, 0, size, value);
        if (index < 0) {
            int insertionPoint = -index - 1;
            System.arraycopy(intArr, insertionPoint + 1, intArr, insertionPoint, size - insertionPoint - 1);
            intArr[size-1] = 0;
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
                    alreadyContained = isInIntArr(transformedValue, true);
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
                    return !(hashset.add(transformedValue));
                case INTARR:
                    return isInIntArr(transformedValue, false);
                case RBM:
                    return rbm.contains(transformedValue);
                default:
                    throw new Exception("currentStructure is none of possible values");
            }
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
            switch (currentStructure) {
                case HASHSET:
                    hashset.remove(transformedValue);
                case INTARR:
                    removeFromIntArr(transformedValue);
                case RBM:
                    rbm.remove(transformedValue);
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

    @Override
    public long getMemorySize() {
        return 0;
    }

    @Override
    public void regenerateStore() {

    }
}
