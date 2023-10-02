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

/**
 * A class used to estimate roaring bitmap memory sizes (and hash set sizes).
 * Values based on experiments with adding randomly distributed integers, which matches the use case for KeyLookupStore.
 * In this use case, true values are much higher than an RBM's self-reported size, especially for small RBMs: see
 * https://github.com/RoaringBitmap/RoaringBitmap/issues/257
 */
public class RBMSizeEstimator {
    public static final int BYTES_IN_MB = 1048576;
    public static final double HASHSET_MEM_SLOPE = 6.46 * Math.pow(10, -5);
    public static final double slope = 0.62;
    public static final double bufferMultiplier = 1.5;
    public static final double intercept = 2.9;

    RBMSizeEstimator() {}

    public static long getSizeInBytes(int numEntries) {
        // Based on a linear fit in log-log space, so that we minimize the error as a proportion rather than as
        // an absolute value. Should be within ~50% of the true value at worst, and should overestimate rather
        // than underestimate the memory usage
        return (long) ((long) Math.pow(numEntries, slope) * (long) Math.pow(10, intercept) * bufferMultiplier);
    }

    public static int getNumEntriesFromSizeInBytes(long sizeInBytes) {
        // This function has some precision issues especially when composed with its inverse:
        // numEntries = getNumEntriesFromSizeInBytes(getSizeInBytes(numEntries))
        // In this case the result can be off by up to a couple percent
        // However, this shouldn't really matter as both functions are based on memory estimates with higher errors than a couple percent
        // and this composition won't happen outside of tests
        return (int) Math.pow(sizeInBytes / (bufferMultiplier * Math.pow(10, intercept)), 1 / slope);

    }

    protected static long convertMBToBytes(double valMB) {
        return (long) (valMB * BYTES_IN_MB);
    }

    protected static double convertBytesToMB(long valBytes) {
        return (double) valBytes / BYTES_IN_MB;
    }

    protected static long getHashsetMemSizeInBytes(int numEntries) {
        return convertMBToBytes(HASHSET_MEM_SLOPE * numEntries);
    }
}
