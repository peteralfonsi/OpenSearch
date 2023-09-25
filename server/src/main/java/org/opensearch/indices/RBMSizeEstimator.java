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
 * A class used to estimate roaring bitmap memory sizes.
 * An instance is made with a particular modulo to avoid recomputing
 * values.
 */
public class RBMSizeEstimator {
    public static final int BYTES_IN_MB = 1048576;
    protected double slope;
    protected double bufferMultiplier;
    protected double intercept;
    protected int modulo;


    RBMSizeEstimator(int modulo) {
        this.modulo = modulo;
        double[] memSizeValues = calculateMemoryCoefficients(modulo);
        this.bufferMultiplier = memSizeValues[0];
        this.slope = memSizeValues[1];
        this.intercept = memSizeValues[2];
    }
    public static double[] calculateMemoryCoefficients(int modulo) {
        // Sets up values to help estimate RBM size given a modulo
        // Returns an array of {bufferMultiplier, slope, intercept}

        double modifiedModulo;
        if (modulo == 0) {
            modifiedModulo = 32.0;
        } else {
            modifiedModulo = Math.log(modulo) / Math.log(2);
        }
        // The effective modulo should be passed in - aka 0.5 * modulo for a hybrid store
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
        return new double[] { bufferMultiplier, slope, intercept };
    }
    public static double getSizeWithModuloInMB(int numEntries, int modulo) {
        double[] memCoefs = calculateMemoryCoefficients(modulo);
        return Math.pow(numEntries, memCoefs[1]) * Math.pow(10, memCoefs[2]) * memCoefs[0];
    }

    public double getSizeInMB(int numEntries) {
        return Math.pow(numEntries, slope) * Math.pow(10, intercept) * bufferMultiplier;
    }

    public int getNumEntriesFromSizeInMB(double sizeInMB) {
        return (int) Math.pow(sizeInMB / (bufferMultiplier * Math.pow(10, intercept)), 1 / slope);
    }

    protected static long convertMBToBytes(double valMB) {
        return (long) (valMB * BYTES_IN_MB);
    }

    protected static double convertBytesToMB(long valBytes) {
        return (double) valBytes / BYTES_IN_MB;
    }

    public double getSlope() {
        return slope;
    }
    public double getIntercept() {
        return intercept;
    }
    public double getBufferMultiplier() {
        return bufferMultiplier;
    }
}
