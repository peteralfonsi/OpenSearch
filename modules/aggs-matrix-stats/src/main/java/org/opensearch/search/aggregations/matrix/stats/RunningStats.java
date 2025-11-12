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

package org.opensearch.search.aggregations.matrix.stats;

import org.opensearch.OpenSearchException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;

/**
 * Descriptive stats gathered per shard. Coordinating node computes final correlation and covariance stats
 * based on these descriptive stats. This single pass, parallel approach is based on:
 * <p>
 * http://prod.sandia.gov/techlib/access-control.cgi/2008/086212.pdf
 */
public class RunningStats implements Writeable, Cloneable {
    /** count of observations (same number of observations per field) */
    protected long docCount = 0;
    // All these primitive arrays store values in the same order as fieldNames
    /** per field sum of observations */
    protected double[] fieldSum;
    /** counts */
    protected long[] counts;
    /** mean values (first moment) */
    protected double[] means;
    /** variance values (second moment) */
    protected double[] variances;
    /** skewness values (third moment) */
    protected double[] skewness;
    /** kurtosis values (fourth moment) */
    protected double[] kurtosis;
    /** covariance values */
    // first index = outer key, second = inner key
    protected double[][] covariances;
    final String[] fieldNames;

    RunningStats(final String[] fieldNames) {
        this.fieldNames = fieldNames;
        init();
    }

    RunningStats(final String[] fieldNames, final double[] fieldVals) {
        this.fieldNames = fieldNames;
        if (fieldVals != null && fieldVals.length > 0) {
            init();
            this.add(fieldNames, fieldVals);
        }
    }

    private void init() {
        assert fieldNames != null;
        counts = new long[fieldNames.length];
        fieldSum = new double[fieldNames.length];
        means = new double[fieldNames.length];
        skewness = new double[fieldNames.length];
        kurtosis = new double[fieldNames.length];
        variances = new double[fieldNames.length];
        covariances = new double[fieldNames.length][fieldNames.length];
    }

    /** Ctor to create an instance of running statistics */
    @SuppressWarnings("unchecked")
    public RunningStats(StreamInput in) throws IOException {
        // TODO: Ensure we sort any field names we read from wire

        // TODO: What if we get a StreamInput from an older version which doesn't publish fieldNames?
        // we could maybe just take the union of all the keysets of the maps, but that technically might be insufficient
        // (if we plan on adding more docs at least)
        // this();
        fieldNames = in.readStringArray();
        docCount = (Long) in.readGenericValue();
        fieldSum = convertFieldDoubleMap((Map<String, Double>) in.readGenericValue()); // TODO: Getting weird cast errors
        counts = convertFieldLongMap((Map<String, Long>) in.readGenericValue());
        means = convertFieldDoubleMap((Map<String, Double>) in.readGenericValue());
        variances = convertFieldDoubleMap((Map<String, Double>) in.readGenericValue());
        skewness = convertFieldDoubleMap((Map<String, Double>) in.readGenericValue());
        kurtosis = convertFieldDoubleMap((Map<String, Double>) in.readGenericValue());
        covariances = convertNestedDoubleMap((Map<String, HashMap<String, Double>>) in.readGenericValue());
        // read doc count
        /*docCount = (Long) in.readGenericValue();
        // read fieldSum
        fieldSum = convertIfNeeded((Map<String, Double>) in.readGenericValue());
        // counts
        counts = convertIfNeeded((Map<String, Long>) in.readGenericValue());
        // means
        means = convertIfNeeded((Map<String, Double>) in.readGenericValue());
        // variances
        variances = convertIfNeeded((Map<String, Double>) in.readGenericValue());
        // skewness
        skewness = convertIfNeeded((Map<String, Double>) in.readGenericValue());
        // kurtosis
        kurtosis = convertIfNeeded((Map<String, Double>) in.readGenericValue());
        // read covariances
        covariances = convertIfNeeded((Map<String, HashMap<String, Double>>) in.readGenericValue());*/
    }

    // Convert Map to HashMap if it isn't
    private static <K, V> HashMap<K, V> convertIfNeeded(Map<K, V> map) {
        if (map instanceof HashMap) {
            return (HashMap<K, V>) map;
        } else {
            return new HashMap<>(map);
        }
    }

    double[] convertFieldDoubleMap(Map<String, Double> serializedMap) {
        assert serializedMap.keySet().equals(new HashSet<>(Arrays.asList(fieldNames)));
        double[] result = new double[fieldNames.length];
        for (int i = 0; i < fieldNames.length; i++) {
            result[i] = serializedMap.get(fieldNames[i]);
        }
        return result;
    }

    // TODO: Any good way to combine these?
    long[] convertFieldLongMap(Map<String, Long> serializedMap) {
        assert serializedMap.keySet().equals(new HashSet<>(Arrays.asList(fieldNames)));
        long[] result = new long[fieldNames.length];
        for (int i = 0; i < fieldNames.length; i++) {
            result[i] = serializedMap.get(fieldNames[i]);
        }
        return result;
    }

    double[][] convertNestedDoubleMap(Map<String, HashMap<String, Double>> serializedMap) {
        assert serializedMap.keySet().equals(new HashSet<>(Arrays.asList(fieldNames)));
        double[][] result = new double[fieldNames.length][fieldNames.length];
        for (int i = 0; i < fieldNames.length; i++) {
            for (int j = i + 1; j < fieldNames.length; j++) {
                result[i][j] = serializedMap.get(fieldNames[i]).get(fieldNames[j]);
            }
        }
        return result;
    }

    // TODO: These are probably just for PoC and can be removed once MatrixStatsResults/InternalMatrixStats are fixed (esp bc going to dump
    // map impl entirely)
    Map<String, Double> convertDoubleArrayToMap(double[] stats) {
        Map<String, Double> result = new HashMap<>();
        for (int i = 0; i < fieldNames.length; i++) {
            result.put(fieldNames[i], stats[i]);
        }
        return result;
    }

    Map<String, Long> convertLongArrayToMap(long[] stats) {
        Map<String, Long> result = new HashMap<>();
        for (int i = 0; i < fieldNames.length; i++) {
            result.put(fieldNames[i], stats[i]);
        }
        return result;
    }

    Map<String, HashMap<String, Double>> convertNestedDoubleArrayToMap(double[][] stats) {
        Map<String, HashMap<String, Double>> result = new HashMap<>();
        for (int i = 0; i < fieldNames.length; i++) {
            HashMap<String, Double> innerMap = new HashMap<>();
            result.put(fieldNames[i], innerMap);
            for (int j = i + 1; j < fieldNames.length; j++) {
                innerMap.put(fieldNames[j], stats[i][j]);
            }
        }
        return result;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // TODO: These don't work yet! They should write maps too oops
        out.writeStringArray(fieldNames);
        // marshall doc count
        out.writeGenericValue(docCount);
        // marshall fieldSum
        out.writeGenericValue(fieldSum);
        // counts
        out.writeGenericValue(counts);
        // mean
        out.writeGenericValue(means);
        // variances
        out.writeGenericValue(variances);
        // skewness
        out.writeGenericValue(skewness);
        // kurtosis
        out.writeGenericValue(kurtosis);
        // covariances
        out.writeGenericValue(covariances);
    }

    /** updates running statistics with a documents field values **/
    public void add(final String[] fn, final double[] fieldVals) { // TODO: For now dont remove argument, but use only this.fieldNames
        if (fieldNames == null) {
            throw new IllegalArgumentException("Cannot add statistics without field names.");
        } else if (fieldVals == null) {
            throw new IllegalArgumentException("Cannot add statistics without field values.");
        } else if (fieldNames.length != fieldVals.length) {
            throw new IllegalArgumentException("Number of field values do not match number of field names.");
        }

        // update total, mean, and variance
        ++docCount;
        double fieldValue;
        double m2, m3; // moments
        double d, dn, dn2, t1;
        final double[] deltas = new double[fieldNames.length];
        for (int i = 0; i < fieldNames.length; ++i) {
            fieldValue = fieldVals[i];
            // update counts
            counts[i]++;
            // update running sum
            fieldSum[i] += fieldValue;
            // update running deltas
            deltas[i] = fieldValue * docCount - fieldSum[i];

            // update running mean, variance, skewness, kurtosis
            if (docCount == 1) {
                means[i] = fieldValue;
                // variances, skewness, kurtosis already initialized to 0
            } else {
                // update running means
                d = fieldValue - means[i];
                dn = d / docCount;
                means[i] += dn;
                // update running variances
                m2 = variances[i];
                t1 = d * dn * (docCount - 1);
                variances[i] += t1;
                // update running skewnesses
                m3 = skewness[i];
                skewness[i] += (t1 * dn * (docCount - 2D) - 3D * dn * m2);
                dn2 = dn * dn;
                kurtosis[i] += t1 * dn2 * (docCount * docCount - 3D * docCount + 3D) + 6D * dn2 * m2 - 4D * dn * m3;
            }
        }

        this.updateCovariance(fieldNames, deltas);
    }

    /** Update covariance matrix */
    private void updateCovariance(final String[] fieldNames, final double[] deltas) {
        if (docCount > 1) {
            for (int i = 0; i < fieldNames.length; ++i) {
                for (int j = i + 1; j < fieldNames.length; j++) {
                    double intermediate = 1.0 / (docCount * (docCount - 1.0)) * deltas[i] * deltas[j];
                    covariances[i][j] += intermediate;
                }
            }
        }
    }

    /**
     * Merges the descriptive statistics of a second data set (e.g., per shard)
     * <p>
     * running computations taken from: http://prod.sandia.gov/techlib/access-control.cgi/2008/086212.pdf
     **/
    public void merge(final RunningStats other) {
        if (other == null) {
            return;
        } else if (!Arrays.equals(other.fieldNames, fieldNames)) {
            throw new IllegalArgumentException("Cannot merge RunningStats with different fieldNames");
        } else if (this.docCount == 0) {
            this.means = other.means;
            this.counts = other.counts;
            this.fieldSum = other.fieldSum;
            this.variances = other.variances;
            this.skewness = other.skewness;
            this.kurtosis = other.kurtosis;
            this.covariances = other.covariances;
            this.docCount = other.docCount;
            return;
        }
        final double nA = docCount;
        final double nB = other.docCount;
        // merge count
        docCount += other.docCount;

        final double[] deltas = new double[fieldNames.length];
        double meanA, varA, skewA, kurtA, meanB, varB, skewB, kurtB;
        double d, d2, d3, d4, n2, nA2, nB2;
        double newSkew, nk;
        // across fields
        for (int i = 0; i < fieldNames.length; i++) {
            meanA = means[i];
            varA = variances[i];
            skewA = skewness[i];
            kurtA = kurtosis[i];
            meanB = other.means[i];
            varB = other.variances[i];
            skewB = other.skewness[i];
            kurtB = other.kurtosis[i];

            // merge counts of two sets
            counts[i] += other.counts[i];

            // merge means of two sets
            means[i] = (nA * means[i] + nB * other.means[i]) / (nA + nB);

            // merge deltas
            deltas[i] = other.fieldSum[i] / nB - fieldSum[i] / nA;

            // merge totals
            fieldSum[i] += other.fieldSum[i];

            // merge variances, skewness, and kurtosis of two sets
            d = meanB - meanA;          // delta mean
            d2 = d * d;                 // delta mean squared
            d3 = d * d2;                // delta mean cubed
            d4 = d2 * d2;               // delta mean 4th power
            n2 = docCount * docCount;   // num samples squared
            nA2 = nA * nA;              // doc A num samples squared
            nB2 = nB * nB;              // doc B num samples squared
            // variance
            variances[i] = varA + varB + d2 * nA * other.docCount / docCount;
            // skewness
            newSkew = skewA + skewB + d3 * nA * nB * (nA - nB) / n2;
            skewness[i] = newSkew + 3D * d * (nA * varB - nB * varA) / docCount;
            // kurtosis
            nk = kurtA + kurtB + d4 * nA * nB * (nA2 - nA * nB + nB2) / (n2 * docCount);
            kurtosis[i] = nk + 6D * d2 * (nA2 * varB + nB2 * varA) / n2 + 4D * d * (nA * skewB - nB * skewA) / docCount;
        }

        this.mergeCovariance(other, deltas);
    }

    /** Merges two covariance matrices */
    private void mergeCovariance(final RunningStats other, final double[] deltas) {
        double f = ((double) (docCount - other.docCount)) * other.docCount / this.docCount; // , dR, newVal;
        for (int i = 0; i < fieldNames.length; i++) {
            for (int j = i + 1; j < fieldNames.length; j++) {
                covariances[i][j] += other.covariances[i][j] + f * deltas[i] * deltas[j];
            }
        }
    }

    @Override
    public RunningStats clone() {
        try {
            return (RunningStats) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new OpenSearchException("Error trying to create a copy of RunningStats");
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RunningStats that = (RunningStats) o;
        return docCount == that.docCount
            && Arrays.equals(fieldSum, that.fieldSum)
            && Arrays.equals(counts, that.counts)
            && Arrays.equals(means, that.means)
            && Arrays.equals(variances, that.variances)
            && Arrays.equals(skewness, that.skewness)
            && Arrays.equals(kurtosis, that.kurtosis)
            && Arrays.deepEquals(covariances, that.covariances);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            docCount,
            Arrays.hashCode(fieldSum),
            Arrays.hashCode(counts),
            Arrays.hashCode(means),
            Arrays.hashCode(variances),
            Arrays.hashCode(skewness),
            Arrays.hashCode(kurtosis),
            Arrays.deepHashCode(covariances)
        );
    }
}
