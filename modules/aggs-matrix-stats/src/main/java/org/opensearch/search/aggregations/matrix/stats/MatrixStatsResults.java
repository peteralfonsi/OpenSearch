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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Descriptive stats gathered per shard. Coordinating node computes final pearson product coefficient
 * based on these descriptive stats
 */
// TODO: For now, this can still return Maps for use by InternalMatrixStats - doesn't look like this is part of the hot loop.
//    In an actual PR, I would adjust this to also use double[] etc
class MatrixStatsResults implements Writeable {
    /** object holding results - computes results in place */
    protected final RunningStats results;
    /** pearson product correlation coefficients */
    protected final Map<String, HashMap<String, Double>> correlation;

    /** Base ctor */
    MatrixStatsResults(String[] fieldNames) {
        results = new RunningStats(fieldNames);
        this.correlation = new HashMap<>();
    }

    /** creates and computes result from provided stats */
    MatrixStatsResults(RunningStats stats) {
        this.results = stats.clone();
        this.correlation = new HashMap<>();
        this.compute();
    }

    /** creates a results object from the given stream */
    @SuppressWarnings("unchecked")
    protected MatrixStatsResults(StreamInput in) {
        try {
            results = new RunningStats(in);
            correlation = (Map<String, HashMap<String, Double>>) in.readGenericValue();
        } catch (IOException e) {
            throw new OpenSearchException("Error trying to create multifield_stats results from stream input", e);
        }
    }

    /** Marshalls MatrixStatsResults */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // marshall results
        results.writeTo(out);
        // marshall correlation
        out.writeGenericValue(correlation);
    }

    /** return document count */
    public final long getDocCount() {
        return results.docCount;
    }

    /** return the field counts - not public, used for getProperty() */
    protected Map<String, Long> getFieldCounts() {
        //return Collections.unmodifiableMap(results.counts);
        return Collections.unmodifiableMap(results.convertLongArrayToMap(results.counts));
    }

    /** return the field count for the requested field */
    public long getFieldCount(String field) {
        /*if (results.counts.containsKey(field) == false) {
            return 0;
        }
        return results.counts.get(field);*/
        for (int i = 0; i < results.fieldNames.length; i++) {
            if (field.equals(results.fieldNames[i])) {
                return results.counts[i];
            }
        }
        return 0;
    }

    /** return the means - not public, used for getProperty() */
    protected Map<String, Double> getMeans() {
        //return Collections.unmodifiableMap(results.means);
        return Collections.unmodifiableMap(results.convertDoubleArrayToMap(results.means));
    }

    /** return the mean for the requested field */
    public double getMean(String field) {
        Map<String, Double> means = getMeans();
        checkField(field, means);
        return means.get(field);
    }

    /** return the variances - not public, used for getProperty() */
    protected Map<String, Double> getVariances() {
        //return Collections.unmodifiableMap(results.variances);
        return Collections.unmodifiableMap(results.convertDoubleArrayToMap(results.variances));
    }

    /** return the variance for the requested field */
    public double getVariance(String field) {
        Map<String, Double> variances = getVariances();
        checkField(field, variances);
        return variances.get(field);
    }

    /** return the skewness - not public, used for getProperty() */
    protected Map<String, Double> getSkewness() {
        //return Collections.unmodifiableMap(results.skewness);
        return Collections.unmodifiableMap(results.convertDoubleArrayToMap(results.skewness));
    }

    /** return the skewness for the requested field */
    public double getSkewness(String field) {
        Map<String, Double> skewness = getSkewness();
        checkField(field, skewness);
        return skewness.get(field);
    }

    /** return the kurtosis */
    protected Map<String, Double> getKurtosis() {
        //return Collections.unmodifiableMap(results.kurtosis);
        return Collections.unmodifiableMap(results.convertDoubleArrayToMap(results.kurtosis));
    }

    /** return the kurtosis for the requested field */
    public double getKurtosis(String field) {
        Map<String, Double> kurtosis = getKurtosis();
        checkField(field, kurtosis);
        return kurtosis.get(field);
    }

    /** return the covariances as a map - not public, used for getProperty() */
    protected Map<String, HashMap<String, Double>> getCovariances() {
        //return Collections.unmodifiableMap(results.covariances);
        return Collections.unmodifiableMap(results.convertNestedDoubleArrayToMap(results.covariances));
    }

    /** return the covariance between two fields */
    public double getCovariance(String fieldX, String fieldY) {
        Map<String, Double> variances = getVariances();
        if (fieldX.equals(fieldY)) {
            checkField(fieldX, variances);
            return variances.get(fieldX);
        }
        // TODO: Anyway not in hot path
        Map<String, HashMap<String, Double>> covariances = results.convertNestedDoubleArrayToMap(results.covariances);
        return getValFromUpperTriangularMatrix(covariances, fieldX, fieldY);
    }

    /** return the correlations as a map - not public, used for getProperty() */
    protected Map<String, HashMap<String, Double>> getCorrelations() {
        return Collections.unmodifiableMap(correlation);
    }

    /** return the correlation coefficient between two fields */
    public Double getCorrelation(String fieldX, String fieldY) {
        if (fieldX.equals(fieldY)) {
            return 1.0;
        }
        return getValFromUpperTriangularMatrix(correlation, fieldX, fieldY);
    }

    /** return the value for two fields in an upper triangular matrix, regardless of row col location. */
    static <M extends Map<String, Double>> double getValFromUpperTriangularMatrix(Map<String, M> map, String fieldX, String fieldY) {
        // for the co-value to exist, one of the two (or both) fields has to be a row key
        if (map.containsKey(fieldX) == false && map.containsKey(fieldY) == false) {
            throw new IllegalArgumentException("neither field " + fieldX + " nor " + fieldY + " exist");
        } else if (map.containsKey(fieldX)) {
            // fieldX exists as a row key
            if (map.get(fieldX).containsKey(fieldY)) {
                // fieldY exists as a col key to fieldX
                return map.get(fieldX).get(fieldY);
            } else {
                // otherwise fieldX is the col key to fieldY
                return map.get(fieldY).get(fieldX);
            }
        } else if (map.containsKey(fieldY)) {
            // fieldX did not exist as a row key, it must be a col key
            return map.get(fieldY).get(fieldX);
        }
        throw new IllegalArgumentException("Coefficient not computed between fields: " + fieldX + " and " + fieldY);
    }

    private void checkField(String field, Map<String, ?> map) {
        if (field == null) {
            throw new IllegalArgumentException("field name cannot be null");
        }
        if (map.containsKey(field) == false) {
            throw new IllegalArgumentException("field " + field + " does not exist");
        }
    }

    /** Computes final covariance, variance, and correlation */
    private void compute() {
        final double nM1 = results.docCount - 1D;
        // compute final skewness and kurtosis
        //for (String fieldName : results.means.keySet()) {
        for (int i = 0; i < results.fieldNames.length; i++) {
            String fieldName = results.fieldNames[i];
            final double var = results.variances[i]; //results.variances.get(fieldName);
            // update skewness
            //results.skewness.put(fieldName, Math.sqrt(results.docCount) * results.skewness.get(fieldName) / Math.pow(var, 1.5D));
            results.skewness[i] = Math.sqrt(results.docCount) * results.skewness[i] / Math.pow(var, 1.5D);
            // update kurtosis
            //results.kurtosis.put(fieldName, (double) results.docCount * results.kurtosis.get(fieldName) / (var * var));
            results.kurtosis[i] = (double) results.docCount * results.kurtosis[i] / (var * var);
            // update variances
            //results.variances.put(fieldName, results.variances.get(fieldName) / nM1);
            results.variances[i] /= nM1;
        }

        // compute final covariances and correlation
        double cor;
        Map<String, Double> variances = getVariances(); // TODO: Below will be changed anyway
        //for (Map.Entry<String, HashMap<String, Double>> row : results.covariances.entrySet()) {
        for (int i = 0; i < results.fieldNames.length; i++) {
            //final String rowName = row.getKey();
            final String rowName = results.fieldNames[i];
            //final HashMap<String, Double> covRow = row.getValue(); // equivalent to covariances[i] slice
            final HashMap<String, Double> corRow = new HashMap<>();
            //for (Map.Entry<String, Double> col : covRow.entrySet()) {
            // TODO: Top right entry already NaN before this method runs
            for (int j = i+1; j < results.fieldNames.length; j++) {
                //final String colName = col.getKey();
                final String colName = results.fieldNames[j];
                // update covariance
                //covRow.put(colName, covRow.get(colName) / nM1);
                results.covariances[i][j] /= nM1;
                // update correlation
                // if there is no variance in the data then correlation is NaN
                if (variances.get(rowName) == 0d || variances.get(colName) == 0d) {
                    cor = Double.NaN;
                } else {
                    final double corDen = Math.sqrt(variances.get(rowName)) * Math.sqrt(variances.get(colName));
                    cor = results.covariances[i][j] / corDen; //covRow.get(colName) / corDen;
                }
                corRow.put(colName, cor);
            }
            //results.covariances.put(rowName, covRow);
            correlation.put(rowName, corRow);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MatrixStatsResults that = (MatrixStatsResults) o;
        return Objects.equals(results, that.results) && Objects.equals(correlation, that.correlation);
    }

    @Override
    public int hashCode() {
        return Objects.hash(results, correlation);
    }
}
