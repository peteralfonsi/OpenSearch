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

package org.opensearch.search.aggregations.bucket.histogram;

import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.ScoreMode;
import org.opensearch.index.fielddata.SortedNumericDoubleValues;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.LeafBucketCollectorBase;
import org.opensearch.search.aggregations.bucket.terms.LongKeyedBucketOrds;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.search.aggregations.support.ValuesSourceConfig;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * An aggregator for numeric values. For a given {@code interval},
 * {@code offset} and {@code value}, it returns the highest number that can be
 * written as {@code interval * x + offset} and yet is less than or equal to
 * {@code value}.
 *
 * @opensearch.internal
 */
public class NumericHistogramAggregator extends AbstractHistogramAggregator {
    private final ValuesSource.Numeric valuesSource;

    public NumericHistogramAggregator(
        String name,
        AggregatorFactories factories,
        double interval,
        double offset,
        BucketOrder order,
        boolean keyed,
        long minDocCount,
        DoubleBounds extendedBounds,
        DoubleBounds hardBounds,
        ValuesSourceConfig valuesSourceConfig,
        SearchContext context,
        Aggregator parent,
        CardinalityUpperBound cardinalityUpperBound,
        Map<String, Object> metadata
    ) throws IOException {
        super(
            name,
            factories,
            interval,
            offset,
            order,
            keyed,
            minDocCount,
            extendedBounds,
            hardBounds,
            valuesSourceConfig.format(),
            context,
            parent,
            cardinalityUpperBound,
            metadata
        );
        // TODO: Stop using null here
        this.valuesSource = valuesSourceConfig.hasValues() ? (ValuesSource.Numeric) valuesSourceConfig.getValuesSource() : null;
        if (valuesSource instanceof ValuesSource.Numeric.FieldData) { // TODO: Do we need a version check here? We may just need to check we
                                                                      // can get PointValues. Later code assumes we can run .doubleValues()
            Double minimumKey = getMinimumKey(valuesSource, context);
            if (minimumKey != null) {
                // Close the default bucketOrds created by the parent class before creating a new one
                this.bucketOrds.close();
                this.bucketOrds = new LongKeyedBucketOrds.MinimumAwareBucketOrds(minimumKey, context.bigArrays());
                useNaturalBucketOrdering = true;
            }
        }
    }

    // Return the key that would be produced for the minimum value in the field across all leaf contexts,
    // or null if this couldn't be determined
    private Double getMinimumKey(ValuesSource.Numeric valuesSource, SearchContext context) {
        ContextIndexSearcher searcher = context.searcher();
        if (searcher == null) return null;
        List<LeafReaderContext> leafReaderContexts = searcher.getLeafContexts();
        if (leafReaderContexts == null || leafReaderContexts.isEmpty()) return null; // TODO: is empty check necessary?

        double overallMin = Double.MAX_VALUE;
        for (LeafReaderContext lrc : leafReaderContexts) {
            LeafReader reader = lrc.reader();
            String fieldName = ((ValuesSource.Numeric.FieldData) valuesSource).getIndexFieldName(); // TODO: This cannot be the best way
            try {
                PointValues pointValues = reader.getPointValues(fieldName);
                if (pointValues == null) return null;
                double leafMin = FloatPoint.decodeDimension(pointValues.getMinPackedValue(), 0); // TODO: Is FloatPoint always right?
                overallMin = Math.min(overallMin, leafMin);
            } catch (IOException e) {
                return null; // If we can't open PointValues to get the minimum value, return null
            }
        }
        return Math.floor((overallMin - offset) / interval);
    }

    @Override
    public ScoreMode scoreMode() {
        if (valuesSource != null && valuesSource.needsScores()) {
            return ScoreMode.COMPLETE;
        }
        return super.scoreMode();
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, final LeafBucketCollector sub) throws IOException {
        if (valuesSource == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }

        final SortedNumericDoubleValues values = valuesSource.doubleValues(ctx);
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long owningBucketOrd) throws IOException {
                if (values.advanceExact(doc)) {
                    final int valuesCount = values.docValueCount();

                    double previousKey = Double.NEGATIVE_INFINITY;
                    for (int i = 0; i < valuesCount; ++i) {
                        double value = values.nextValue();
                        double key = Math.floor((value - offset) / interval);
                        assert key >= previousKey;
                        if (key == previousKey) {
                            continue;
                        }
                        if (hardBounds == null || hardBounds.contain(key * interval)) {
                            // TODO: mapDoubleKeyToLong() has an if inside it. For performance reasons, would it be better if that `if` were
                            // outside, as its gonna run millions of times? I think prob not, as it should figure it out p quick?
                            long bucketOrd = bucketOrds.add(owningBucketOrd, mapDoubleKeyToLong(key));
                            if (bucketOrd < 0) { // already seen
                                bucketOrd = -1 - bucketOrd;
                                collectExistingBucket(sub, doc, bucketOrd);
                            } else {
                                collectBucket(sub, doc, bucketOrd);
                            }
                        }
                        previousKey = key;
                    }
                }
            }
        };
    }
}
