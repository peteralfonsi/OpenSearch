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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregatorTestCase;
import org.opensearch.search.aggregations.bucket.terms.LongKeyedBucketOrds;
import org.opensearch.search.aggregations.metrics.InternalMin;
import org.opensearch.search.aggregations.metrics.MinAggregationBuilder;
import org.opensearch.search.aggregations.support.AggregationInspectionHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class NumericHistogramAggregatorTests extends AggregatorTestCase {
    // Whether we use MinimumAwareBucketOrds depends on whether we can get PointValues from the reader.
    // In tests we can achieve this by using SortedNumericDocValuesField (no PointValues) or something like LongField (has PointValues).
    // The agg results should be the same regardless of bucketOrds implementation.

    public void testLongs() throws Exception {
        MappedFieldType fieldType = longField("field");
        HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("my_agg").field("field").interval(5);
        Consumer<InternalHistogram> checkingLambda = (histogram) -> {
            assertEquals(6, histogram.getBuckets().size());
            assertEquals(-10d, histogram.getBuckets().get(0).getKey());
            assertEquals(2, histogram.getBuckets().get(0).getDocCount());
            assertEquals(-5d, histogram.getBuckets().get(1).getKey());
            assertEquals(0, histogram.getBuckets().get(1).getDocCount());
            assertEquals(0d, histogram.getBuckets().get(2).getKey());
            assertEquals(1, histogram.getBuckets().get(2).getDocCount());
            assertEquals(5d, histogram.getBuckets().get(3).getKey());
            assertEquals(2, histogram.getBuckets().get(3).getDocCount());
            assertEquals(10d, histogram.getBuckets().get(4).getKey());
            assertEquals(0, histogram.getBuckets().get(4).getDocCount());
            assertEquals(15d, histogram.getBuckets().get(5).getKey());
            assertEquals(1, histogram.getBuckets().get(5).getDocCount());
            assertTrue(AggregationInspectionHelper.hasValue(histogram));
        };

        // Check with no PointValues
        CheckedConsumer<RandomIndexWriter, IOException> indexingLambda = (w) -> {
            for (long value : new long[] { 7, 3, -10, -6, 5, 15 }) {
                Document doc = new Document();
                doc.add(new SortedNumericDocValuesField("field", value));
                w.addDocument(doc);
            }
        };
        testWhileCheckingBucketOrds(aggBuilder, indexingLambda, checkingLambda, fieldType, LongKeyedBucketOrds.FromSingle.class);

        // Check with PointValues
        indexingLambda = (w) -> {
            for (long value : new long[] { 7, 3, -10, -6, 5, 15 }) {
                Document doc = new Document();
                doc.add(new LongField("field", value, Field.Store.NO));
                w.addDocument(doc);
            }
        };
        testWhileCheckingBucketOrds(
            aggBuilder,
            indexingLambda,
            checkingLambda,
            fieldType,
            LongKeyedBucketOrds.MinimumAwareBucketOrds.class
        );
    }

    public void testDoubles() throws Exception {
        MappedFieldType fieldType = doubleField("field");
        HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("my_agg").field("field").interval(5);
        Consumer<InternalHistogram> checkingLambda = (histogram) -> {
            assertEquals(6, histogram.getBuckets().size());
            assertEquals(6, histogram.getBuckets().size());
            assertEquals(-10d, histogram.getBuckets().get(0).getKey());
            assertEquals(2, histogram.getBuckets().get(0).getDocCount());
            assertEquals(-5d, histogram.getBuckets().get(1).getKey());
            assertEquals(0, histogram.getBuckets().get(1).getDocCount());
            assertEquals(0d, histogram.getBuckets().get(2).getKey());
            assertEquals(1, histogram.getBuckets().get(2).getDocCount());
            assertEquals(5d, histogram.getBuckets().get(3).getKey());
            assertEquals(2, histogram.getBuckets().get(3).getDocCount());
            assertEquals(10d, histogram.getBuckets().get(4).getKey());
            assertEquals(0, histogram.getBuckets().get(4).getDocCount());
            assertEquals(15d, histogram.getBuckets().get(5).getKey());
            assertEquals(1, histogram.getBuckets().get(5).getDocCount());
            assertTrue(AggregationInspectionHelper.hasValue(histogram));
        };

        // Check with no PointValues
        CheckedConsumer<RandomIndexWriter, IOException> indexingLambda = (w) -> {
            for (double value : new double[] { 9.3, 3.2, -10, -6.5, 5.3, 15.1 }) {
                Document doc = new Document();
                doc.add(new SortedNumericDocValuesField("field", NumericUtils.doubleToSortableLong(value)));
                w.addDocument(doc);
            }
        };
        testWhileCheckingBucketOrds(aggBuilder, indexingLambda, checkingLambda, fieldType, LongKeyedBucketOrds.FromSingle.class);

        // Check with PointValues
        indexingLambda = (w) -> {
            for (double value : new double[] { 9.3, 3.2, -10, -6.5, 5.3, 15.1 }) {
                Document doc = new Document();
                doc.add(new DoubleField("field", value, Field.Store.NO));
                w.addDocument(doc);
            }
        };
        testWhileCheckingBucketOrds(
            aggBuilder,
            indexingLambda,
            checkingLambda,
            fieldType,
            LongKeyedBucketOrds.MinimumAwareBucketOrds.class
        );

    }

    public void testDates() throws Exception {
        List<String> dataset = Arrays.asList(
            "2019-11-01T01:07:45",
            "2019-11-02T03:43:34",
            "2019-11-03T04:11:00",
            "2019-11-04T05:11:31",
            "2019-11-05T08:24:05",
            "2019-11-06T13:09:32",
            "2019-11-07T13:47:43",
            "2019-11-08T16:14:34",
            "2019-11-09T17:09:50",
            "2019-11-10T22:55:46"
        );

        String fieldName = "date_field";
        DateFieldMapper.DateFieldType fieldType = dateField(fieldName, DateFieldMapper.Resolution.MILLISECONDS);
        HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("my_agg").field(fieldName).interval(1000 * 60 * 60 * 24);
        Consumer<InternalHistogram> checkingLambda = (histogram) -> { assertTrue(AggregationInspectionHelper.hasValue(histogram)); };

        // Check with no PointValues
        CheckedConsumer<RandomIndexWriter, IOException> indexingLambda = (w) -> {
            Document document = new Document();
            for (String date : dataset) {
                long instant = fieldType.parse(date);
                document.add(new SortedNumericDocValuesField(fieldName, instant));
                w.addDocument(document);
                document.clear();
            }
        };
        testWhileCheckingBucketOrds(aggBuilder, indexingLambda, checkingLambda, fieldType, LongKeyedBucketOrds.FromSingle.class);

        // Check with PointValues
        indexingLambda = (w) -> {
            Document document = new Document();
            for (String date : dataset) {
                long instant = fieldType.parse(date);
                document.add(new LongField(fieldName, instant, Field.Store.NO));
                w.addDocument(document);
                document.clear();
            }
        };
        // For now it's expected to use FromSingle implementation as this is not a NumberFieldMapper.NumberFieldType
        testWhileCheckingBucketOrds(aggBuilder, indexingLambda, checkingLambda, fieldType, LongKeyedBucketOrds.FromSingle.class);
    }

    public void testIrrationalInterval() throws Exception {
        MappedFieldType fieldType = longField("field");
        HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("my_agg").field("field").interval(Math.PI);
        Consumer<InternalHistogram> checkingLambda = (histogram) -> {
            assertEquals(6, histogram.getBuckets().size());
            assertEquals(-4 * Math.PI, histogram.getBuckets().get(0).getKey());
            assertEquals(1, histogram.getBuckets().get(0).getDocCount());
            assertEquals(-3 * Math.PI, histogram.getBuckets().get(1).getKey());
            assertEquals(1, histogram.getBuckets().get(1).getDocCount());
            assertEquals(-2 * Math.PI, histogram.getBuckets().get(2).getKey());
            assertEquals(0, histogram.getBuckets().get(2).getDocCount());
            assertEquals(-Math.PI, histogram.getBuckets().get(3).getKey());
            assertEquals(0, histogram.getBuckets().get(3).getDocCount());
            assertEquals(0d, histogram.getBuckets().get(4).getKey());
            assertEquals(2, histogram.getBuckets().get(4).getDocCount());
            assertEquals(Math.PI, histogram.getBuckets().get(5).getKey());
            assertEquals(1, histogram.getBuckets().get(5).getDocCount());
            assertTrue(AggregationInspectionHelper.hasValue(histogram));
        };

        // Check with no PointValues
        CheckedConsumer<RandomIndexWriter, IOException> indexingLambda = (w) -> {
            for (long value : new long[] { 3, 2, -10, 5, -9 }) {
                Document doc = new Document();
                doc.add(new SortedNumericDocValuesField("field", value));
                w.addDocument(doc);
            }
        };
        testWhileCheckingBucketOrds(aggBuilder, indexingLambda, checkingLambda, fieldType, LongKeyedBucketOrds.FromSingle.class);

        // Check with PointValues
        indexingLambda = (w) -> {
            for (long value : new long[] { 3, 2, -10, 5, -9 }) {
                Document doc = new Document();
                doc.add(new LongField("field", value, Field.Store.NO));
                w.addDocument(doc);
            }
        };
        testWhileCheckingBucketOrds(
            aggBuilder,
            indexingLambda,
            checkingLambda,
            fieldType,
            LongKeyedBucketOrds.MinimumAwareBucketOrds.class
        );
    }

    public void testMinDocCount() throws Exception {
        MappedFieldType fieldType = longField("field");
        HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("my_agg").field("field").interval(10).minDocCount(2);
        Consumer<InternalHistogram> checkingLambda = (histogram) -> {
            assertEquals(2, histogram.getBuckets().size());
            assertEquals(-10d, histogram.getBuckets().get(0).getKey());
            assertEquals(2, histogram.getBuckets().get(0).getDocCount());
            assertEquals(0d, histogram.getBuckets().get(1).getKey());
            assertEquals(3, histogram.getBuckets().get(1).getDocCount());
            assertTrue(AggregationInspectionHelper.hasValue(histogram));
        };

        // Check with no PointValues
        CheckedConsumer<RandomIndexWriter, IOException> indexingLambda = (w) -> {
            for (long value : new long[] { 7, 3, -10, -6, 5, 50 }) {
                Document doc = new Document();
                doc.add(new SortedNumericDocValuesField("field", value));
                w.addDocument(doc);
            }
        };
        testWhileCheckingBucketOrds(aggBuilder, indexingLambda, checkingLambda, fieldType, LongKeyedBucketOrds.FromSingle.class);

        // Check with PointValues
        indexingLambda = (w) -> {
            for (long value : new long[] { 7, 3, -10, -6, 5, 50 }) {
                Document doc = new Document();
                doc.add(new LongField("field", value, Field.Store.NO));
                w.addDocument(doc);
            }
        };
        testWhileCheckingBucketOrds(
            aggBuilder,
            indexingLambda,
            checkingLambda,
            fieldType,
            LongKeyedBucketOrds.MinimumAwareBucketOrds.class
        );
    }

    public void testMissing() throws Exception {
        MappedFieldType fieldType = longField("field");
        HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("my_agg").field("field").interval(5).missing(2d);
        Consumer<InternalHistogram> checkingLambda = (histogram) -> {
            assertEquals(6, histogram.getBuckets().size());
            assertEquals(-10d, histogram.getBuckets().get(0).getKey());
            assertEquals(2, histogram.getBuckets().get(0).getDocCount());
            assertEquals(-5d, histogram.getBuckets().get(1).getKey());
            assertEquals(0, histogram.getBuckets().get(1).getDocCount());
            assertEquals(0d, histogram.getBuckets().get(2).getKey());
            assertEquals(7, histogram.getBuckets().get(2).getDocCount());
            assertEquals(5d, histogram.getBuckets().get(3).getKey());
            assertEquals(2, histogram.getBuckets().get(3).getDocCount());
            assertEquals(10d, histogram.getBuckets().get(4).getKey());
            assertEquals(0, histogram.getBuckets().get(4).getDocCount());
            assertEquals(15d, histogram.getBuckets().get(5).getKey());
            assertEquals(1, histogram.getBuckets().get(5).getDocCount());
            assertTrue(AggregationInspectionHelper.hasValue(histogram));
        };

        // Check with no PointValues
        CheckedConsumer<RandomIndexWriter, IOException> indexingLambda = (w) -> {
            for (long value : new long[] { 7, 3, -10, -6, 5, 15 }) {
                Document doc = new Document();
                doc.add(new SortedNumericDocValuesField("field", value));
                w.addDocument(doc);
                w.addDocument(new Document());
            }
        };
        testWhileCheckingBucketOrds(aggBuilder, indexingLambda, checkingLambda, fieldType, LongKeyedBucketOrds.FromSingle.class);

        // Check with PointValues
        indexingLambda = (w) -> {
            for (long value : new long[] { 7, 3, -10, -6, 5, 15 }) {
                Document doc = new Document();
                doc.add(new LongField("field", value, Field.Store.NO));
                w.addDocument(doc);
                w.addDocument(new Document());
            }
        };
        // For now it's expected to use FromSingle implementation as the ValuesSource class is MissingValues rather than
        // ValuesSource.Numeric.FieldData.
        testWhileCheckingBucketOrds(aggBuilder, indexingLambda, checkingLambda, fieldType, LongKeyedBucketOrds.FromSingle.class);
    }

    public void testMissingUnmappedField() throws Exception {
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            for (int i = 0; i < 7; i++) {
                Document doc = new Document();
                w.addDocument(doc);
            }

            HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("my_agg").field("field").interval(5).missing(2d);
            MappedFieldType type = null;
            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalHistogram histogram = searchAndReduce(searcher, new MatchAllDocsQuery(), aggBuilder, type);

                assertEquals(1, histogram.getBuckets().size());

                assertEquals(0d, histogram.getBuckets().get(0).getKey());
                assertEquals(7, histogram.getBuckets().get(0).getDocCount());

                assertTrue(AggregationInspectionHelper.hasValue(histogram));

                NumericHistogramAggregator agg = (NumericHistogramAggregator) createAggregator(aggBuilder, searcher, type);
                assertEquals(LongKeyedBucketOrds.FromSingle.class, agg.getBucketOrds().getClass());
            }
        }
    }

    public void testMissingUnmappedFieldBadType() throws Exception {
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            for (int i = 0; i < 7; i++) {
                w.addDocument(new Document());
            }

            String missingValue = "🍌🍌🍌";
            HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("my_agg").field("field")
                .interval(5)
                .missing(missingValue);
            MappedFieldType type = null;
            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                Throwable t = expectThrows(
                    IllegalArgumentException.class,
                    () -> { searchAndReduce(searcher, new MatchAllDocsQuery(), aggBuilder, type); }
                );
                // This throws a number format exception (which is a subclass of IllegalArgumentException) and might be ok?
                assertThat(t.getMessage(), containsString(missingValue));
            }
        }
    }

    public void testIncorrectFieldType() throws Exception {
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            for (String value : new String[] { "foo", "bar", "baz", "quux" }) {
                Document doc = new Document();
                doc.add(new SortedSetDocValuesField("field", new BytesRef(value)));
                w.addDocument(doc);
            }

            HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("my_agg").field("field").interval(5);
            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);

                expectThrows(IllegalArgumentException.class, () -> {
                    searchAndReduce(searcher, new MatchAllDocsQuery(), aggBuilder, keywordField("field"));
                });
            }
        }

    }

    public void testOffset() throws Exception {
        MappedFieldType fieldType = doubleField("field");
        HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("my_agg").field("field").interval(5).offset(Math.PI);
        Consumer<InternalHistogram> checkingLambda = (histogram) -> {
            assertEquals(4, histogram.getBuckets().size());
            assertEquals(-10 + Math.PI, histogram.getBuckets().get(0).getKey());
            assertEquals(2, histogram.getBuckets().get(0).getDocCount());
            assertEquals(-5 + Math.PI, histogram.getBuckets().get(1).getKey());
            assertEquals(0, histogram.getBuckets().get(1).getDocCount());
            assertEquals(Math.PI, histogram.getBuckets().get(2).getKey());
            assertEquals(2, histogram.getBuckets().get(2).getDocCount());
            assertEquals(5 + Math.PI, histogram.getBuckets().get(3).getKey());
            assertEquals(1, histogram.getBuckets().get(3).getDocCount());
            assertTrue(AggregationInspectionHelper.hasValue(histogram));
        };

        // Check with no PointValues
        CheckedConsumer<RandomIndexWriter, IOException> indexingLambda = (w) -> {
            for (double value : new double[] { 9.3, 3.2, -5, -6.5, 5.3 }) {
                Document doc = new Document();
                doc.add(new SortedNumericDocValuesField("field", NumericUtils.doubleToSortableLong(value)));
                w.addDocument(doc);
            }
        };
        testWhileCheckingBucketOrds(aggBuilder, indexingLambda, checkingLambda, fieldType, LongKeyedBucketOrds.FromSingle.class);

        // Check with PointValues
        indexingLambda = (w) -> {
            for (double value : new double[] { 9.3, 3.2, -5, -6.5, 5.3 }) {
                Document doc = new Document();
                doc.add(new DoubleField("field", value, Field.Store.NO));
                w.addDocument(doc);
            }
        };
        testWhileCheckingBucketOrds(
            aggBuilder,
            indexingLambda,
            checkingLambda,
            fieldType,
            LongKeyedBucketOrds.MinimumAwareBucketOrds.class
        );
    }

    public void testRandomOffset() throws Exception {
        MappedFieldType fieldType = doubleField("field");
        final double offset = randomDouble();
        final double interval = 5;
        final double expectedOffset = offset % interval;
        HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("my_agg").field("field").interval(interval).offset(offset);
        Consumer<InternalHistogram> checkingLambda = (histogram) -> {
            assertEquals(4, histogram.getBuckets().size());

            assertEquals(-10 + expectedOffset, histogram.getBuckets().get(0).getKey());
            assertEquals(1, histogram.getBuckets().get(0).getDocCount());

            assertEquals(-5 + expectedOffset, histogram.getBuckets().get(1).getKey());
            assertEquals(0, histogram.getBuckets().get(1).getDocCount());

            assertEquals(expectedOffset, histogram.getBuckets().get(2).getKey());
            assertEquals(1, histogram.getBuckets().get(2).getDocCount());

            assertEquals(5 + expectedOffset, histogram.getBuckets().get(3).getKey());
            assertEquals(1, histogram.getBuckets().get(3).getDocCount());

            assertTrue(AggregationInspectionHelper.hasValue(histogram));
        };

        // Check with no PointValues
        CheckedConsumer<RandomIndexWriter, IOException> indexingLambda = (w) -> {
            for (double value : new double[] { 9.3, 3.2, -5 }) {
                Document doc = new Document();
                doc.add(new SortedNumericDocValuesField("field", NumericUtils.doubleToSortableLong(value)));
                w.addDocument(doc);
            }
        };
        testWhileCheckingBucketOrds(aggBuilder, indexingLambda, checkingLambda, fieldType, LongKeyedBucketOrds.FromSingle.class);

        // Check with PointValues
        indexingLambda = (w) -> {
            for (double value : new double[] { 9.3, 3.2, -5 }) {
                Document doc = new Document();
                doc.add(new DoubleField("field", value, Field.Store.NO));
                w.addDocument(doc);
            }
        };
        testWhileCheckingBucketOrds(
            aggBuilder,
            indexingLambda,
            checkingLambda,
            fieldType,
            LongKeyedBucketOrds.MinimumAwareBucketOrds.class
        );
    }

    public void testExtendedBounds() throws Exception {
        MappedFieldType fieldType = doubleField("field");
        HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("my_agg").field("field")
            .interval(5)
            .extendedBounds(-12, 13);
        Consumer<InternalHistogram> checkingLambda = (histogram) -> {
            assertEquals(6, histogram.getBuckets().size());
            assertEquals(-15d, histogram.getBuckets().get(0).getKey());
            assertEquals(0, histogram.getBuckets().get(0).getDocCount());
            assertEquals(-10d, histogram.getBuckets().get(1).getKey());
            assertEquals(0, histogram.getBuckets().get(1).getDocCount());
            assertEquals(-5d, histogram.getBuckets().get(2).getKey());
            assertEquals(2, histogram.getBuckets().get(2).getDocCount());
            assertEquals(0d, histogram.getBuckets().get(3).getKey());
            assertEquals(2, histogram.getBuckets().get(3).getDocCount());
            assertEquals(5d, histogram.getBuckets().get(4).getKey());
            assertEquals(0, histogram.getBuckets().get(4).getDocCount());
            assertEquals(10d, histogram.getBuckets().get(5).getKey());
            assertEquals(0, histogram.getBuckets().get(5).getDocCount());
            assertTrue(AggregationInspectionHelper.hasValue(histogram));
        };

        // Check with no PointValues
        CheckedConsumer<RandomIndexWriter, IOException> indexingLambda = (w) -> {
            for (double value : new double[] { 3.2, -5, -4.5, 4.3 }) {
                Document doc = new Document();
                doc.add(new SortedNumericDocValuesField("field", NumericUtils.doubleToSortableLong(value)));
                w.addDocument(doc);
            }
        };
        testWhileCheckingBucketOrds(aggBuilder, indexingLambda, checkingLambda, fieldType, LongKeyedBucketOrds.FromSingle.class);

        // Check with PointValues
        indexingLambda = (w) -> {
            for (double value : new double[] { 3.2, -5, -4.5, 4.3 }) {
                Document doc = new Document();
                doc.add(new DoubleField("field", value, Field.Store.NO));
                w.addDocument(doc);
            }
        };
        testWhileCheckingBucketOrds(
            aggBuilder,
            indexingLambda,
            checkingLambda,
            fieldType,
            LongKeyedBucketOrds.MinimumAwareBucketOrds.class
        );
    }

    public void testAsSubAgg() throws Exception {
        AggregationBuilder request = new HistogramAggregationBuilder("outer").field("outer")
            .interval(5)
            .subAggregation(
                new HistogramAggregationBuilder("inner").field("inner")
                    .interval(5)
                    .subAggregation(new MinAggregationBuilder("min").field("n"))
            );
        CheckedConsumer<RandomIndexWriter, IOException> buildIndexWithoutPointValues = iw -> {
            List<List<IndexableField>> docs = new ArrayList<>();
            for (int n = 0; n < 10000; n++) {
                docs.add(
                    List.of(
                        new SortedNumericDocValuesField("outer", n % 100),
                        new SortedNumericDocValuesField("inner", n / 100),
                        new SortedNumericDocValuesField("n", n)
                    )
                );
            }
            iw.addDocuments(docs);
        };
        CheckedConsumer<RandomIndexWriter, IOException> buildIndexWithPointValues = iw -> {
            List<List<IndexableField>> docs = new ArrayList<>();
            for (int n = 0; n < 10000; n++) {
                docs.add(
                    List.of(
                        new LongField("outer", n % 100, Field.Store.NO),
                        new LongField("inner", n / 100, Field.Store.NO),
                        new LongField("n", n, Field.Store.NO)
                    )
                );
            }
            iw.addDocuments(docs);
        };
        Consumer<InternalHistogram> verify = outer -> {
            assertThat(outer.getBuckets(), hasSize(20));
            for (int outerIdx = 0; outerIdx < 20; outerIdx++) {
                InternalHistogram.Bucket outerBucket = outer.getBuckets().get(outerIdx);
                assertThat(outerBucket.getKey(), equalTo(5.0 * outerIdx));
                InternalHistogram inner = outerBucket.getAggregations().get("inner");
                assertThat(inner.getBuckets(), hasSize(20));
                for (int innerIdx = 0; innerIdx < 20; innerIdx++) {
                    InternalHistogram.Bucket innerBucket = inner.getBuckets().get(innerIdx);
                    assertThat(innerBucket.getKey(), equalTo(5.0 * innerIdx));
                    InternalMin min = innerBucket.getAggregations().get("min");
                    assertThat(min.getValue(), equalTo(outerIdx * 5.0 + innerIdx * 500.0));
                }
            }
        };

        // Without PointValues, the outer agg should use FromSingle and the inner should use FromMany for bucketOrds
        testSubAggWhileCheckingBucketOrds(
            request,
            buildIndexWithoutPointValues,
            verify,
            LongKeyedBucketOrds.FromSingle.class,
            LongKeyedBucketOrds.FromMany.class,
            longField("outer"),
            longField("inner"),
            longField("n")
        );
        // With PointValues, the outer agg should use MinimumAwareBucketOrds and the inner should use FromMany for bucketOrds
        testSubAggWhileCheckingBucketOrds(
            request,
            buildIndexWithPointValues,
            verify,
            LongKeyedBucketOrds.MinimumAwareBucketOrds.class,
            LongKeyedBucketOrds.FromMany.class,
            longField("outer"),
            longField("inner"),
            longField("n")
        );
    }

    // We can't use just testCase() since bucketOrds is not available from InternalHistogram
    private void testWhileCheckingBucketOrds(
        HistogramAggregationBuilder builder,
        CheckedConsumer<RandomIndexWriter, IOException> indexingLambda,
        Consumer<InternalHistogram> checkingLambda,
        MappedFieldType mappedFieldType,
        Class<? extends LongKeyedBucketOrds> expectedBucketOrdsImpl
    ) throws Exception {
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            indexingLambda.accept(w);
            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalHistogram histogram = searchAndReduce(searcher, new MatchAllDocsQuery(), builder, mappedFieldType);
                checkingLambda.accept(histogram);
                // searchAndReduce() doesn't expose the actual built aggregator, but we need to check its bucketOrds implementation is as
                // expected.
                // So, build a second NumericHistogramAggregator using the same builder, and check that one.
                NumericHistogramAggregator agg = (NumericHistogramAggregator) createAggregator(builder, searcher, mappedFieldType);
                assertEquals(expectedBucketOrdsImpl, agg.getBucketOrds().getClass());
            }
        }
    }

    private void testSubAggWhileCheckingBucketOrds(
        AggregationBuilder builder,
        CheckedConsumer<RandomIndexWriter, IOException> indexingLambda,
        Consumer<InternalHistogram> checkingLambda,
        Class<? extends LongKeyedBucketOrds> expectedOuterBucketOrdsImpl,
        Class<? extends LongKeyedBucketOrds> expectedInnerBucketOrdsImpl,
        MappedFieldType... fieldTypes
    ) throws Exception {
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            indexingLambda.accept(w);
            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalHistogram histogram = searchAndReduce(searcher, new MatchAllDocsQuery(), builder, fieldTypes);
                checkingLambda.accept(histogram);

                NumericHistogramAggregator agg = (NumericHistogramAggregator) createAggregator(builder, searcher, fieldTypes);
                assertEquals(expectedOuterBucketOrdsImpl, agg.getBucketOrds().getClass());
                NumericHistogramAggregator innerAgg = (NumericHistogramAggregator) agg.subAggregators()[0];
                assertEquals(expectedInnerBucketOrdsImpl, innerAgg.getBucketOrds().getClass());
            }
        }
    }
}
