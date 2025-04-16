/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.query;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Class with helper utils for creating the complement of range queries. These can be useful for boolean query rewrites.
 * TODO: Instead of this class, could we make Lucene changes allowing PointRangeQuery itself to just return the inverse, similar to optimization on PointRangeQuery line 346?
 */
public class RangeQueryComplementUtils {
    /**
     * Returns a list of queries which, when OR-ed together, make up the complement of the range query passed in.
     * The complement can only be returned under the assumption each document in the relevant field has exactly 1 value, and the query is 1-dimensional.
     * TODO: Expand this to handle the 0-value case.
     * If the input Query implementation isn't supported, null can be returned instead.
     * PointRangeQuery and IndexOrDocValuesQuery wrapping PointRangeQuery are currently supported, for DateFieldType only.
     * @return the complement.
     */
    public static List<Query> getComplement(Query query, QueryShardContext qsContext, IndexSearcher indexSearcher) {
        if (query instanceof PointRangeQuery prq) {
            return getPointRangeQueryComplement(prq, qsContext, indexSearcher);
        }
        if (query instanceof IndexOrDocValuesQuery iodvq) {
            return getIndexOrDocValuesQueryComplement(iodvq, qsContext, indexSearcher);
        }
        return null;
    }

    private static List<Query> getPointRangeQueryComplement(PointRangeQuery prq, QueryShardContext qsContext, IndexSearcher indexSearcher) {
        // Return the complement as a list of clauses, which can be OR-ed together for a runnable query.
        if (!checkIfPointRangeQueryEligible(prq, indexSearcher)) {
            return null;
        }
        MappedFieldType fieldType = qsContext.getFieldType(prq.getField());
        if (fieldType instanceof DateFieldMapper.DateFieldType dft) {
            return getDateFieldComplement(prq, qsContext, dft);
        }
        return null;
    }

    private static boolean checkIfPointRangeQueryEligible(PointRangeQuery prq, IndexSearcher indexSearcher) {
        if (prq.getNumDims() != 1) {
            return false;
        }
        // Ensure no segment has any docs without exactly 1 value for this field. If so, running boolean queries with the complement won't behave as expected
        // TODO: Make it work for missing values?
        // TODO: Handling for null / errors in getting contexts or reader
        for (LeafReaderContext lrc : indexSearcher.getLeafContexts()) {
            LeafReader reader = lrc.reader();
            PointValues values;
            try {
                values = reader.getPointValues(prq.getField()); // TODO: Is this an expensive operation?? I don't think it is since PointRangeQuery does it a few times... but not sure.
            } catch (IOException e) {
                // If we can't get PointValues to check on the number of values per doc, assume the query is ineligible
                return false;
            }
            if (!(values.getDocCount() == reader.maxDoc() && values.getDocCount() == values.size())) {
                return false;
            }
        }
        return true;
    }

    private static List<Query> getDateFieldComplement(PointRangeQuery prq, QueryShardContext qsContext, DateFieldMapper.DateFieldType fieldType) {
        DateFieldMapper.Resolution resolution = fieldType.resolution();
        if (resolution != DateFieldMapper.Resolution.MILLISECONDS) {
            // TODO: Allow this for nanosecond-resolution date fields
            return null;
        }
        long l = resolution.parsePointAsMillis(prq.getLowerPoint());
        long u = resolution.parsePointAsMillis(prq.getUpperPoint());

        if (l == Long.MIN_VALUE && u == Long.MAX_VALUE) {
            // This should not happen, but if it does the complement should be MatchNoDocsQuery
            return List.of(new MatchNoDocsQuery());
        }

        List<Query> complement = new ArrayList<>();

        // Handle the lower section of the complement, if present
        // The logic inside DateFieldType.rangeQuery handles everything around IndexOrDocValuesQuery, we don't have to construct them ourselves
        // Lucene point range queries have inclusive bounds, so we never want to include the bound from the original query when building our new query.
        if (l != Long.MIN_VALUE) {
            complement.add(
                fieldType.rangeQuery(
                    resolution.toInstant(Long.MIN_VALUE), // TODO: not sure about this but it appears to work
                    resolution.toInstant(l),
                    true,
                    false,
                    null,
                    null,
                    null,
                    qsContext
                ));
        }
        // Handle the upper section of the complement, if present
        if (u != Long.MAX_VALUE) {
            complement.add(
                fieldType.rangeQuery(
                    resolution.toInstant(u),
                    resolution.toInstant(Long.MAX_VALUE),
                    false,
                    true,
                    null,
                    null,
                    null,
                    qsContext
                ));
        }
        return complement;
    }

    /*private static List<Query> getPointRangeQueryComplementList(PointRangeQuery prq, MappedFieldType fieldType) {
        // Return the complement as a list of clauses, which can be OR-ed together for a runnable query.
        // If there are two items in the list, the one representing the lower range is first.
        if (prq.getNumDims() != 1) {
            return null;
        }
        if (fieldType instanceof DateFieldMapper.DateFieldType) {
            // DateFieldType ultimately uses LongPoint range queries.
            long[] bounds = getLongBounds(prq);
            long l = bounds[0];
            long u = bounds[1];

            if (l == Long.MIN_VALUE && u == Long.MAX_VALUE) {
                // This should not happen, but if it does the complement should be MatchNoDocsQuery
                return List.of(new MatchNoDocsQuery());
            }

            List<Query> complement = new ArrayList<>();
            // Handle the lower section of the complement, if present
            if (l != Long.MIN_VALUE) {
                complement.add(LongPoint.newRangeQuery(prq.getField(), Long.MIN_VALUE, l - 1));
            }
            // Handle the upper section of the complement, if present
            if (u != Long.MAX_VALUE) {
                 complement.add(LongPoint.newRangeQuery(prq.getField(), u + 1, Long.MAX_VALUE));
            }
            return complement;
        }
        return null;
    }*/

    /*long[] bounds = getLongBounds(prq);
        long l = bounds[0];
        long u = bounds[1];*/

    /*private static long[] getLongBounds(PointRangeQuery prq) {
        // By the time we read these from the PointRangeQuery, gte/lte, timezone, and similar things are already taken into account.
        byte[] lowerPoint = prq.getLowerPoint();
        byte[] upperPoint = prq.getUpperPoint();

        long[] lowerPointLong = new long[1]; // TODO: This should only ever require 1 long since we checked for 1-D?
        LongPoint.unpack(new BytesRef(lowerPoint), 0, lowerPointLong);
        long[] upperPointLong = new long[1];
        LongPoint.unpack(new BytesRef(upperPoint), 0, upperPointLong);

        long l = lowerPointLong[0];
        long u = upperPointLong[0];
        return new long[]{l, u};
    }*/

    private static List<Query> getIndexOrDocValuesQueryComplement(IndexOrDocValuesQuery iodvq, QueryShardContext qsContext, IndexSearcher indexSearcher) {
        if (iodvq.getIndexQuery() instanceof PointRangeQuery prq) {
            return getPointRangeQueryComplement(prq, qsContext, indexSearcher);
        }
        return null;
    }
}
