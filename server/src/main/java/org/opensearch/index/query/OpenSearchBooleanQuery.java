/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.query;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.opensearch.search.internal.ContextIndexSearcher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A wrapper over Lucene's BooleanQuery that has extra rewrite logic. This is for rewrite logic that can't
 * be handled in the query builder, for example if it needs to check whether all docs in the shard have exactly 1 value.
 */
public class OpenSearchBooleanQuery extends Query {
    private static final List<BooleanClause.Occur> nonMustNotOccurTypes = List.of(BooleanClause.Occur.SHOULD, BooleanClause.Occur.MUST, BooleanClause.Occur.FILTER); // Avoid recreating this each time
    final BooleanQuery wrapped;
    OpenSearchBooleanQuery(BooleanQuery wrapped) {
        this.wrapped = wrapped;
    }

    // TODO: break up this fn
    private BooleanQuery rewriteMustNotRangeClausesToShould(IndexSearcher indexSearcher) throws IOException {
        // Return early if we don't have access to QueryShardContext
        QueryShardContext qsContext = getQueryShardContext(indexSearcher);
        if (qsContext == null) {
            return wrapped;
        }

        Collection<Query> mustNotClauses = wrapped.getClauses(BooleanClause.Occur.MUST_NOT);
        List<Query> ineligibleMustNotClauses = new ArrayList<>();
        // For now, only handle the case where there's exactly 1 range query for this field.
        Map<String, Integer> fieldCounts = new HashMap<>();
        Set<Query> rangeQueries = new HashSet<>();
        for (Query clause : mustNotClauses) {
            String prqField = getPointRangeQueryField(clause);
            if (prqField != null) {
                fieldCounts.merge(prqField, 1, Integer::sum);
                rangeQueries.add(clause);
            } else {
                ineligibleMustNotClauses.add(clause);
            }
        }

        boolean changed = false;
        BooleanQuery.Builder rewritten = new BooleanQuery.Builder();

        for (Query clause : rangeQueries) {
            if (fieldCounts.get(getPointRangeQueryField(clause)) != 1) {
                // There's more than one MUST_NOT clause for this field. Don't rewrite it.
                ineligibleMustNotClauses.add(clause);
                continue;
            }

            List<Query> complement = RangeQueryComplementUtils.getComplement(clause, qsContext, indexSearcher);
            if (complement == null) {
                // Couldn't get the complement for this clause. Don't rewrite it.
                ineligibleMustNotClauses.add(clause);
                continue;
            }
            BooleanQuery.Builder nestedBooleanQuery = new BooleanQuery.Builder();
            nestedBooleanQuery.setMinimumNumberShouldMatch(1);
            for (Query complementComponent : complement) {
                nestedBooleanQuery.add(complementComponent, BooleanClause.Occur.SHOULD);
            }

            changed = true;
            rewritten.add(nestedBooleanQuery.build(), BooleanClause.Occur.MUST);
        }


        if (changed) {
            // If there were originally should clauses and no must/filter clauses, null minimumShouldMatch is set to a default of 1
            // within Lucene.
            // But if there was originally a must or filter clause, the default is 0.
            // If we added a must clause due to this rewrite, we should respect what the original default would have been.
            // TODO: Test this still works, and that defaults behave as I expect when working at this level.
            if (!wrapped.getClauses(BooleanClause.Occur.SHOULD).isEmpty()
                && wrapped.getClauses(BooleanClause.Occur.MUST).isEmpty()
                && wrapped.getClauses(BooleanClause.Occur.FILTER).isEmpty()) {
                rewritten.setMinimumNumberShouldMatch(1);
            }

            // Add original clauses which were not rewritten to the new query
            for (BooleanClause.Occur occur : nonMustNotOccurTypes) {
                for (Query clause : wrapped.getClauses(occur)) {
                    rewritten.add(clause, occur);
                }
            }
            for (Query ineligibleMustNotClause : ineligibleMustNotClauses) {
                rewritten.add(ineligibleMustNotClause, BooleanClause.Occur.MUST_NOT);
            }
            return rewritten.build();
        }
        return wrapped;
    }

    private QueryShardContext getQueryShardContext(IndexSearcher indexSearcher) {
        if (indexSearcher instanceof ContextIndexSearcher cis) {
            return cis.getQueryShardContext();
        }
        return null;
    }

    private String getPointRangeQueryField(Query clause) {
        // Check if the query is PointRangeQuery or wraps PointRangeQuery. If so, return its field. If not, return null.
        if (clause instanceof PointRangeQuery prq) {
            return prq.getField();
        }
        if (clause instanceof IndexOrDocValuesQuery iodvq) {
            if (iodvq.getIndexQuery() instanceof PointRangeQuery prq) {
                return prq.getField();
            }
        }
        return null;
    }

    @Override
    public Query rewrite(IndexSearcher indexSearcher) throws IOException {
        try {
            // rewrite() is repeatedly called until the query stops changing, so BooleanQuery.rewrite() logic for the wrapped query
            // should be handled automatically
            return rewriteMustNotRangeClausesToShould(indexSearcher);
        } catch (IOException e) {
            return wrapped;
        }
    }

    @Override
    public String toString(String field) {
        return wrapped.toString(field);
    }

    @Override
    public void visit(QueryVisitor visitor) {
        // This shouldn't end up being called since rewrite() returns BooleanQuery anyway, but this will be correct if it somehow happens.
        wrapped.visit(visitor);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof OpenSearchBooleanQuery)) {
            return false;
        }
        return wrapped.equals(((OpenSearchBooleanQuery) obj).wrapped);
    }

    @Override
    public int hashCode() {
        return 31 * wrapped.hashCode();
    }

    // pkg-private for testing
    Query getWrapped() {
        return wrapped;
    }
}
