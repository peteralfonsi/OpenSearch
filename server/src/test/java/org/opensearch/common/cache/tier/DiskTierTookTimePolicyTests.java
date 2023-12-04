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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.common.cache.tier;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.opensearch.action.OriginalIndices;
import org.opensearch.action.OriginalIndicesTests;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.common.UUIDs;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.indices.IndicesRequestCache;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.internal.AliasFilter;
import org.opensearch.search.internal.ShardSearchContextId;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.function.Function;

public class DiskTierTookTimePolicyTests extends OpenSearchTestCase {
    private final Function<BytesReference, CachePolicyInfoWrapper> transformationFunction = (data) -> {
        try {
            return IndicesRequestCache.getPolicyInfo(data);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    };

    private DiskTierTookTimePolicy getTookTimePolicy() {
        // dummy settings
        Settings dummySettings = Settings.EMPTY;
        ClusterSettings dummyClusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        return new DiskTierTookTimePolicy(dummySettings, dummyClusterSettings, transformationFunction);
    }

    public void testQSRSetupFunction() throws IOException {
        Long ttn = 100000000000L;
        QuerySearchResult qsr = getQSR(ttn);
        assertEquals(ttn, qsr.getTookTimeNanos());
    }

    public void testTookTimePolicy() throws Exception {
        DiskTierTookTimePolicy tookTimePolicy = getTookTimePolicy();

        // manually set threshold for test
        double threshMillis = 10;
        long shortMillis = (long) (0.9 * threshMillis);
        long longMillis = (long) (1.5 * threshMillis);
        tookTimePolicy.setThreshold(new TimeValue((long) threshMillis));
        BytesReference shortTime = getValidPolicyInput(getQSR(shortMillis * 1000000));
        BytesReference longTime = getValidPolicyInput(getQSR(longMillis * 1000000));

        boolean shortResult = tookTimePolicy.checkData(shortTime);
        assertFalse(shortResult);
        boolean longResult = tookTimePolicy.checkData(longTime);
        assertTrue(longResult);

        DiskTierTookTimePolicy disabledPolicy = getTookTimePolicy();
        disabledPolicy.setThreshold(TimeValue.ZERO);
        shortResult = disabledPolicy.checkData(shortTime);
        assertTrue(shortResult);
        longResult = disabledPolicy.checkData(longTime);
        assertTrue(longResult);
    }

    public static QuerySearchResult getQSR(long tookTimeNanos) {
        // package-private, also used by IndicesRequestCacheTests.java
        // setup from QuerySearchResultTests.java
        ShardId shardId = new ShardId("index", "uuid", randomInt());
        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(randomBoolean());
        ShardSearchRequest shardSearchRequest = new ShardSearchRequest(
            OriginalIndicesTests.randomOriginalIndices(),
            searchRequest,
            shardId,
            1,
            new AliasFilter(null, Strings.EMPTY_ARRAY),
            1.0f,
            randomNonNegativeLong(),
            null,
            new String[0]
        );
        ShardSearchContextId id = new ShardSearchContextId(UUIDs.base64UUID(), randomLong());
        QuerySearchResult result = new QuerySearchResult(
            id,
            new SearchShardTarget("node", shardId, null, OriginalIndices.NONE),
            shardSearchRequest
        );
        TopDocs topDocs = new TopDocs(new TotalHits(randomLongBetween(0, Long.MAX_VALUE), TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]);
        result.topDocs(new TopDocsAndMaxScore(topDocs, randomBoolean() ? Float.NaN : randomFloat()), new DocValueFormat[0]);

        result.setTookTimeNanos(tookTimeNanos);
        return result;
    }

    private BytesReference getValidPolicyInput(QuerySearchResult qsr) throws IOException {
        // When it's used in the cache, the policy will receive BytesReferences which have a CachePolicyInfoWrapper
        // at the beginning of them, followed by the actual QSR.
        CachePolicyInfoWrapper policyInfo = new CachePolicyInfoWrapper(qsr.getTookTimeNanos());
        BytesStreamOutput out = new BytesStreamOutput();
        policyInfo.writeTo(out);
        qsr.writeTo(out);
        return out.bytes();
    }
}
