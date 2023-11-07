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
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.internal.AliasFilter;
import org.opensearch.search.internal.ShardSearchContextId;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Optional;

public class DiskTierTookTimePolicyTests extends OpenSearchTestCase {
    private DiskTierTookTimePolicy getTookTimePolicy() {
        // dummy settings
        Settings dummySettings = Settings.EMPTY;
        ClusterSettings dummyClusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        return new DiskTierTookTimePolicy(dummySettings, dummyClusterSettings);
    }

    public void testQSRSetupFunction() throws IOException {
        Long ttn = 100000000000L;
        BytesReference qsrBytes = getQSRBytesReference(ttn);
        QuerySearchResult qsr = new QuerySearchResult(qsrBytes.streamInput());
        assertEquals(ttn, qsr.getTookTimeNanos());
    }

    public void testBadBytesReference() throws Exception {
        BytesReference badQSR = new BytesArray("I love bytes!!!");
        DiskTierTookTimePolicy tookTimePolicy = getTookTimePolicy();
        assertThrows(IOException.class, () -> tookTimePolicy.checkData(badQSR));
    }

    public void testTookTimePolicy() throws Exception {
        DiskTierTookTimePolicy tookTimePolicy = getTookTimePolicy();

        // manually set threshold for test
        double threshMillis = 10;
        long shortMillis = (long) (0.9 * threshMillis);
        long longMillis = (long) (1.5 * threshMillis);
        tookTimePolicy.setThreshold(new TimeValue((long) threshMillis));
        BytesReference shortQSR = getQSRBytesReference(shortMillis * 1000000);
        BytesReference longQSR = getQSRBytesReference(longMillis * 1000000);

        boolean shortResult = tookTimePolicy.checkData(shortQSR);
        assertFalse(shortResult);
        boolean longResult = tookTimePolicy.checkData(longQSR);
        assertTrue(longResult);
    }

     static BytesReference getQSRBytesReference(long tookTimeNanos) throws IOException {
        // package-private, also used by EhCacheDiskCachingTierTests.java
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
        BytesStreamOutput out = new BytesStreamOutput();
        // it appears to need a boolean and then a ShardSearchContextId written to the stream before the QSR in order to deserialize?
        out.writeBoolean(false);
        id.writeTo(out);

        result.writeToNoId(out);
        return out.bytes();
    }
}
