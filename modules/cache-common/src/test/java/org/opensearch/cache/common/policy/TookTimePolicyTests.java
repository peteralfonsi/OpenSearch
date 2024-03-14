/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cache.common.policy;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.opensearch.common.Randomness;
import org.opensearch.common.cache.policy.CachedQueryResult;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Random;
import java.util.function.Function;

public class TookTimePolicyTests extends OpenSearchTestCase {
    private final Function<BytesReference, CachedQueryResult.PolicyValues> transformationFunction = (data) -> {
        try {
            return CachedQueryResult.getPolicyValues(data);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    };

    private TookTimePolicy<BytesReference> getTookTimePolicy(TimeValue threshold) {
        return new TookTimePolicy<>(threshold, transformationFunction);
    }

    public void testTookTimePolicy() throws Exception {
        double threshMillis = 10;
        long shortMillis = (long) (0.9 * threshMillis);
        long longMillis = (long) (1.5 * threshMillis);
        TookTimePolicy<BytesReference> tookTimePolicy = getTookTimePolicy(new TimeValue((long) threshMillis));
        BytesReference shortTime = getValidPolicyInput(shortMillis * 1000000);
        BytesReference longTime = getValidPolicyInput(longMillis * 1000000);

        boolean shortResult = tookTimePolicy.test(shortTime);
        assertFalse(shortResult);
        boolean longResult = tookTimePolicy.test(longTime);
        assertTrue(longResult);

        TookTimePolicy<BytesReference> disabledPolicy = getTookTimePolicy(TimeValue.ZERO);
        shortResult = disabledPolicy.test(shortTime);
        assertTrue(shortResult);
        longResult = disabledPolicy.test(longTime);
        assertTrue(longResult);
    }

    public void testMissingWrapper() throws Exception {
        TookTimePolicy<BytesReference> tookTimePolicy = getTookTimePolicy(TimeValue.ZERO);
        BytesStreamOutput out = new BytesStreamOutput();
        getQSR().writeToNoId(out);
        BytesReference missingWrapper = out.bytes();
        boolean allowedMissingWrapper = tookTimePolicy.test(missingWrapper);
        assertFalse(allowedMissingWrapper);
    }

    public void testNegativeOneInput() throws Exception {
        // PolicyValues with -1 took time can be passed to this policy if we shouldn't accept it for whatever reason
        TookTimePolicy<BytesReference> tookTimePolicy = getTookTimePolicy(TimeValue.ZERO);
        BytesReference minusOne = getValidPolicyInput(-1L);
        assertFalse(tookTimePolicy.test(minusOne));
    }

    private BytesReference getValidPolicyInput(Long tookTimeNanos) throws IOException {
        // When it's used in the cache, the policy will receive BytesReferences which come from
        // serializing a CachedQueryResult.
        CachedQueryResult cachedQueryResult = new CachedQueryResult(getQSR(), tookTimeNanos);
        BytesStreamOutput out = new BytesStreamOutput();
        cachedQueryResult.writeToNoId(out);
        return out.bytes();
    }

    private QuerySearchResult getQSR() {
        // We can't mock the QSR with mockito because the class is final. Construct a real one
        QuerySearchResult mockQSR = new QuerySearchResult();

        // duplicated from DfsQueryPhaseTests.java
        mockQSR.topDocs(
            new TopDocsAndMaxScore(
                new TopDocs(new TotalHits(1, TotalHits.Relation.EQUAL_TO), new ScoreDoc[] { new ScoreDoc(42, 1.0F) }),
                2.0F
            ),
            new DocValueFormat[0]
        );
        return mockQSR;
    }

    private void writeRandomBytes(StreamOutput out, int numBytes) throws IOException {
        Random rand = Randomness.get();
        byte[] bytes = new byte[numBytes];
        rand.nextBytes(bytes);
        out.writeBytes(bytes);
    }
}
