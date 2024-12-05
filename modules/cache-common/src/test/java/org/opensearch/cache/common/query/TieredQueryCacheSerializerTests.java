/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cache.common.query;

import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.util.BitDocIdSet;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.RoaringDocIdSet;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.List;

// Tests for the serializers in TieredQueryCache.
public class TieredQueryCacheSerializerTests extends OpenSearchTestCase {

    public void testCacheAndCountSerializer() throws Exception {
        List<Integer> docs = List.of(1, 4, 5, 16, 288, 5838);
        int maxDoc = Collections.max(docs) + 250;
        int count = 17;
        // Check for both BigDocIdSet and RoaringDocIdSet
        for (DocIdSet set : new DocIdSet[] { getBitDocIdSet(docs), getRoaringDocIdSet(docs, maxDoc) }) {
            TieredQueryCache.CacheAndCount original = new TieredQueryCache.CacheAndCount(set, count, maxDoc);
            TieredQueryCache.CacheAndCountSerializer ser = new TieredQueryCache.CacheAndCountSerializer();
            byte[] serialized = ser.serialize(original);
            TieredQueryCache.CacheAndCount deserialized = ser.deserialize(serialized);
            assertTrue(ser.equals(original, serialized));
            assertEquals(original, deserialized);
        }
    }

    public void testCompositeKeySerializer() throws Exception {
        // TODO
    }

    private BitDocIdSet getBitDocIdSet(List<Integer> docs) {
        BitSet bitset = new FixedBitSet(Collections.max(docs) + 1);
        for (int doc : docs) {
            bitset.set(doc);
        }
        return new BitDocIdSet(bitset);
    }

    private RoaringDocIdSet getRoaringDocIdSet(List<Integer> docs, int maxDoc) {
        RoaringDocIdSet.Builder builder = new RoaringDocIdSet.Builder(maxDoc);
        for (int doc : docs) {
            builder.add(doc);
        }
        return builder.build();
    }
}
