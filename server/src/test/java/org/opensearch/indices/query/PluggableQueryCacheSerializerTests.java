/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.query;

import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.util.BitDocIdSet;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.RoaringDocIdSet;
import org.opensearch.common.Randomness;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

// Tests for the serializers in TieredQueryCache.
public class PluggableQueryCacheSerializerTests extends OpenSearchTestCase {

    public void testCacheAndCountSerializer() throws Exception {
        List<Integer> docs = List.of(1, 4, 5, 16, 288, 5838);
        int maxDoc = Collections.max(docs) + 250;
        int count = 17;
        // Check for both BigDocIdSet and RoaringDocIdSet
        for (DocIdSet set : new DocIdSet[] { getBitDocIdSet(docs), getRoaringDocIdSet(docs, maxDoc) }) {
            PluggableQueryCache.CacheAndCount original = new PluggableQueryCache.CacheAndCount(set, count, maxDoc);
            PluggableQueryCache.CacheAndCountSerializer ser = new PluggableQueryCache.CacheAndCountSerializer();
            byte[] serialized = ser.serialize(original);
            PluggableQueryCache.CacheAndCount deserialized = ser.deserialize(serialized);
            assertTrue(ser.equals(original, serialized));
            assertEquals(original, deserialized);
            assertTrue(serialized.length > PluggableQueryCache.CacheAndCountSerializer.BLOCK_SIZE * Integer.BYTES);
        }
    }

    public void testCacheAndCountSerializerLongDocIdSet() throws Exception {
        List<Integer> docs = new ArrayList<>();

        // populate docs with > block size...
        int lastDoc = 1;
        docs.add(lastDoc);
        Random rand = Randomness.get();
        for (int i = 0; i < PluggableQueryCache.CacheAndCountSerializer.BLOCK_SIZE + 252; i++) {
            lastDoc += rand.nextInt(5) + 1;
            docs.add(lastDoc);
        }
        int maxDoc = Collections.max(docs) + 250;
        int count = 17;

        for (DocIdSet set : new DocIdSet[] { getBitDocIdSet(docs), getRoaringDocIdSet(docs, maxDoc) }) {
            PluggableQueryCache.CacheAndCount original = new PluggableQueryCache.CacheAndCount(set, count, maxDoc);
            PluggableQueryCache.CacheAndCountSerializer ser = new PluggableQueryCache.CacheAndCountSerializer();
            byte[] serialized = ser.serialize(original);
            PluggableQueryCache.CacheAndCount deserialized = ser.deserialize(serialized);
            assertTrue(ser.equals(original, serialized));
            assertEquals(original, deserialized);
            assertTrue(serialized.length > 2 * PluggableQueryCache.CacheAndCountSerializer.BLOCK_SIZE * Integer.BYTES);
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
