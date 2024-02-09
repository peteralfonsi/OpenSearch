/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.tier;

import org.opensearch.common.Randomness;
import org.opensearch.common.cache.stats.CacheStatsDimension;
import org.opensearch.common.cache.stats.ICacheKey;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Random;
import java.util.UUID;

public class ICacheKeySerializerTests extends OpenSearchTestCase {
    // For these tests, we use BytesReference as K, since we already have a Serializer<BytesReference, byte[]> implementation
    public void testEquality() throws Exception {
        BytesReferenceSerializer keySer = new BytesReferenceSerializer();
        ICacheKeySerializer<BytesReference> serializer = new ICacheKeySerializer<>(keySer);

        int numDimensionsTested = 10;
        for (int i = 0; i < numDimensionsTested; i++) {
            CacheStatsDimension dim = getRandomDim();
            ICacheKey<BytesReference> key = new ICacheKey<>(getRandomBytesReference(), List.of(dim));
            byte[] serialized = serializer.serialize(key);
            assertTrue(serializer.equals(key, serialized));
            ICacheKey<BytesReference> deserialized = serializer.deserialize(serialized);
            assertEquals(key, deserialized);
        }
    }

    private CacheStatsDimension getRandomDim() {
        return new CacheStatsDimension(UUID.randomUUID().toString(), UUID.randomUUID().toString());
    }

    private BytesReference getRandomBytesReference() {
        byte[] bytesValue = new byte[1000];
        Random rand = Randomness.get();
        rand.nextBytes(bytesValue);
        return new BytesArray(bytesValue);
    }

}
