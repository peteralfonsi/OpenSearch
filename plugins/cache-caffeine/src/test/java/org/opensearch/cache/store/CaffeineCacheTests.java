/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cache.store;

import org.opensearch.common.cache.ICacheKey;
import org.opensearch.common.cache.stats.CacheStatsDimension;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class CaffeineCacheTests extends OpenSearchTestCase {
    public void testAddAndGet() throws Exception {
        CaffeineCache<String, String> cache = new CaffeineCache<>();

        Map<ICacheKey<String>, String> keyValueMap = new HashMap<>();
        int numKeys = 100;
        for (int i = 0; i < numKeys; i++) {
            ICacheKey<String> key = getICacheKey(UUID.randomUUID().toString());
            String value = UUID.randomUUID().toString();
            cache.put(key, value);
            keyValueMap.put(key, value);
        }
        for (ICacheKey<String> key : keyValueMap.keySet()) {
            assertEquals(keyValueMap.get(key), cache.get(key));
        }
    }

    private ICacheKey<String> getICacheKey(String key) {
        return new ICacheKey<>(key, List.of(new CacheStatsDimension("dimName", "dimValue")));
    }
}
