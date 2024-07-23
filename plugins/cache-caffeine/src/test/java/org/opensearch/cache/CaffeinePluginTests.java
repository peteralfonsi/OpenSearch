/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cache;

import org.opensearch.cache.store.CaffeineHeapCache;
import org.opensearch.common.cache.ICache;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;

public class CaffeinePluginTests extends OpenSearchTestCase {

    private CaffeineCachePlugin caffeineCachePlugin = new CaffeineCachePlugin();

    public void testGetCacheStoreTypeMap() {
        Map<String, ICache.Factory> factoryMap = caffeineCachePlugin.getCacheFactoryMap();
        assertNotNull(factoryMap);
        assertNotNull(factoryMap.get(CaffeineHeapCache.CaffeineHeapCacheFactory.NAME));
    }
}
