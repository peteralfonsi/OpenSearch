/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache;

import org.opensearch.common.cache.stats.CacheStats;
import org.opensearch.common.cache.stats.CacheStatsDimension;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.core.xcontent.XContent;
import org.opensearch.indices.IndicesService;

import java.io.IOException;
import java.util.EnumMap;
import java.util.List;

/**
 * A service used for aggregating and returning stats for OpenSearch caches.
 */
public class CacheService extends AbstractLifecycleComponent {

    private EnumMap<CacheType, ICache> cacheMap;

    private IndicesService indicesService; // Used to map indices to shards for request cache

    public CacheService(IndicesService indicesService) {
        this.indicesService = indicesService;
    }
    @Override
    protected void doStart() {

    }

    @Override
    protected void doStop() {

    }

    @Override
    protected void doClose() throws IOException {

    }

    public void registerCache(CacheType cacheType, ICache cache) {
        assert (!cacheMap.containsKey(cacheType)) : "Cache type " + cacheType.toString() + " already has a registered ICache";
        assert (cache != null) : "Registered cache cannot be null";
        cacheMap.put(cacheType, cache);
    }

    public void deregisterCache(CacheType cacheType) {
        // Is this needed?
        cacheMap.remove(cacheType);
    }

    CacheStats getStats(CacheType cacheType, List<CacheStatsDimension> dimensions) {
        // NodeCacheStats calls this when it's running toXContent
        switch (cacheType) {
            case INDICES_REQUEST_CACHE:

                break;
            default:
                break;
        }
    }

    NodeCacheStats stats() {
        return new NodeCacheStats(this);
    }
}
