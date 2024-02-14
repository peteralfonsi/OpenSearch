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
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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

    private ICache getCache(CacheType cacheType) {
        assert cacheMap.containsKey(cacheType) : "No registered ICache for " + cacheType.toString();
        return cacheMap.get(cacheType);
    }

    List<CacheStats> getStats(CacheType cacheType, List<CacheStatsDimension> dimensions) {
        // NodeCacheStats calls this when it's running toXContent
        switch (cacheType) {
            case INDICES_REQUEST_CACHE:
                return getRequestCacheStats(dimensions);
            default:
                return null;
        }
    }

    List<CacheStats> getRequestCacheStats(List<CacheStatsDimension> dimensions) {
        // Parse the dimensions list to see what we need to aggregate by.
        // Allowed combinations are: empty list, "shardId", "tier", "indices", "tier" + "indices", "tier + shard"
        ICache cache = getCache(CacheType.INDICES_REQUEST_CACHE);

        HashSet<String> dimensionValues = new HashSet<>();
        List<String> allowedValues = List.of(CacheStatsDimension.INDICES_DIMENSION_NAME, CacheStatsDimension.SHARDS_DIMENSION_NAME, CacheStatsDimension.TIER_DIMENSION_NAME);
        for (CacheStatsDimension dim : dimensions) {
            if (allowedValues.contains(dim.dimensionValue)) {
                dimensionValues.add(dim.dimensionValue);
                // TODO: What to do with unrecognized values? For now, ignoring them.
            }
        }
        if (dimensionValues.isEmpty()) {
            // Don't aggregate by anything; return totals.
            return List.of(cache.stats());
        } else if (dimensionValues.size() == 1) {
            if (dimensionValues.contains(CacheStatsDimension.TIER_DIMENSION_NAME)) {
                return List.of(
                    cache.stats
                )
            }

        }



    }

    NodeCacheStats stats() {
        return new NodeCacheStats(this);
    }
}
