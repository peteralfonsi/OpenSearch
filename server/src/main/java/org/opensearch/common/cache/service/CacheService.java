/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.service;

import org.opensearch.action.admin.indices.stats.CommonStatsFlags;
import org.opensearch.common.cache.CacheType;
import org.opensearch.common.cache.ICache;
import org.opensearch.common.cache.settings.CacheSettings;
import org.opensearch.common.cache.stats.CacheStats;
import org.opensearch.common.cache.stats.CacheStatsDimension;
import org.opensearch.common.cache.stats.CacheStatsResponse;
import org.opensearch.common.cache.store.config.CacheConfig;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Service responsible to create caches.
 */
public class CacheService {

    // Common values for dimension names and values below:
    public static final String TIER_DIMENSION_NAME = "tier";
    public static final String TIER_DIMENSION_VALUE_ON_HEAP = "on_heap";
    public static final String TIER_DIMENSION_VALUE_DISK = "disk";

    // TODO: This is a placeholder - probably this should be defined elsewhere
    public static final List<String> API_SUPPORTED_TIERS = List.of(TIER_DIMENSION_VALUE_ON_HEAP, TIER_DIMENSION_VALUE_DISK);

    public static final String INDICES_DIMENSION_NAME = "indices";
    public static final String SHARDS_DIMENSION_NAME = "shardId";

    public static final List<String> REQUEST_CACHE_DIMENSION_NAMES = List.of(INDICES_DIMENSION_NAME, SHARDS_DIMENSION_NAME, TIER_DIMENSION_NAME);

    private final Map<String, ICache.Factory> cacheStoreTypeFactories;
    private final Settings settings;
    private Map<CacheType, ICache<?, ?>> cacheTypeMap;
    private final IndicesService indicesService; // Used to map indices to shards for request cache


    public CacheService(Map<String, ICache.Factory> cacheStoreTypeFactories, Settings settings, IndicesService indicesService) {
        this.cacheStoreTypeFactories = cacheStoreTypeFactories;
        this.settings = settings;
        this.cacheTypeMap = new HashMap<>();
        this.indicesService = indicesService;
    }

    public Map<CacheType, ICache<?, ?>> getCacheTypeMap() {
        return this.cacheTypeMap;
    }

    public <K, V> ICache<K, V> createCache(CacheConfig<K, V> config, CacheType cacheType) {
        Setting<String> cacheSettingForCacheType = CacheSettings.CACHE_TYPE_STORE_NAME.getConcreteSettingForNamespace(
            cacheType.getSettingPrefix()
        );
        String storeName = cacheSettingForCacheType.get(settings);
        if (storeName == null || storeName.isBlank()) {
            throw new IllegalArgumentException("No configuration exists for cache type: " + cacheType);
        }
        if (!cacheStoreTypeFactories.containsKey(storeName)) {
            throw new IllegalArgumentException("No store name: [" + storeName + "] is registered for cache type: " + cacheType);
        }
        ICache.Factory factory = cacheStoreTypeFactories.get(storeName);
        ICache<K, V> iCache = factory.create(config, cacheType, cacheStoreTypeFactories);
        cacheTypeMap.put(cacheType, iCache);
        return iCache;
    }

    public void registerCache(CacheType cacheType, ICache cache) {
        assert (!cacheTypeMap.containsKey(cacheType)) : "Cache type " + cacheType.toString() + " already has a registered ICache";
        assert (cache != null) : "Registered cache cannot be null";
        cacheTypeMap.put(cacheType, cache);
    }

    public void deregisterCache(CacheType cacheType) {
        // Is this needed?
        cacheTypeMap.remove(cacheType);
    }

    // pkg private for testing
    ICache getCache(CacheType cacheType) {
        assert cacheTypeMap.containsKey(cacheType) : "No registered ICache for " + cacheType.toString();
        return cacheTypeMap.get(cacheType);
    }

    // for testing
    IndicesService getIndicesService() {
        return indicesService;
    }

    AggregatedStats getRequestCacheStats() {
        // Return an AggregatedStats object which is split out by all three level options.
        // Then the NodeCacheStats object can sum later according to whatever level it receives.
        // (We don't know which levels at NodeCacheStats creation time)
        CacheStats cacheStats = getCache(CacheType.INDICES_REQUEST_CACHE).stats();
        AggregatedStats stats = new AggregatedStats(REQUEST_CACHE_DIMENSION_NAMES);

        for (final IndexService indexService : indicesService) {
            String indexName = getIndexName(indexService);
            for (final IndexShard indexShard : indexService) {
                String indexShardName = getShardName(indexShard);
                CacheStatsDimension shardDimension = new CacheStatsDimension(SHARDS_DIMENSION_NAME, indexShardName);
                for (String tier : API_SUPPORTED_TIERS) {
                    CacheStatsDimension tierDimension = new CacheStatsDimension(TIER_DIMENSION_NAME, tier);
                    stats.put(List.of(indexName, indexShardName, tier), cacheStats.getStatsByDimensions(List.of(shardDimension, tierDimension)));
                }
            }
        }
        return stats;
    }

    // pkg-private for testing
    String getIndexName(IndexService indexService) {
        // TODO: Is this correct?
        return indexService.getIndexSettings().getIndex().getName();
    }

    // pkg-private for testing
    String getShardName(IndexShard indexShard) {
        // TODO: Is this correct?
        return indexShard.shardId().toString();
    }

    String getIndexNameFromShardName(String shardName) {
        String[] parts = shardName.split("\\[");
        return parts[1].split("]")[0];
    }

    CacheStatsResponse getTotalRequestStats() {
        // TODO: DEBUG ONLY _ REMOOVE
        try {
            ICache cache = getCache(CacheType.INDICES_REQUEST_CACHE);
            return cache.stats().getTotalStats();
        } catch (AssertionError e) {
            return new CacheStatsResponse();
        }
    }

    public NodeCacheStats stats(CommonStatsFlags flags) {
        // TODO: Filter what we have to pass in based on values in flags. Do this after we change to the enummap for NCS constructor.
        AggregatedStats requestStats = getRequestCacheStats();
        CacheStatsResponse totalRequestStats = getTotalRequestStats(); // We always return the total stats as well, to spare the NodeCacheStats from having to sum it up
        return new NodeCacheStats(flags, requestStats, totalRequestStats);
    }
}
