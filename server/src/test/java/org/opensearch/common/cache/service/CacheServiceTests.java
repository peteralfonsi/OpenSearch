/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.service;

import org.opensearch.action.admin.indices.create.CreateIndexAction;
import org.opensearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.cache.CacheType;
import org.opensearch.common.cache.ICache;
import org.opensearch.common.cache.ICacheKey;
import org.opensearch.common.cache.LoadAwareCacheLoader;
import org.opensearch.common.cache.module.CacheModule;
import org.opensearch.common.cache.settings.CacheSettings;
import org.opensearch.common.cache.stats.CacheStats;
import org.opensearch.common.cache.stats.CacheStatsDimension;
import org.opensearch.common.cache.stats.CacheStatsResponse;
import org.opensearch.common.cache.stats.MultiDimensionCacheStats;
import org.opensearch.common.cache.store.OpenSearchOnHeapCache;
import org.opensearch.common.cache.store.config.CacheConfig;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.ShardNotFoundException;
import org.opensearch.indices.IndicesService;
import org.opensearch.plugins.CachePlugin;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

import static junit.framework.TestCase.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CacheServiceTests extends OpenSearchSingleNodeTestCase {

    public void testWithCreateCacheForIndicesRequestCacheType() {
        CachePlugin mockPlugin1 = mock(CachePlugin.class);
        ICache.Factory factory1 = mock(ICache.Factory.class);
        Map<String, ICache.Factory> factoryMap = Map.of("cache1", factory1);
        when(mockPlugin1.getCacheFactoryMap()).thenReturn(factoryMap);

        Setting<String> indicesRequestCacheSetting = CacheSettings.getConcreteSettingForCacheType(CacheType.INDICES_REQUEST_CACHE);

        CacheModule cacheModule = new CacheModule(
            List.of(mockPlugin1),
            Settings.builder().put(indicesRequestCacheSetting.getKey(), "cache1").build(),
            getInstanceFromNode(IndicesService.class)
        );
        CacheConfig<String, String> config = mock(CacheConfig.class);
        ICache<String, String> onHeapCache = mock(OpenSearchOnHeapCache.class);
        when(factory1.create(eq(config), eq(CacheType.INDICES_REQUEST_CACHE), any(Map.class))).thenReturn(onHeapCache);

        CacheService cacheService = cacheModule.getCacheService();
        ICache<String, String> ircCache = cacheService.createCache(config, CacheType.INDICES_REQUEST_CACHE);
        assertEquals(onHeapCache, ircCache);
    }

    public void testWithCreateCacheWithNoStoreNamePresentForCacheType() {
        ICache.Factory factory1 = mock(ICache.Factory.class);
        Map<String, ICache.Factory> factoryMap = Map.of("cache1", factory1);
        CacheService cacheService = new CacheService(
            factoryMap,
            Settings.builder().build(),
            getInstanceFromNode(IndicesService.class));

        CacheConfig<String, String> config = mock(CacheConfig.class);
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> cacheService.createCache(config, CacheType.INDICES_REQUEST_CACHE)
        );
        assertEquals("No configuration exists for cache type: INDICES_REQUEST_CACHE", ex.getMessage());
    }

    public void testWithCreateCacheWithInvalidStoreNameAssociatedForCacheType() {
        ICache.Factory factory1 = mock(ICache.Factory.class);
        Setting<String> indicesRequestCacheSetting = CacheSettings.getConcreteSettingForCacheType(CacheType.INDICES_REQUEST_CACHE);
        Map<String, ICache.Factory> factoryMap = Map.of("cache1", factory1);
        CacheService cacheService = new CacheService(
            factoryMap,
            Settings.builder().put(indicesRequestCacheSetting.getKey(), "cache").build(),
            getInstanceFromNode(IndicesService.class)
        );

        CacheConfig<String, String> config = mock(CacheConfig.class);
        ICache<String, String> onHeapCache = mock(OpenSearchOnHeapCache.class);
        when(factory1.create(config, CacheType.INDICES_REQUEST_CACHE, factoryMap)).thenReturn(onHeapCache);

        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> cacheService.createCache(config, CacheType.INDICES_REQUEST_CACHE)
        );
        assertEquals("No store name: [cache] is registered for cache type: INDICES_REQUEST_CACHE", ex.getMessage());
    }

    // To test aggregation logic, create an IndicesService that has the appropriate indices and shards existing.
    // Then, match those with the shard id values fed into the MockCacheTiers. (There's no integration between IRC and ICache right now).
    // CacheService only relies on IndicesService for its iterator.

    public void testGetRequestStatsSingleTier() throws Exception {
        // TODO: I can't figure out how to create an index with 0 shards, so we can't test that case yet.
        //List<String> indexNames = List.of("index1", "index2", "index3");
        //Map<String, Integer> numIndexShards = Map.of("index1", 4, "index2", 10, "index3", 0);

        List<String> indexNames = List.of("index1", "index2");
        Map<String, Integer> numIndexShards = Map.of("index1", 4, "index2", 10);
        CacheService service = getCacheServiceWithIRC(indexNames, numIndexShards);
        for (String tierName : CacheService.API_SUPPORTED_TIERS) {
            for (CacheType cacheType : CacheType.values()) {
                service.deregisterCache(cacheType);
            }
            MockCacheTier tier = new MockCacheTier(new MultiDimensionCacheStats(List.of(CacheService.SHARDS_DIMENSION_NAME), tierName));
            service.registerCache(CacheType.INDICES_REQUEST_CACHE, tier);
            Map<String, Map<String, CacheStatsResponse>> expectedResults = populateStats(service, tier.stats(), numIndexShards);

            AggregatedStats requestStats = service.getRequestCacheStats();
            assertEquals(CacheService.REQUEST_CACHE_DIMENSION_NAMES, requestStats.getDimensionNames());
            CacheStatsResponse sumTotal = new CacheStatsResponse();
            for (String indexName : requestStats.getInnerMapKeySet(List.of())) {
                for (String shardName : requestStats.getInnerMapKeySet(List.of(indexName))) {
                    for (String tierNameKey : requestStats.getInnerMapKeySet(List.of(indexName, shardName))) {
                        CacheStatsResponse got = requestStats.getResponse(List.of(indexName, shardName, tierNameKey));
                        CacheStatsResponse expected;
                        sumTotal.add(got);
                        if (tierNameKey.equals(tierName)) {
                            expected = expectedResults.get(indexName).get(shardName);
                        } else {
                            expected = new CacheStatsResponse(0,0,0,0,0);
                        }
                        assertEquals(expected, got);
                    }
                }
            }
            assertEquals(sumTotal, service.getTotalRequestStats());
        }
    }

    public void testGetRequestStatsOnHeapOnlyTiered() throws Exception {
        List<String> indexNames = List.of("index1", "index2");
        Map<String, Integer> numIndexShards = Map.of("index1", 4, "index2", 10);
        CacheService service = getCacheServiceWithIRC(indexNames, numIndexShards);

        CacheServiceTests.MockCacheTier tieredSpilloverCache = new MockCacheTier(new MockTieredSpilloverCacheStats(
            new MultiDimensionCacheStats(List.of(CacheService.SHARDS_DIMENSION_NAME), CacheService.TIER_DIMENSION_VALUE_ON_HEAP),
            new MultiDimensionCacheStats(List.of(CacheService.SHARDS_DIMENSION_NAME), CacheService.TIER_DIMENSION_VALUE_DISK)
        ));
        service.registerCache(CacheType.INDICES_REQUEST_CACHE, tieredSpilloverCache);
        CacheStats heapStats = ((MockTieredSpilloverCacheStats) service.getCache(CacheType.INDICES_REQUEST_CACHE).stats()).heapStats;
        CacheStats diskStats = ((MockTieredSpilloverCacheStats) service.getCache(CacheType.INDICES_REQUEST_CACHE).stats()).diskStats;

        Map<String, Map<String, CacheStatsResponse>> expectedHeapResults = populateStats(service, heapStats, numIndexShards);
        Map<String, Map<String, CacheStatsResponse>> expectedDiskResults = populateStats(service, diskStats, numIndexShards);
        Map<String, Map<String, Map<String, CacheStatsResponse>>> expectedResultsByTier = Map.of(CacheService.TIER_DIMENSION_VALUE_ON_HEAP, expectedHeapResults, CacheService.TIER_DIMENSION_VALUE_DISK, expectedDiskResults);

        AggregatedStats requestStats = service.getRequestCacheStats();
        CacheStatsResponse sumTotal = new CacheStatsResponse();
        assertEquals(CacheService.REQUEST_CACHE_DIMENSION_NAMES, requestStats.getDimensionNames());
        for (String indexName : requestStats.getInnerMapKeySet(List.of())) {
            for (String shardName : requestStats.getInnerMapKeySet(List.of(indexName))) {
                for (String tierName : requestStats.getInnerMapKeySet(List.of(indexName, shardName))) {
                    CacheStatsResponse expected = expectedResultsByTier.get(tierName).get(indexName).get(shardName);
                    CacheStatsResponse got = requestStats.getResponse(List.of(indexName, shardName, tierName));
                    sumTotal.add(got);
                    assertEquals(expected, got);
                }
            }
        }
        assertEquals(sumTotal, service.getTotalRequestStats());
    }

    public void testRequestCacheAggregationWithNoIndices() throws Exception {
        CacheService service = getCacheServiceWithIRC(List.of(), Map.of());
        MockCacheTier onHeap = new MockCacheTier(new MultiDimensionCacheStats(List.of(CacheService.SHARDS_DIMENSION_NAME), CacheService.TIER_DIMENSION_VALUE_ON_HEAP));
        service.registerCache(CacheType.INDICES_REQUEST_CACHE, onHeap);
        Map<String, Map<String, CacheStatsResponse>> expectedResults = populateStats(service, onHeap.stats(), Map.of());

        AggregatedStats requestStats = service.getRequestCacheStats();
        assertEquals(CacheService.REQUEST_CACHE_DIMENSION_NAMES, requestStats.getDimensionNames());
        assertEquals(List.of(), requestStats.getInnerMapKeySet(List.of()));
    }

    static Map<String, Map<String, CacheStatsResponse>> populateStats(CacheService service, CacheStats stats, Map<String, Integer> numIndexShards) {
        // Populate the request cache stats for all indices and shards with random numbers of hits, misses, etc
        // Return a nested map, from (indexName, shardName) -> expected cacheStatsResponse for that shard
        // This allows us to reuse the same populateStats method across tests
        ICache requestCache = service.getCache(CacheType.INDICES_REQUEST_CACHE);
        Map<String, Map<String, CacheStatsResponse>> result = new HashMap<>();

        for (IndexService indexService : service.getIndicesService()) {
            String indexName = service.getIndexName(indexService);
            result.put(indexName, new HashMap<>());
            for (int shardId = 0; shardId < numIndexShards.get(indexName); shardId++) {
                IndexShard shard = indexService.getShard(shardId);
                String shardName = service.getShardName(shard);
                int numHits = between(0, 100);
                int numMisses = between(0, 100);
                int numEvictions = between(0, 100);
                int memorySize = between(0, 100);
                int numEntries = between(0, 100);
                List<CacheStatsDimension> shardDims = List.of(new CacheStatsDimension(CacheService.SHARDS_DIMENSION_NAME, service.getShardName(shard)));

                for (int i = 0; i < numHits; i++) {
                    stats.incrementHitsByDimensions(shardDims);
                }
                for (int i = 0; i < numMisses; i++) {
                    stats.incrementMissesByDimensions(shardDims);
                }
                for (int i = 0; i < numEvictions; i++) {
                    stats.incrementEvictionsByDimensions(shardDims);
                }
                stats.incrementMemorySizeByDimensions(shardDims, memorySize);
                for (int i = 0; i < numEntries; i++) {
                    stats.incrementEntriesByDimensions(shardDims);
                }
                CacheStatsResponse response = new CacheStatsResponse(numHits, numMisses, numEvictions, memorySize, numEntries);
                result.get(indexName).put(shardName, response);
            }
        }
        return result;
    }

    CacheService getCacheServiceWithIRC(List<String> indexNames, Map<String, Integer> numIndexShards) {
        ICache.Factory factory1 = mock(ICache.Factory.class);
        Setting<String> indicesRequestCacheSetting = CacheSettings.getConcreteSettingForCacheType(CacheType.INDICES_REQUEST_CACHE);
        Map<String, ICache.Factory> factoryMap = Map.of("cache1", factory1);

        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        for (String indexName : indexNames) {
            CreateIndexRequestBuilder indexBuilder = new CreateIndexRequestBuilder(client(), CreateIndexAction.INSTANCE).setIndex(indexName);
            Settings.Builder indexSettingsBuilder = Settings.builder();
            if (numIndexShards.get(indexName) > 0) {
                indexSettingsBuilder.put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numIndexShards.get(indexName));
                // TODO: How to create an index with 0 shards? By default there's 1 and this setting doesn't allow 0
            }
            indexBuilder.setSettings(indexSettingsBuilder);
            IndexService index = createIndex(indexName, indexBuilder);
            for (int i = 0; i < numIndexShards.get(indexName); i++) {
                // Check we can get all the shards we expect
                index.getShard(i);
            }
            if (numIndexShards.get(indexName) == 0) {
                assertThrows(ShardNotFoundException.class, () -> index.getShard(0));
            }
        }
        return new CacheService(
            factoryMap,
            Settings.builder().put(indicesRequestCacheSetting.getKey(), "cache").build(),
            indicesService
        );
    }

    // Nothing in this mock ICache needs to be functional except the stats
    static class MockCacheTier implements ICache<String, String> {
        private final CacheStats stats;
        MockCacheTier(CacheStats stats) {

            this.stats = stats;
        }
        @Override
        public String get(ICacheKey<String> key) {
            return null;
        }
        @Override
        public void put(ICacheKey<String> key, String value) {}
        @Override
        public String computeIfAbsent(ICacheKey<String> key, LoadAwareCacheLoader<ICacheKey<String>, String> loader) throws Exception {
            return null;
        }
        @Override
        public void invalidate(ICacheKey<String> key) {}
        @Override
        public void invalidateAll() {}
        @Override
        public Iterable<ICacheKey<String>> keys() {
            return null;
        }
        @Override
        public long count() {
            return stats.getTotalEntries();
        }
        @Override
        public void refresh() {}
        @Override
        public CacheStats stats() {
            return stats;
        }
        @Override
        public void close() throws IOException {}
    }

    // TODO: This is a "mock class", but it's the same as a partially-integrated version of a TSC stats implementation that's currently blocked.
    //  It has been tested in a separate branch. We need this to test tier aggregation logic.
    //  When this gets added, change the tests to use the actual class.
    static class MockTieredSpilloverCacheStats implements CacheStats {
        final MultiDimensionCacheStats heapStats;
        final MultiDimensionCacheStats diskStats;
        public static final String TIER_DIMENSION_NAME = "tier";
        public static final String TIER_DIMENSION_VALUE_ON_HEAP = "on_heap";
        public static final String TIER_DIMENSION_VALUE_DISK = "disk";

        public MockTieredSpilloverCacheStats(MultiDimensionCacheStats heapStats, MultiDimensionCacheStats diskStats) {
            this.heapStats = heapStats;
            this.diskStats = diskStats;
        }

        public MockTieredSpilloverCacheStats(StreamInput in) throws IOException {
            heapStats = new MultiDimensionCacheStats(in);
            diskStats = new MultiDimensionCacheStats(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            heapStats.writeTo(out);
            diskStats.writeTo(out);
        }

        @Override
        public CacheStatsResponse getTotalStats() {
            return new CacheStatsResponse(
                getTotalHits(),
                getTotalMisses(),
                getTotalEvictions(),
                getTotalMemorySize(),
                getTotalEntries()
            );
        }

        @Override
        public CacheStatsResponse getStatsByDimensions(List<CacheStatsDimension> dimensions) {
            return new CacheStatsResponse(
                getHitsByDimensions(dimensions),
                getMissesByDimensions(dimensions),
                getEvictionsByDimensions(dimensions),
                getMemorySizeByDimensions(dimensions),
                getEntriesByDimensions(dimensions)
            );
        }

        @Override
        public long getTotalHits() {
            return heapStats.getTotalHits() + diskStats.getTotalHits();
        }

        @Override
        public long getTotalMisses() {
            return heapStats.getTotalMisses() + diskStats.getTotalMisses();
        }

        @Override
        public long getTotalEvictions() {
            return heapStats.getTotalEvictions() + diskStats.getTotalEvictions();
        }

        @Override
        public long getTotalMemorySize() {
            return heapStats.getTotalMemorySize() + diskStats.getTotalMemorySize();
        }

        @Override
        public long getTotalEntries() {
            return heapStats.getTotalEntries() + diskStats.getTotalEntries();
        }

        private CacheStatsDimension getTierDimension(List<CacheStatsDimension> dimensions) {
            for (CacheStatsDimension dim : dimensions) {
                if (dim.dimensionName.equals(TIER_DIMENSION_NAME)) {
                    return dim;
                }
            }
            return null;
        }

        private long getValueByDimensions(
            List<CacheStatsDimension> dimensions,
            Function<List<CacheStatsDimension>, Long> heapStatsGetterByDimensions,
            Supplier<Long> heapStatsGetterTotal,
            Function<List<CacheStatsDimension>, Long> diskStatsGetterByDimensions,
            Supplier<Long> diskStatsGetterTotal) {

            CacheStatsDimension tierDimension = getTierDimension(dimensions);
            if (tierDimension == null) {
                // We aren't slicing by tier; add results from both tiers
                return heapStatsGetterByDimensions.apply(dimensions) + diskStatsGetterByDimensions.apply(dimensions);
            } else {
                // We are slicing by tier. Pass the dimensions list (without the tier dimension) to the relevant tier's CacheStats.
                assert tierDimension.dimensionValue.equals(TIER_DIMENSION_VALUE_ON_HEAP)
                    || tierDimension.dimensionValue.equals(TIER_DIMENSION_VALUE_DISK)
                    : "Tier dimension had unrecognized value " + tierDimension.dimensionValue;
                List<CacheStatsDimension> tierDims = new ArrayList<>(dimensions); // The list passed in can be immutable; make a mutable copy
                tierDims.remove(tierDimension);
                if (tierDims.isEmpty()) {
                    // If there are no other dimensions, use the Supplier (which gets the total value from the tier)
                    if (tierDimension.dimensionValue.equals(TIER_DIMENSION_VALUE_ON_HEAP)) {
                        return heapStatsGetterTotal.get();
                    } else {
                        return diskStatsGetterTotal.get();
                    }
                } else {
                    // If there are other dimensions, use the Function (which gets the value by dimensions from the tier)
                    if (tierDimension.dimensionValue.equals(TIER_DIMENSION_VALUE_ON_HEAP)) {
                        return heapStatsGetterByDimensions.apply(tierDims);
                    } else {
                        return diskStatsGetterByDimensions.apply(tierDims);
                    }
                }
            }
        }

        @Override
        public long getHitsByDimensions(List<CacheStatsDimension> dimensions) {
            return getValueByDimensions(dimensions, heapStats::getHitsByDimensions, heapStats::getTotalHits, diskStats::getHitsByDimensions, diskStats::getTotalHits);
        }

        @Override
        public long getMissesByDimensions(List<CacheStatsDimension> dimensions) {
            return getValueByDimensions(dimensions, heapStats::getMissesByDimensions, heapStats::getTotalMisses, diskStats::getMissesByDimensions, diskStats::getTotalMisses);
        }

        @Override
        public long getEvictionsByDimensions(List<CacheStatsDimension> dimensions) {
            return getValueByDimensions(dimensions, heapStats::getEvictionsByDimensions, heapStats::getTotalEvictions, diskStats::getEvictionsByDimensions, diskStats::getTotalEvictions);
        }

        @Override
        public long getMemorySizeByDimensions(List<CacheStatsDimension> dimensions) {
            return getValueByDimensions(dimensions, heapStats::getMemorySizeByDimensions, heapStats::getTotalMemorySize, diskStats::getMemorySizeByDimensions, diskStats::getTotalMemorySize);
        }

        @Override
        public long getEntriesByDimensions(List<CacheStatsDimension> dimensions) {
            return getValueByDimensions(dimensions, heapStats::getEntriesByDimensions, heapStats::getTotalEntries, diskStats::getEntriesByDimensions, diskStats::getTotalEntries);
        }

        // The below functions shouldn't be used, so they do nothing. Values are incremented by the tiers themselves,
        // and they increment the heapStats or diskStats objects directly.

        @Override
        public void incrementHitsByDimensions(List<CacheStatsDimension> dimensions) {}
        @Override
        public void incrementMissesByDimensions(List<CacheStatsDimension> dimensions) {}
        @Override
        public void incrementEvictionsByDimensions(List<CacheStatsDimension> dimensions) {}
        @Override
        public void incrementMemorySizeByDimensions(List<CacheStatsDimension> dimensions, long amountBytes) {}
        @Override
        public void incrementEntriesByDimensions(List<CacheStatsDimension> dimensions) {}
        @Override
        public void decrementEntriesByDimensions(List<CacheStatsDimension> dimensions) {}
    }
}
