/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.cache.stats.CacheStats;
import org.opensearch.common.cache.stats.CacheStatsDimension;
import org.opensearch.common.cache.stats.CacheStatsResponse;
import org.opensearch.common.cache.stats.SingleDimensionCacheStats;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

public class CacheServiceTests extends OpenSearchSingleNodeTestCase {

    public void testInit() throws Exception {
        List<String> indexNames = List.of("index1", "index2");
        Map<String, Integer> numIndexShards = Map.of("index1", 4, "index2", 10);
        CacheService service = getCacheService(indexNames, numIndexShards);
    }

    // To test aggregation logic, create an IndicesService that has the appropriate indices and shards existent.
    // Then, match those with the shard id values fed into the MockCacheTiers. (There's no integration between IRC and ICache right now).
    // CacheService only relies on IndicesService for its iterator.

    public void testRequestCacheShardsAggregation() throws Exception {
        List<String> indexNames = List.of("index1", "index2");
        Map<String, Integer> numIndexShards = Map.of("index1", 4, "index2", 10);
        CacheService service = getCacheService(indexNames, numIndexShards);

        MockCacheTier onHeap = new MockCacheTier(new SingleDimensionCacheStats(CacheService.SHARDS_DIMENSION_NAME, CacheService.TIER_DIMENSION_VALUE_ON_HEAP));
        service.registerCache(CacheType.INDICES_REQUEST_CACHE, onHeap);
        CacheStats stats = service.getCache(CacheType.INDICES_REQUEST_CACHE).stats();
        Map<String, Map<String, CacheStatsResponse>> expectedResults = populateStats(service, stats, numIndexShards);
        List<CacheService.ResponseAndDimensions> aggregated = service.getStats(
            CacheType.INDICES_REQUEST_CACHE,
            List.of(new CacheStatsDimension(CacheService.SHARDS_DIMENSION_NAME, "")));

        for (CacheService.ResponseAndDimensions responseAndDimensions : aggregated) {
            String shardName = responseAndDimensions.getDimensionWithName(CacheService.SHARDS_DIMENSION_NAME).dimensionValue;
            String indexName = service.getIndexNameFromShardName(shardName);
            assertEquals(expectedResults.get(indexName).get(shardName), responseAndDimensions.response);
        }
        int shardsInExpectedResults = 0;
        for (String indexName : expectedResults.keySet()) {
            shardsInExpectedResults += expectedResults.get(indexName).keySet().size();
        }
        assertEquals(shardsInExpectedResults, aggregated.size());
    }

    public void testRequestCacheIndicesAggregation() throws Exception {
        List<String> indexNames = List.of("index1", "index2");
        Map<String, Integer> numIndexShards = Map.of("index1", 4, "index2", 10);
        CacheService service = getCacheService(indexNames, numIndexShards);

        MockCacheTier onHeap = new MockCacheTier(new SingleDimensionCacheStats(CacheService.SHARDS_DIMENSION_NAME, CacheService.TIER_DIMENSION_VALUE_ON_HEAP));
        service.registerCache(CacheType.INDICES_REQUEST_CACHE, onHeap);
        CacheStats stats = service.getCache(CacheType.INDICES_REQUEST_CACHE).stats();
        Map<String, Map<String, CacheStatsResponse>> expectedResults = populateStats(service, stats, numIndexShards);
        List<CacheService.ResponseAndDimensions> aggregated = service.getStats(
            CacheType.INDICES_REQUEST_CACHE,
            List.of(new CacheStatsDimension(CacheService.INDICES_DIMENSION_NAME, "")));

        for (CacheService.ResponseAndDimensions responseAndDimensions : aggregated) {
            String indexName = responseAndDimensions.getDimensionWithName(CacheService.INDICES_DIMENSION_NAME).dimensionValue;
            CacheStatsResponse expectedIndexResult = sumByIndexName(expectedResults, indexName);
            assertEquals(expectedIndexResult, responseAndDimensions.response);
            assertEquals((long) numIndexShards.get(indexName), (long) expectedResults.get(indexName).keySet().size());
        }
        assertEquals(indexNames.size(), aggregated.size());
    }

    public void testRequestCacheTiersAggregation() throws Exception {
        List<String> indexNames = List.of("index1", "index2");
        Map<String, Integer> numIndexShards = Map.of("index1", 4, "index2", 10);
        CacheService service = getCacheService(indexNames, numIndexShards);
        MockCacheTier tieredSpilloverCache = new MockCacheTier(new MockTieredSpilloverCacheStats(
            new SingleDimensionCacheStats(CacheService.SHARDS_DIMENSION_NAME, CacheService.TIER_DIMENSION_VALUE_ON_HEAP),
            new SingleDimensionCacheStats(CacheService.SHARDS_DIMENSION_NAME, CacheService.TIER_DIMENSION_VALUE_DISK)
        ));
        service.registerCache(CacheType.INDICES_REQUEST_CACHE, tieredSpilloverCache);

        CacheStats heapStats = ((MockTieredSpilloverCacheStats) service.getCache(CacheType.INDICES_REQUEST_CACHE).stats()).heapStats;
        CacheStats diskStats = ((MockTieredSpilloverCacheStats) service.getCache(CacheType.INDICES_REQUEST_CACHE).stats()).diskStats;
        Map<String, Map<String, CacheStatsResponse>> expectedHeapResults = populateStats(service, heapStats, numIndexShards);
        Map<String, Map<String, CacheStatsResponse>> expectedDiskResults = populateStats(service, diskStats, numIndexShards);
        Map<String, Map<String, Map<String, CacheStatsResponse>>> expectedResultsByTier = Map.of(CacheService.TIER_DIMENSION_VALUE_ON_HEAP, expectedHeapResults, CacheService.TIER_DIMENSION_VALUE_DISK, expectedDiskResults);

        List<CacheService.ResponseAndDimensions> aggregated = service.getStats(
            CacheType.INDICES_REQUEST_CACHE,
            List.of(new CacheStatsDimension(CacheService.TIER_DIMENSION_NAME, "")));

        for (CacheService.ResponseAndDimensions responseAndDimensions : aggregated) {
            assertEquals(1, responseAndDimensions.dimensions.size());
            String tierName = responseAndDimensions.getDimensionWithName(CacheService.TIER_DIMENSION_NAME).dimensionValue;

            CacheStatsResponse tierTotal = null;
            tierTotal = sumTotal(expectedResultsByTier.get(tierName));
            assertEquals(tierTotal, responseAndDimensions.response);
        }

        assertEquals(CacheService.API_SUPPORTED_TIERS.size(), aggregated.size());
    }

    public void testRequestCacheShardsAndTiersAggregation() throws Exception {
        List<String> indexNames = List.of("index1", "index2");
        Map<String, Integer> numIndexShards = Map.of("index1", 4, "index2", 10);
        CacheService service = getCacheService(indexNames, numIndexShards);
        MockCacheTier tieredSpilloverCache = new MockCacheTier(new MockTieredSpilloverCacheStats(
            new SingleDimensionCacheStats(CacheService.SHARDS_DIMENSION_NAME, CacheService.TIER_DIMENSION_VALUE_ON_HEAP),
            new SingleDimensionCacheStats(CacheService.SHARDS_DIMENSION_NAME, CacheService.TIER_DIMENSION_VALUE_DISK)
        ));
        service.registerCache(CacheType.INDICES_REQUEST_CACHE, tieredSpilloverCache);

        CacheStats heapStats = ((MockTieredSpilloverCacheStats) service.getCache(CacheType.INDICES_REQUEST_CACHE).stats()).heapStats;
        CacheStats diskStats = ((MockTieredSpilloverCacheStats) service.getCache(CacheType.INDICES_REQUEST_CACHE).stats()).diskStats;
        Map<String, Map<String, CacheStatsResponse>> expectedHeapResults = populateStats(service, heapStats, numIndexShards);
        Map<String, Map<String, CacheStatsResponse>> expectedDiskResults = populateStats(service, diskStats, numIndexShards);
        Map<String, Map<String, Map<String, CacheStatsResponse>>> expectedResultsByTier = Map.of(CacheService.TIER_DIMENSION_VALUE_ON_HEAP, expectedHeapResults, CacheService.TIER_DIMENSION_VALUE_DISK, expectedDiskResults);

        List<CacheService.ResponseAndDimensions> aggregated = service.getStats(
            CacheType.INDICES_REQUEST_CACHE,
            List.of(
                new CacheStatsDimension(CacheService.TIER_DIMENSION_NAME, ""),
                new CacheStatsDimension(CacheService.SHARDS_DIMENSION_NAME, ""))
        );

        for (CacheService.ResponseAndDimensions responseAndDimensions : aggregated) {
            String tierName = responseAndDimensions.getDimensionWithName(CacheService.TIER_DIMENSION_NAME).dimensionValue;
            String shardName = responseAndDimensions.getDimensionWithName(CacheService.SHARDS_DIMENSION_NAME).dimensionValue;
            String indexName = service.getIndexNameFromShardName(shardName);

            assertEquals(expectedResultsByTier.get(tierName).get(indexName).get(shardName), responseAndDimensions.response);
        }

        int shardsInExpectedResults = 0;
        for (String indexName : expectedHeapResults.keySet()) {
            shardsInExpectedResults += expectedHeapResults.get(indexName).keySet().size();
        }
        for (String indexName : expectedDiskResults.keySet()) {
            shardsInExpectedResults += expectedDiskResults.get(indexName).keySet().size();
        }
        assertEquals(shardsInExpectedResults, aggregated.size());
    }

    public void testRequestCacheIndicesAndTiersAggregation() throws Exception {
        List<String> indexNames = List.of("index1", "index2");
        Map<String, Integer> numIndexShards = Map.of("index1", 4, "index2", 10);
        CacheService service = getCacheService(indexNames, numIndexShards);
        MockCacheTier tieredSpilloverCache = new MockCacheTier(new MockTieredSpilloverCacheStats(
            new SingleDimensionCacheStats(CacheService.SHARDS_DIMENSION_NAME, CacheService.TIER_DIMENSION_VALUE_ON_HEAP),
            new SingleDimensionCacheStats(CacheService.SHARDS_DIMENSION_NAME, CacheService.TIER_DIMENSION_VALUE_DISK)
        ));
        service.registerCache(CacheType.INDICES_REQUEST_CACHE, tieredSpilloverCache);

        CacheStats heapStats = ((MockTieredSpilloverCacheStats) service.getCache(CacheType.INDICES_REQUEST_CACHE).stats()).heapStats;
        CacheStats diskStats = ((MockTieredSpilloverCacheStats) service.getCache(CacheType.INDICES_REQUEST_CACHE).stats()).diskStats;
        Map<String, Map<String, CacheStatsResponse>> expectedHeapResults = populateStats(service, heapStats, numIndexShards);
        Map<String, Map<String, CacheStatsResponse>> expectedDiskResults = populateStats(service, diskStats, numIndexShards);
        Map<String, Map<String, Map<String, CacheStatsResponse>>> expectedResultsByTier = Map.of(CacheService.TIER_DIMENSION_VALUE_ON_HEAP, expectedHeapResults, CacheService.TIER_DIMENSION_VALUE_DISK, expectedDiskResults);

        List<CacheService.ResponseAndDimensions> aggregated = service.getStats(
            CacheType.INDICES_REQUEST_CACHE,
            List.of(
                new CacheStatsDimension(CacheService.TIER_DIMENSION_NAME, ""),
                new CacheStatsDimension(CacheService.INDICES_DIMENSION_NAME, ""))
        );

        for (CacheService.ResponseAndDimensions responseAndDimensions : aggregated) {
            String indexName = responseAndDimensions.getDimensionWithName(CacheService.INDICES_DIMENSION_NAME).dimensionValue;
            String tierName = responseAndDimensions.getDimensionWithName(CacheService.TIER_DIMENSION_NAME).dimensionValue;
            CacheStatsResponse expectedIndexResult = sumByIndexName(expectedResultsByTier.get(tierName), indexName);
            assertEquals(expectedIndexResult, responseAndDimensions.response);
            assertEquals((long) numIndexShards.get(indexName), (long) expectedResultsByTier.get(tierName).get(indexName).keySet().size());
        }
        assertEquals(indexNames.size() * CacheService.API_SUPPORTED_TIERS.size(), aggregated.size());
    }

    public void testInvalidDimensions() throws Exception {
        // TODO: Fill in once we decide what to do with incorrect dimension/level inputs
    }

    public void testRequestCacheAggregationWithNoIndices() throws Exception {
        // TODO
    }

    public void testRequestCacheAggregationWithNoShards() throws Exception {
        // TODO
    }

    // TODO: Add various checks for invalid inputs etc

    private CacheService getCacheService(List<String> indexNames, Map<String, Integer> numIndexShards) {
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        for (String indexName : indexNames) {
            Settings indexSettings = Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numIndexShards.get(indexName)).build();
            IndexService index = createIndex(indexName, indexSettings);
            for (int i = 0; i < numIndexShards.get(indexName); i++) {
                // Check we can get all the shards we expect
                index.getShard(i);
            }
        }
        return new CacheService(indicesService);
    }

    private Map<String, Map<String, CacheStatsResponse>> populateStats(CacheService service, CacheStats stats, Map<String, Integer> numIndexShards) {
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

    private CacheStatsResponse sumByIndexName(Map<String, Map<String, CacheStatsResponse>> expectedResults, String indexName) {
        CacheStatsResponse expectedIndexResult = null;
        for (String shardName : expectedResults.get(indexName).keySet()) {
            expectedIndexResult = expectedResults.get(indexName).get(shardName).add(expectedIndexResult);
        }
        return expectedIndexResult;
    }

    private CacheStatsResponse sumTotal(Map<String, Map<String, CacheStatsResponse>> expectedResults) {
        CacheStatsResponse expectedIndexResult = null;
        for (String indexName : expectedResults.keySet()) {
            expectedIndexResult = sumByIndexName(expectedResults, indexName).add(expectedIndexResult);
        }
        return expectedIndexResult;
    }

    private String printResponse(CacheStatsResponse response ) {
        return "" + response.hits + ", " + response.misses + ", " + response.evictions + ", " + response.memorySize + ", " + response.entries;
    }

    // Nothing in this mock ICache needs to be functional except the stats
    private class MockCacheTier implements ICache<String, String> {
        private final CacheStats stats;
        private MockCacheTier(CacheStats stats) {

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
    private class MockTieredSpilloverCacheStats implements CacheStats {
        final SingleDimensionCacheStats heapStats;
        final SingleDimensionCacheStats diskStats;
        public static final String TIER_DIMENSION_NAME = "tier";
        public static final String TIER_DIMENSION_VALUE_ON_HEAP = "on_heap";
        public static final String TIER_DIMENSION_VALUE_DISK = "disk";

        public MockTieredSpilloverCacheStats(SingleDimensionCacheStats heapStats, SingleDimensionCacheStats diskStats) {
            this.heapStats = heapStats;
            this.diskStats = diskStats;
        }

        public MockTieredSpilloverCacheStats(StreamInput in) throws IOException {
            heapStats = new SingleDimensionCacheStats(in);
            diskStats = new SingleDimensionCacheStats(in);
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
