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
import org.opensearch.action.admin.indices.stats.CommonStatsFlags;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.cache.CacheType;
import org.opensearch.common.cache.ICache;
import org.opensearch.common.cache.settings.CacheSettings;
import org.opensearch.common.cache.stats.CacheStats;
import org.opensearch.common.cache.stats.CacheStatsResponse;
import org.opensearch.common.cache.stats.MultiDimensionCacheStats;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.BytesStreamInput;
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.ShardNotFoundException;
import org.opensearch.indices.IndicesService;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;

public class NodeCacheStatsTests extends OpenSearchSingleNodeTestCase {
    public void testRequestCacheShardsAggregation() throws Exception {
        List<String> indexNames = List.of("index1", "index2");
        Map<String, Integer> numIndexShards = Map.of("index1", 4, "index2", 10);
        String tierName = CacheService.TIER_DIMENSION_VALUE_ON_HEAP;
        CacheService service = getCacheService(indexNames, numIndexShards);
        Tuple<NodeCacheStats, Map<String, Map<String, CacheStatsResponse>>> setup = getNodeCacheStatsAndExpectedResultsForSingleTier(service, numIndexShards, tierName);
        NodeCacheStats stats = setup.v1();
        Map<String, Map<String, CacheStatsResponse>> expectedResults = setup.v2();

        AggregatedStats aggregated = stats.aggregateRequestStatsByLevel(new String[]{"shards"});

        assertEquals(List.of(CacheService.SHARDS_DIMENSION_NAME), aggregated.getDimensionNames());

        for (String shardName : aggregated.getInnerMapKeySet(List.of())) {
            String indexName = service.getIndexNameFromShardName(shardName);
            CacheStatsResponse shardResponse = aggregated.getResponse(List.of(shardName));
            assertEquals(expectedResults.get(indexName).get(shardName), shardResponse);
        }
        int shardsInExpectedResults = 0;
        for (String indexName : expectedResults.keySet()) {
            shardsInExpectedResults += expectedResults.get(indexName).keySet().size();
        }
        assertEquals(shardsInExpectedResults, aggregated.getSize());
    }

    public void testRequestCacheIndicesAggregation() throws Exception {
        List<String> indexNames = List.of("index1", "index2");
        Map<String, Integer> numIndexShards = Map.of("index1", 4, "index2", 10);
        String tierName = CacheService.TIER_DIMENSION_VALUE_ON_HEAP;
        CacheService service = getCacheService(indexNames, numIndexShards);
        Tuple<NodeCacheStats, Map<String, Map<String, CacheStatsResponse>>> setup = getNodeCacheStatsAndExpectedResultsForSingleTier(service, numIndexShards, tierName);
        NodeCacheStats stats = setup.v1();
        Map<String, Map<String, CacheStatsResponse>> expectedResults = setup.v2();

        AggregatedStats aggregated = stats.aggregateRequestStatsByLevel(new String[]{"indices"});

        assertEquals(List.of(CacheService.INDICES_DIMENSION_NAME), aggregated.getDimensionNames());
        for (String indexName : aggregated.getInnerMapKeySet(List.of())) {
            CacheStatsResponse expectedIndexResult = sumByIndexName(expectedResults, indexName);
            assertEquals(expectedIndexResult, aggregated.getResponse(List.of(indexName)));
        }
        assertEquals(indexNames.size(), aggregated.getSize());
    }

    public void testRequestCacheTiersAggregation() throws Exception {
        List<String> indexNames = List.of("index1", "index2");
        Map<String, Integer> numIndexShards = Map.of("index1", 4, "index2", 10);
        CacheService service = getCacheService(indexNames, numIndexShards);
        Tuple<NodeCacheStats, Map<String, Map<String, Map<String, CacheStatsResponse>>>> setup = getNodeCacheStatsAndExpectedResultsForTiers(service, numIndexShards);
        NodeCacheStats stats = setup.v1();
        Map<String, Map<String, Map<String, CacheStatsResponse>>> expectedResultsByTier = setup.v2();

        AggregatedStats aggregated = stats.aggregateRequestStatsByLevel(new String[]{"tier"});

        for (String tierName : aggregated.getInnerMapKeySet(List.of())) {
            CacheStatsResponse tierTotal = sumTotal(expectedResultsByTier.get(tierName));
            assertEquals(tierTotal, aggregated.getResponse(List.of(tierName)));
        }
        assertEquals(CacheService.API_SUPPORTED_TIERS.size(), aggregated.getSize());
    }

    public void testRequestCacheShardsAndTiersAggregation() throws Exception {
        List<String> indexNames = List.of("index1", "index2");
        Map<String, Integer> numIndexShards = Map.of("index1", 4, "index2", 10);
        CacheService service = getCacheService(indexNames, numIndexShards);
        Tuple<NodeCacheStats, Map<String, Map<String, Map<String, CacheStatsResponse>>>> setup = getNodeCacheStatsAndExpectedResultsForTiers(service, numIndexShards);
        NodeCacheStats stats = setup.v1();
        Map<String, Map<String, Map<String, CacheStatsResponse>>> expectedResultsByTier = setup.v2();

        AggregatedStats aggregated = stats.aggregateRequestStatsByLevel(new String[]{"tier", "shards"});

        for (String shardName : aggregated.getInnerMapKeySet(List.of())) {
            for (String tierName : aggregated.getInnerMapKeySet(List.of(shardName))) {
                String indexName = service.getIndexNameFromShardName(shardName);
                assertEquals(expectedResultsByTier.get(tierName).get(indexName).get(shardName), aggregated.getResponse(List.of(shardName, tierName)));
            }
        }

        int shardsInExpectedResults = 0;
        for (String indexName : expectedResultsByTier.get(CacheService.TIER_DIMENSION_VALUE_ON_HEAP).keySet()) {
            shardsInExpectedResults += expectedResultsByTier.get(CacheService.TIER_DIMENSION_VALUE_ON_HEAP).get(indexName).keySet().size();
        }
        for (String indexName : expectedResultsByTier.get(CacheService.TIER_DIMENSION_VALUE_DISK).keySet()) {
            shardsInExpectedResults += expectedResultsByTier.get(CacheService.TIER_DIMENSION_VALUE_DISK).get(indexName).keySet().size();
        }
        assertEquals(shardsInExpectedResults, aggregated.getSize());
    }

    public void testRequestCacheIndicesAndTiersAggregation() throws Exception {
        List<String> indexNames = List.of("index1", "index2");
        Map<String, Integer> numIndexShards = Map.of("index1", 4, "index2", 10);
        CacheService service = getCacheService(indexNames, numIndexShards);
        Tuple<NodeCacheStats, Map<String, Map<String, Map<String, CacheStatsResponse>>>> setup = getNodeCacheStatsAndExpectedResultsForTiers(service, numIndexShards);
        NodeCacheStats stats = setup.v1();
        Map<String, Map<String, Map<String, CacheStatsResponse>>> expectedResultsByTier = setup.v2();

        AggregatedStats aggregated = stats.aggregateRequestStatsByLevel(new String[]{"tier", "indices"});

        for (String indexName : aggregated.getInnerMapKeySet(List.of())) {
            for (String tierName : aggregated.getInnerMapKeySet(List.of(indexName))) {
                CacheStatsResponse expectedIndexResult = sumByIndexName(expectedResultsByTier.get(tierName), indexName);
                assertEquals(expectedIndexResult, aggregated.getResponse(List.of(indexName, tierName)));
                assertEquals((long) numIndexShards.get(indexName), (long) expectedResultsByTier.get(tierName).get(indexName).keySet().size());
            }
        }
        assertEquals(indexNames.size() * CacheService.API_SUPPORTED_TIERS.size(), aggregated.getSize());
    }

    public void testRequestCacheNoLevels() throws Exception {
        List<String> indexNames = List.of("index1", "index2");
        Map<String, Integer> numIndexShards = Map.of("index1", 4, "index2", 10);
        CacheService service = getCacheService(indexNames, numIndexShards);
        Tuple<NodeCacheStats, Map<String, Map<String, Map<String, CacheStatsResponse>>>> setup = getNodeCacheStatsAndExpectedResultsForTiers(service, numIndexShards);
        NodeCacheStats stats = setup.v1();

        AggregatedStats aggregated = stats.aggregateRequestStatsByLevel(new String[]{});
        assertEquals(List.of(), aggregated.getDimensionNames());
        assertEquals(stats.getTotalRequestStats(), aggregated.getResponse(List.of()));
        assertEquals(1, aggregated.getSize());
    }

    public void testRequestCacheIncorrectLevels() throws Exception {
        List<String> indexNames = List.of("index1", "index2");
        Map<String, Integer> numIndexShards = Map.of("index1", 4, "index2", 10);
        CacheService service = getCacheService(indexNames, numIndexShards);
        Tuple<NodeCacheStats, Map<String, Map<String, Map<String, CacheStatsResponse>>>> setup = getNodeCacheStatsAndExpectedResultsForTiers(service, numIndexShards);
        NodeCacheStats stats = setup.v1();

        String[][] incorrectLevels = new String[][]{
            new String[] {"tiers"}, // Should be "tier"
            new String[] {"unknown_level"},
            new String[] {"shards", "indices"}, // Disallowed combination
            new String[] {"shards", "indices", "tier"}
        };
        for (String[] incorrectLevelsArr : incorrectLevels) {
            assertThrows(IllegalArgumentException.class, () -> stats.aggregateRequestStatsByLevel(incorrectLevelsArr));
        }
    }

    public void testSerialization() throws Exception {
        List<String> indexNames = List.of("index1", "index2");
        Map<String, Integer> numIndexShards = Map.of("index1", 4, "index2", 10);
        CacheService service = getCacheService(indexNames, numIndexShards);
        Tuple<NodeCacheStats, Map<String, Map<String, Map<String, CacheStatsResponse>>>> setup = getNodeCacheStatsAndExpectedResultsForTiers(service, numIndexShards);
        NodeCacheStats stats = setup.v1();

        BytesStreamOutput os = new BytesStreamOutput();
        stats.writeTo(os);
        BytesStreamInput is = new BytesStreamInput(BytesReference.toBytes(os.bytes()));
        NodeCacheStats deserialized = new NodeCacheStats(is);

        assertEquals(stats, deserialized);
    }

    private Tuple<NodeCacheStats, Map<String, Map<String, CacheStatsResponse>>> getNodeCacheStatsAndExpectedResultsForSingleTier(CacheService service, Map<String, Integer> numIndexShards, String tierName) {
        CacheServiceTests.MockCacheTier onHeap = new CacheServiceTests.MockCacheTier(new MultiDimensionCacheStats(List.of(CacheService.SHARDS_DIMENSION_NAME), tierName));
        service.registerCache(CacheType.INDICES_REQUEST_CACHE, onHeap);
        CacheStats stats = service.getCache(CacheType.INDICES_REQUEST_CACHE).stats();
        Map<String, Map<String, CacheStatsResponse>> expectedResults = CacheServiceTests.populateStats(service, stats, numIndexShards);
        CommonStatsFlags flags = new CommonStatsFlags();
        flags.includeCacheType(CacheType.INDICES_REQUEST_CACHE);
        return new Tuple<>(service.stats(flags), expectedResults);
    }

    private Tuple<NodeCacheStats, Map<String, Map<String, Map<String, CacheStatsResponse>>>> getNodeCacheStatsAndExpectedResultsForTiers(CacheService service, Map<String, Integer> numIndexShards) {
        CacheServiceTests.MockCacheTier tieredSpilloverCache = new CacheServiceTests.MockCacheTier(new CacheServiceTests.MockTieredSpilloverCacheStats(
            new MultiDimensionCacheStats(List.of(CacheService.SHARDS_DIMENSION_NAME), CacheService.TIER_DIMENSION_VALUE_ON_HEAP),
            new MultiDimensionCacheStats(List.of(CacheService.SHARDS_DIMENSION_NAME), CacheService.TIER_DIMENSION_VALUE_DISK)
        ));
        service.registerCache(CacheType.INDICES_REQUEST_CACHE, tieredSpilloverCache);

        CacheStats heapStats = ((CacheServiceTests.MockTieredSpilloverCacheStats) service.getCache(CacheType.INDICES_REQUEST_CACHE).stats()).heapStats;
        CacheStats diskStats = ((CacheServiceTests.MockTieredSpilloverCacheStats) service.getCache(CacheType.INDICES_REQUEST_CACHE).stats()).diskStats;
        Map<String, Map<String, CacheStatsResponse>> expectedHeapResults = CacheServiceTests.populateStats(service, heapStats, numIndexShards);
        Map<String, Map<String, CacheStatsResponse>> expectedDiskResults = CacheServiceTests.populateStats(service, diskStats, numIndexShards);
        Map<String, Map<String, Map<String, CacheStatsResponse>>> expectedResultsByTier = Map.of(CacheService.TIER_DIMENSION_VALUE_ON_HEAP, expectedHeapResults, CacheService.TIER_DIMENSION_VALUE_DISK, expectedDiskResults);
        CommonStatsFlags flags = new CommonStatsFlags();
        flags.includeCacheType(CacheType.INDICES_REQUEST_CACHE);
        return new Tuple<>(service.stats(flags), expectedResultsByTier);
    }

    // Duplicated from CacheServiceTests, we can't import it statically since it depends on the test case's node to get the IndicesService
    CacheService getCacheService(List<String> indexNames, Map<String, Integer> numIndexShards) {
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

    private CacheStatsResponse sumByIndexName(Map<String, Map<String, CacheStatsResponse>> expectedResults, String indexName) {
        CacheStatsResponse expectedIndexResult = new CacheStatsResponse();
        for (String shardName : expectedResults.get(indexName).keySet()) {
            expectedIndexResult.add(expectedResults.get(indexName).get(shardName));
        }
        return expectedIndexResult;
    }

    private CacheStatsResponse sumTotal(Map<String, Map<String, CacheStatsResponse>> expectedResults) {
        CacheStatsResponse expectedIndexResult = new CacheStatsResponse();
        for (String indexName : expectedResults.keySet()) {
            expectedIndexResult.add(sumByIndexName(expectedResults, indexName));
        }
        return expectedIndexResult;
    }
}
