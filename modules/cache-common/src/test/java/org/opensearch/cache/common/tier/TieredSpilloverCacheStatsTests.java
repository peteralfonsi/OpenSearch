/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cache.common.tier;

import org.opensearch.common.Randomness;
import org.opensearch.common.cache.Cache;
import org.opensearch.common.cache.ICacheKey;
import org.opensearch.common.cache.stats.CacheStats;
import org.opensearch.common.cache.stats.StatsHolder;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.BytesStreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

/*import static org.opensearch.cache.common.tier.TieredSpilloverCacheStats.TIER_DIMENSION_NAME;
import static org.opensearch.cache.common.tier.TieredSpilloverCacheStats.TIER_DIMENSION_VALUE_ON_HEAP;
import static org.opensearch.cache.common.tier.TieredSpilloverCacheStats.TIER_DIMENSION_VALUE_DISK;
import static org.opensearch.cache.common.tier.TieredSpilloverCacheStats.combineTierResponses;*/

public class TieredSpilloverCacheStatsTests extends OpenSearchTestCase {
    /*private static List<String> dimensionNames = List.of("dim1", "dim2", "dim3");
    private static List<String> tierNames = List.of(TIER_DIMENSION_VALUE_ON_HEAP, TIER_DIMENSION_VALUE_DISK);
    public void testGets() throws Exception {
        StatsHolder heapStats = new StatsHolder(dimensionNames);
        StatsHolder diskStats = new StatsHolder(dimensionNames);
        Map<String, List<String>> usedDimensionValues = getUsedDimensionValues(heapStats, 10);
        Map<String, Map<Set<CacheStatsDimension>, CacheStatsResponse>> expected = populateStats(heapStats, diskStats, usedDimensionValues, 100, 2);
        TieredSpilloverCacheStats stats = new TieredSpilloverCacheStats(heapStats.createSnapshot(), diskStats.createSnapshot(), dimensionNames);

        CacheStatsResponse heapTotalStats = totalSumExpected(expected.get(TIER_DIMENSION_VALUE_ON_HEAP));
        CacheStatsResponse diskTotalStats = totalSumExpected(expected.get(TIER_DIMENSION_VALUE_DISK));
        CacheStatsResponse.Snapshot totalTSCStats = TieredSpilloverCacheStats.combineTierResponses(
            heapTotalStats.snapshot(), diskTotalStats.snapshot());

        // test total gets
        assertEquals(totalTSCStats, stats.getTotalStats());

        assertEquals(totalTSCStats.getHits(), stats.getTotalHits());
        assertEquals(totalTSCStats.getMisses(), stats.getTotalMisses());
        assertEquals(totalTSCStats.getEvictions(), stats.getTotalEvictions());
        assertEquals(totalTSCStats.getSizeInBytes(), stats.getTotalSizeInBytes());
        assertEquals(totalTSCStats.getEntries(), stats.getTotalEntries());

        assertEquals(heapTotalStats.snapshot(), stats.getTotalHeapStats());
        assertEquals(diskTotalStats.snapshot(), stats.getTotalDiskStats());
    }

    public void testEmptyDimensionNames() throws Exception {
        StatsHolder heapStats = new StatsHolder(List.of());
        StatsHolder diskStats = new StatsHolder(List.of());

        Map<String, List<String>> usedDimensionValues = getUsedDimensionValues(heapStats, 10);
        Map<String, Map<Set<CacheStatsDimension>, CacheStatsResponse>> expected = populateStats(heapStats, diskStats, usedDimensionValues, 10, 10);
        TieredSpilloverCacheStats stats = new TieredSpilloverCacheStats(heapStats.createSnapshot(), diskStats.createSnapshot(), List.of());

        CacheStatsResponse heapTotalStats = totalSumExpected(expected.get(TIER_DIMENSION_VALUE_ON_HEAP));
        CacheStatsResponse diskTotalStats = totalSumExpected(expected.get(TIER_DIMENSION_VALUE_DISK));
        CacheStatsResponse.Snapshot totalTSCStats = TieredSpilloverCacheStats.combineTierResponses(heapTotalStats.snapshot(), diskTotalStats.snapshot());

        assertEquals(totalTSCStats, stats.getTotalStats());
    }

    public void testSerialization() throws Exception {
        StatsHolder heapStats = new StatsHolder(dimensionNames);
        StatsHolder diskStats = new StatsHolder(dimensionNames);
        Map<String, List<String>> usedDimensionValues = getUsedDimensionValues(heapStats, 10);
        Map<String, Map<Set<CacheStatsDimension>, CacheStatsResponse>> expected = populateStats(heapStats, diskStats, usedDimensionValues, 100, 2);
        TieredSpilloverCacheStats stats = new TieredSpilloverCacheStats(heapStats.createSnapshot(), diskStats.createSnapshot(), dimensionNames);

        BytesStreamOutput os = new BytesStreamOutput();
        stats.writeTo(os);
        BytesStreamInput is = new BytesStreamInput(BytesReference.toBytes(os.bytes()));
        TieredSpilloverCacheStats deserialized = new TieredSpilloverCacheStats(is);

        assertEquals(stats.heapStats.aggregateByLevels(dimensionNames), deserialized.heapStats.aggregateByLevels(dimensionNames));
        assertEquals(stats.diskStats.aggregateByLevels(dimensionNames), deserialized.diskStats.aggregateByLevels(dimensionNames));
    }

    public void testCombineTierResponses() throws Exception {
        CacheStatsResponse.Snapshot heapResponse = new CacheStatsResponse.Snapshot(1,2,3,4,5);
        CacheStatsResponse.Snapshot diskResponse = new CacheStatsResponse.Snapshot(2,3,4,5,6);
        CacheStatsResponse.Snapshot tscResponse = TieredSpilloverCacheStats.combineTierResponses(heapResponse, diskResponse);
        assertEquals(new CacheStatsResponse.Snapshot(3, 3, 4, 9, 11), tscResponse);
    }

    public void testAggregationSomeLevelsWithoutTier() throws Exception {
        StatsHolder heapStats = new StatsHolder(dimensionNames);
        StatsHolder diskStats = new StatsHolder(dimensionNames);
        Map<String, List<String>> usedDimensionValues = getUsedDimensionValues(heapStats, 10);
        Map<String, Map<Set<CacheStatsDimension>, CacheStatsResponse>> expected = populateStats(heapStats, diskStats, usedDimensionValues, 100, 2);
        TieredSpilloverCacheStats stats = new TieredSpilloverCacheStats(heapStats.createSnapshot(), diskStats.createSnapshot(), dimensionNames);

        for (int i = 0; i < (1 << dimensionNames.size()); i++) {
            // Test each combination of possible levels
            List<String> levels = new ArrayList<>();
            for (int nameIndex = 0; nameIndex < dimensionNames.size(); nameIndex++) {
                if ((i & (1 << nameIndex)) != 0) {
                    levels.add(dimensionNames.get(nameIndex));
                }
            }
            if (levels.size() == 0) {
                assertThrows(IllegalArgumentException.class, () -> stats.aggregateByLevels(levels));
            }
            else {
                Map<StatsHolder.Key, CacheStatsResponse.Snapshot> aggregated = stats.aggregateByLevels(levels);
                for (Map.Entry<StatsHolder.Key, CacheStatsResponse.Snapshot> aggregatedEntry : aggregated.entrySet()) {
                    StatsHolder.Key aggregatedKey = aggregatedEntry.getKey();
                    Map<String, CacheStatsResponse> expectedResponseForTierMap = new HashMap<>();
                    for (String tier : new String[]{TIER_DIMENSION_VALUE_ON_HEAP, TIER_DIMENSION_VALUE_DISK}) {
                        CacheStatsResponse expectedResponseForTier = new CacheStatsResponse();
                        for (Set<CacheStatsDimension> expectedDims : expected.get(tier).keySet()) {
                            List<String> orderedDimValues = StatsHolder.getOrderedDimensionValues(
                                new ArrayList<>(expectedDims),
                                dimensionNames
                            );
                            if (orderedDimValues.containsAll(aggregatedKey.getDimensionValues())) {
                                expectedResponseForTier.add(expected.get(tier).get(expectedDims));
                            }
                        }
                        if (expectedResponseForTier.equals(new CacheStatsResponse())) {
                            expectedResponseForTier = null; // If it's all 0, there were no keys
                        }
                        expectedResponseForTierMap.put(tier, expectedResponseForTier);
                    }
                    CacheStatsResponse expectedHeapResponse = expectedResponseForTierMap.get(TIER_DIMENSION_VALUE_ON_HEAP);
                    CacheStatsResponse expectedDiskResponse = expectedResponseForTierMap.get(TIER_DIMENSION_VALUE_DISK);
                    if (expectedHeapResponse != null && expectedDiskResponse != null) {
                        assertEquals(combineTierResponses(expectedHeapResponse.snapshot(), expectedDiskResponse.snapshot()), aggregatedEntry.getValue());
                    } else if (expectedHeapResponse != null) {
                        assertEquals(expectedHeapResponse.snapshot(), aggregatedEntry.getValue());
                    } else {
                        assertEquals(expectedDiskResponse.snapshot(), aggregatedEntry.getValue());
                    }
                }
            }
        }
    }

    public void testAggregationSomeLevelsWithTier() throws Exception {
        StatsHolder heapStats = new StatsHolder(dimensionNames);
        StatsHolder diskStats = new StatsHolder(dimensionNames);
        Map<String, List<String>> usedDimensionValues = getUsedDimensionValues(heapStats, 10);
        Map<String, Map<Set<CacheStatsDimension>, CacheStatsResponse>> expected = populateStats(heapStats, diskStats, usedDimensionValues, 100, 2);
        TieredSpilloverCacheStats stats = new TieredSpilloverCacheStats(heapStats.createSnapshot(), diskStats.createSnapshot(), dimensionNames);

        for (int i = 0; i < (1 << dimensionNames.size()); i++) {
            // Test each combination of possible levels
            List<String> levels = new ArrayList<>();
            for (int nameIndex = 0; nameIndex < dimensionNames.size(); nameIndex++) {
                if ((i & (1 << nameIndex)) != 0) {
                    levels.add(dimensionNames.get(nameIndex));
                }
            }
            levels.add(TIER_DIMENSION_NAME);
            if (levels.size() == 1) {
                assertThrows(IllegalArgumentException.class, () -> stats.aggregateByLevels(levels));
            }
            else {
                Map<StatsHolder.Key, CacheStatsResponse.Snapshot> aggregated = stats.aggregateByLevels(levels);
                for (Map.Entry<StatsHolder.Key, CacheStatsResponse.Snapshot> aggregatedEntry : aggregated.entrySet()) {
                    StatsHolder.Key aggregatedKey = aggregatedEntry.getKey();
                    String aggregatedKeyTier = aggregatedKey.getDimensionValues().get(aggregatedKey.getDimensionValues().size()-1);
                    CacheStatsResponse expectedResponse = new CacheStatsResponse();
                    for (Set<CacheStatsDimension> expectedDims : expected.get(aggregatedKeyTier).keySet()) {
                        List<String> orderedDimValues = StatsHolder.getOrderedDimensionValues(
                            new ArrayList<>(expectedDims),
                            dimensionNames
                        );
                        orderedDimValues.add(aggregatedKeyTier);
                        if (orderedDimValues.containsAll(aggregatedKey.getDimensionValues())) {
                            expectedResponse.add(expected.get(aggregatedKeyTier).get(expectedDims));
                        }
                    }
                    assertEquals(expectedResponse.snapshot(), aggregatedEntry.getValue());
                }
            }
        }
    }

    private CacheStatsResponse totalSumExpected(Map<Set<CacheStatsDimension>, CacheStatsResponse> expected) {
        CacheStatsResponse result = new CacheStatsResponse();
        for (Set<CacheStatsDimension> key : expected.keySet()) {
            result.add(expected.get(key));
        }
        return result;
    }

    // Fill the tier stats and return a nested map from tier type and dimensions -> expected response
    // Modified from MultiDimensionCacheStatsTests - we can't import it without adding a dependency on server.test module.
    private Map<String, Map<Set<CacheStatsDimension>, CacheStatsResponse>> populateStats(StatsHolder heapStats, StatsHolder diskStats, Map<String, List<String>> usedDimensionValues, int numDistinctValuePairs, int numRepetitionsPerValue) {
        Map<String, Map<Set<CacheStatsDimension>, CacheStatsResponse>> expected = new HashMap<>();
        expected.put(TIER_DIMENSION_VALUE_ON_HEAP, new HashMap<>());
        expected.put(TIER_DIMENSION_VALUE_DISK, new HashMap<>());

        Random rand = Randomness.get();
        Map<String, StatsHolder> statsHolderMap = Map.of(tierNames.get(0), heapStats, tierNames.get(1), diskStats);
        for (String tier : tierNames) {
            for (int i = 0; i < numDistinctValuePairs; i++) {
                StatsHolder stats = statsHolderMap.get(tier);
                List<CacheStatsDimension> dimensions = getRandomDimList(stats.getDimensionNames(), usedDimensionValues, true, rand);
                Set<CacheStatsDimension> dimSet = new HashSet<>(dimensions);
                Map<Set<CacheStatsDimension>, CacheStatsResponse> tierExpected = expected.get(tier);
                if (tierExpected.get(dimSet) == null) {
                    tierExpected.put(dimSet, new CacheStatsResponse());
                }
                ICacheKey<String> dummyKey = getDummyKey(dimensions);
                for (int j = 0; j < numRepetitionsPerValue; j++) {

                    int numHitIncrements = rand.nextInt(10);
                    for (int k = 0; k < numHitIncrements; k++) {
                        stats.incrementHits(dummyKey);
                        tierExpected.get(new HashSet<>(dimensions)).hits.inc();
                    }

                    int numMissIncrements = rand.nextInt(10);
                    for (int k = 0; k < numMissIncrements; k++) {
                        stats.incrementMisses(dummyKey);
                        tierExpected.get(new HashSet<>(dimensions)).misses.inc();
                    }

                    int numEvictionIncrements = rand.nextInt(10);
                    for (int k = 0; k < numEvictionIncrements; k++) {
                        stats.incrementEvictions(dummyKey);
                        tierExpected.get(new HashSet<>(dimensions)).evictions.inc();
                    }

                    int numMemorySizeIncrements = rand.nextInt(10);
                    for (int k = 0; k < numMemorySizeIncrements; k++) {
                        long memIncrementAmount = rand.nextInt(5000);
                        stats.incrementSizeInBytes(dummyKey, memIncrementAmount);
                        tierExpected.get(new HashSet<>(dimensions)).sizeInBytes.inc(memIncrementAmount);
                    }

                    int numEntryIncrements = rand.nextInt(9) + 1;
                    for (int k = 0; k < numEntryIncrements; k++) {
                        stats.incrementEntries(dummyKey);
                        tierExpected.get(new HashSet<>(dimensions)).entries.inc();
                    }

                    int numEntryDecrements = rand.nextInt(numEntryIncrements);
                    for (int k = 0; k < numEntryDecrements; k++) {
                        stats.decrementEntries(dummyKey);
                        tierExpected.get(new HashSet<>(dimensions)).entries.dec();
                    }
                }

            }
        }
        return expected;
    }

    // Duplicated below functions from MultiDimensionCacheStatsTests. We can't import them without adding a dependency on server.test for this module.

    private List<CacheStatsDimension> getRandomDimList(List<String> dimensionNames, Map<String, List<String>> usedDimensionValues, boolean pickValueForAllDims, Random rand) {
        List<CacheStatsDimension> result = new ArrayList<>();
        for (String dimName : dimensionNames) {
            if (pickValueForAllDims || rand.nextBoolean()) { // if pickValueForAllDims, always pick a value for each dimension, otherwise do so 50% of the time
                int index = between(0, usedDimensionValues.get(dimName).size() - 1);
                result.add(new CacheStatsDimension(dimName, usedDimensionValues.get(dimName).get(index)));
            }
        }
        return result;
    }
    private Map<String, List<String>> getUsedDimensionValues(StatsHolder stats, int numValuesPerDim) {
        Map<String, List<String>> usedDimensionValues = new HashMap<>();
        for (int i = 0; i < stats.getDimensionNames().size(); i++) {
            List<String> values = new ArrayList<>();
            for (int j = 0; j < numValuesPerDim; j++) {
                values.add(UUID.randomUUID().toString());
            }
            usedDimensionValues.put(stats.getDimensionNames().get(i), values);
        }
        return usedDimensionValues;
    }

    private static ICacheKey<String> getDummyKey(List<CacheStatsDimension> dims) {
        return new ICacheKey<>(null, dims);
    }*/

}
