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
import org.opensearch.common.cache.stats.CacheStats;
import org.opensearch.common.cache.stats.CacheStatsDimension;
import org.opensearch.common.cache.stats.CacheStatsResponse;
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
import java.util.UUID;

public class TieredSpilloverCacheStatsTests extends OpenSearchTestCase {
    private static List<String> dimensionNames = List.of("dim1", "dim2", "dim3");
    private static List<String> tierNames = List.of(TieredSpilloverCacheStats.TIER_DIMENSION_VALUE_ON_HEAP, TieredSpilloverCacheStats.TIER_DIMENSION_VALUE_DISK);
    public void testGets() throws Exception {
        StatsHolder heapStats = new StatsHolder(dimensionNames, Settings.EMPTY, StatsHolder.TrackingMode.ALL_COMBINATIONS);
        StatsHolder diskStats = new StatsHolder(dimensionNames, Settings.EMPTY, StatsHolder.TrackingMode.ALL_COMBINATIONS);
        Map<String, List<String>> usedDimensionValues = getUsedDimensionValues(heapStats, 10);
        Map<String, Map<Set<CacheStatsDimension>, CacheStatsResponse>> expected = populateStats(heapStats, diskStats, usedDimensionValues, 100, 2);
        TieredSpilloverCacheStats stats = new TieredSpilloverCacheStats(heapStats, diskStats);

        CacheStatsResponse heapTotalStats = totalSumExpected(expected.get(TieredSpilloverCacheStats.TIER_DIMENSION_VALUE_ON_HEAP));
        CacheStatsResponse diskTotalStats = totalSumExpected(expected.get(TieredSpilloverCacheStats.TIER_DIMENSION_VALUE_DISK));
        CacheStatsResponse totalTSCStats = TieredSpilloverCacheStats.combineTierResponses(heapTotalStats, diskTotalStats);

        // test total gets
        assertEquals(totalTSCStats, stats.getTotalStats());
        assertEquals(totalTSCStats, stats.getStatsByDimensions(List.of()));

        assertEquals(totalTSCStats.getHits(), stats.getTotalHits());
        assertEquals(totalTSCStats.getMisses(), stats.getTotalMisses());
        assertEquals(totalTSCStats.getEvictions(), stats.getTotalEvictions());
        assertEquals(totalTSCStats.getMemorySize(), stats.getTotalMemorySize());
        assertEquals(totalTSCStats.getEntries(), stats.getTotalEntries());


        // test gets by non-tier dimensions, following same method as MultiDimensionCacheStatsTests
        for (int i = 0; i < 100; i++) {
            List<CacheStatsDimension> aggregationDims = getRandomDimList(dimensionNames, usedDimensionValues, false, Randomness.get());

            Map<String, CacheStatsResponse> expectedTierResponses = new HashMap<>();
            for (String tier : expected.keySet()) {
                CacheStatsResponse expectedTierResponse = new CacheStatsResponse();
                for (Set<CacheStatsDimension> dimSet : expected.get(tier).keySet()) {
                    if (dimSet.containsAll(aggregationDims)) {
                        expectedTierResponse.add(expected.get(tier).get(dimSet));
                    }
                }
                expectedTierResponses.put(tier, expectedTierResponse);
            }
            CacheStatsResponse expectedTotalResponse = TieredSpilloverCacheStats.combineTierResponses(
                expectedTierResponses.get(TieredSpilloverCacheStats.TIER_DIMENSION_VALUE_ON_HEAP),
                expectedTierResponses.get(TieredSpilloverCacheStats.TIER_DIMENSION_VALUE_DISK));
            checkStatsObject(expectedTotalResponse, stats, aggregationDims);
        }

        // test gets by tier dimension only
        List<CacheStatsDimension> heapDims = List.of(new CacheStatsDimension(CacheStatsDimension.TIER_DIMENSION_NAME, TieredSpilloverCacheStats.TIER_DIMENSION_VALUE_ON_HEAP));
        List<CacheStatsDimension> diskDims = List.of(new CacheStatsDimension(CacheStatsDimension.TIER_DIMENSION_NAME, TieredSpilloverCacheStats.TIER_DIMENSION_VALUE_DISK));
        List<List<CacheStatsDimension>> tierDimsList = Arrays.asList(heapDims, diskDims);
        List<CacheStatsResponse> totalResponses = List.of(heapTotalStats, diskTotalStats);

        for (int i = 0; i < 2; i++) {
            CacheStatsResponse tierTotalStats = totalResponses.get(i);
            List<CacheStatsDimension> tierDims = tierDimsList.get(i);
            checkStatsObject(tierTotalStats, stats, tierDims);
        }


        // test gets by tier dimension and other dimension
        for (String tier : tierNames) {
            for (int i = 0; i < 100; i++) {
                List<CacheStatsDimension> aggregationDims = getRandomDimList(dimensionNames, usedDimensionValues, false, Randomness.get());
                CacheStatsResponse expectedResponse = new CacheStatsResponse();
                for (Set<CacheStatsDimension> dimSet : expected.get(tier).keySet()) {
                    if (dimSet.containsAll(aggregationDims)) {
                        expectedResponse.add(expected.get(tier).get(dimSet));
                    }
                }
                List<CacheStatsDimension> aggDimsWithTier = new ArrayList<>(aggregationDims);
                aggDimsWithTier.add(new CacheStatsDimension(CacheStatsDimension.TIER_DIMENSION_NAME, tier));
                checkStatsObject(expectedResponse, stats, aggDimsWithTier);
            }
        }
    }

    public void testInvalidTierName() throws Exception {
        StatsHolder heapStats = new StatsHolder(dimensionNames, Settings.EMPTY, StatsHolder.TrackingMode.ALL_COMBINATIONS);
        StatsHolder diskStats = new StatsHolder(dimensionNames, Settings.EMPTY, StatsHolder.TrackingMode.ALL_COMBINATIONS);
        Map<String, List<String>> usedDimensionValues = getUsedDimensionValues(heapStats, 10);
        populateStats(heapStats, diskStats, usedDimensionValues, 10, 10);
        TieredSpilloverCacheStats stats = new TieredSpilloverCacheStats(heapStats, diskStats);

        List<CacheStatsDimension> invalidTierDims = List.of(new CacheStatsDimension(CacheStatsDimension.TIER_DIMENSION_NAME, "nonexistent_tier"));
        assertThrows(IllegalArgumentException.class, () -> stats.getEntriesByDimensions(invalidTierDims));
    }

    public void testEmptyDimensionNames() throws Exception {
        StatsHolder heapStats = new StatsHolder(List.of(), Settings.EMPTY, StatsHolder.TrackingMode.ALL_COMBINATIONS);
        StatsHolder diskStats = new StatsHolder(List.of(), Settings.EMPTY, StatsHolder.TrackingMode.ALL_COMBINATIONS);

        Map<String, List<String>> usedDimensionValues = getUsedDimensionValues(heapStats, 10);
        Map<String, Map<Set<CacheStatsDimension>, CacheStatsResponse>> expected = populateStats(heapStats, diskStats, usedDimensionValues, 10, 10);
        TieredSpilloverCacheStats stats = new TieredSpilloverCacheStats(heapStats, diskStats);

        CacheStatsResponse heapTotalStats = totalSumExpected(expected.get(TieredSpilloverCacheStats.TIER_DIMENSION_VALUE_ON_HEAP));
        CacheStatsResponse diskTotalStats = totalSumExpected(expected.get(TieredSpilloverCacheStats.TIER_DIMENSION_VALUE_DISK));
        CacheStatsResponse totalTSCStats = TieredSpilloverCacheStats.combineTierResponses(heapTotalStats, diskTotalStats);

        checkStatsObject(heapTotalStats, stats, List.of(new CacheStatsDimension(CacheStatsDimension.TIER_DIMENSION_NAME, TieredSpilloverCacheStats.TIER_DIMENSION_VALUE_ON_HEAP)));
        checkStatsObject(diskTotalStats, stats, List.of(new CacheStatsDimension(CacheStatsDimension.TIER_DIMENSION_NAME, TieredSpilloverCacheStats.TIER_DIMENSION_VALUE_DISK)));

        assertEquals(totalTSCStats, stats.getStatsByDimensions(List.of()));
        assertEquals(totalTSCStats, stats.getTotalStats());
    }

    public void testSerialization() throws Exception {
        StatsHolder heapStats = new StatsHolder(dimensionNames, Settings.EMPTY, StatsHolder.TrackingMode.ALL_COMBINATIONS);
        StatsHolder diskStats = new StatsHolder(dimensionNames, Settings.EMPTY, StatsHolder.TrackingMode.ALL_COMBINATIONS);
        Map<String, List<String>> usedDimensionValues = getUsedDimensionValues(heapStats, 10);
        Map<String, Map<Set<CacheStatsDimension>, CacheStatsResponse>> expected = populateStats(heapStats, diskStats, usedDimensionValues, 100, 2);
        TieredSpilloverCacheStats stats = new TieredSpilloverCacheStats(heapStats, diskStats);

        BytesStreamOutput os = new BytesStreamOutput();
        stats.writeTo(os);
        BytesStreamInput is = new BytesStreamInput(BytesReference.toBytes(os.bytes()));
        TieredSpilloverCacheStats deserialized = new TieredSpilloverCacheStats(is);

        for (String tier : expected.keySet()) {
            for (Set<CacheStatsDimension> dimsSet : expected.get(tier).keySet()) {
                CacheStatsResponse originalResponse = stats.getStatsByDimensions(new ArrayList<>(dimsSet));
                CacheStatsResponse deserializedResponse = deserialized.getStatsByDimensions(new ArrayList<>(dimsSet));
                assertEquals(originalResponse, deserializedResponse);
            }
        }
    }

    public void testCombineTierResponses() throws Exception {
        CacheStatsResponse heapResponse = new CacheStatsResponse(1,2,3,4,5);
        CacheStatsResponse diskResponse = new CacheStatsResponse(2,3,4,5,6);
        CacheStatsResponse tscResponse = TieredSpilloverCacheStats.combineTierResponses(heapResponse, diskResponse);
        assertEquals(new CacheStatsResponse(3, 3, 4, 9, 11), tscResponse);
    }

    private void checkStatsObject(CacheStatsResponse expected, CacheStats stats, List<CacheStatsDimension> aggregationDims) {
        assertEquals(expected, stats.getStatsByDimensions(aggregationDims));
        assertEquals(expected.getHits(), stats.getHitsByDimensions(aggregationDims));
        assertEquals(expected.getMisses(), stats.getMissesByDimensions(aggregationDims));
        assertEquals(expected.getEvictions(), stats.getEvictionsByDimensions(aggregationDims));
        assertEquals(expected.getMemorySize(), stats.getMemorySizeByDimensions(aggregationDims));
        assertEquals(expected.getEntries(), stats.getEntriesByDimensions(aggregationDims));
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
        expected.put(TieredSpilloverCacheStats.TIER_DIMENSION_VALUE_ON_HEAP, new HashMap<>());
        expected.put(TieredSpilloverCacheStats.TIER_DIMENSION_VALUE_DISK, new HashMap<>());

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
                for (int j = 0; j < numRepetitionsPerValue; j++) {

                    int numHitIncrements = rand.nextInt(10);
                    for (int k = 0; k < numHitIncrements; k++) {
                        stats.incrementHitsByDimensions(dimensions);
                        tierExpected.get(new HashSet<>(dimensions)).hits.inc();
                    }

                    int numMissIncrements = rand.nextInt(10);
                    for (int k = 0; k < numMissIncrements; k++) {
                        stats.incrementMissesByDimensions(dimensions);
                        tierExpected.get(new HashSet<>(dimensions)).misses.inc();
                    }

                    int numEvictionIncrements = rand.nextInt(10);
                    for (int k = 0; k < numEvictionIncrements; k++) {
                        stats.incrementEvictionsByDimensions(dimensions);
                        tierExpected.get(new HashSet<>(dimensions)).evictions.inc();
                    }

                    int numMemorySizeIncrements = rand.nextInt(10);
                    for (int k = 0; k < numMemorySizeIncrements; k++) {
                        long memIncrementAmount = rand.nextInt(5000);
                        stats.incrementMemorySizeByDimensions(dimensions, memIncrementAmount);
                        tierExpected.get(new HashSet<>(dimensions)).memorySize.inc(memIncrementAmount);
                    }

                    int numEntryIncrements = rand.nextInt(9) + 1;
                    for (int k = 0; k < numEntryIncrements; k++) {
                        stats.incrementEntriesByDimensions(dimensions);
                        tierExpected.get(new HashSet<>(dimensions)).entries.inc();
                    }

                    int numEntryDecrements = rand.nextInt(numEntryIncrements);
                    for (int k = 0; k < numEntryDecrements; k++) {
                        stats.decrementEntriesByDimensions(dimensions);
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
}
