/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.stats;

import org.opensearch.common.Randomness;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.BytesStreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class SingleDimensionCacheStatsTests extends OpenSearchTestCase {
    private final String dimensionName = "shardId";
    private final String tierName = "test_tier";
    public void testAddAndGet() throws Exception {
        StatsAndExpectedResults statsAndExpectedResults = getPopulatedStats(tierName);
        SingleDimensionCacheStats stats = statsAndExpectedResults.stats;

        checkShardResults(statsAndExpectedResults);
        checkTotalResults(statsAndExpectedResults);

        // Check values returned for a nonexistent dimension value or name return 0
        assertEquals(0, stats.getHitsByDimensions(List.of(new CacheStatsDimension(dimensionName, "nonexistent"))));
        assertEquals(0, stats.getHitsByDimensions(List.of(new CacheStatsDimension("nonexistentName", "nonexistentValue"))));

        // Check sending too many values causes an assertion error
        assertThrows(AssertionError.class, () -> stats.getHitsByDimensions(List.of(getDim(0), new CacheStatsDimension("test", "value"))));
    }

    public void testTierFiltering() throws Exception {
        StatsAndExpectedResults statsAndExpectedResults = getPopulatedStats(tierName);
        SingleDimensionCacheStats stats = statsAndExpectedResults.stats;

        // Values should be returned if the tier dimension value matches the one passed to SingleDimensionCacheStats. Otherwise we should get 0.
        CacheStatsDimension matchingTierDim = new CacheStatsDimension(CacheStatsDimension.TIER_DIMENSION_NAME, tierName);
        CacheStatsDimension nonMatchingTierDim = new CacheStatsDimension(CacheStatsDimension.TIER_DIMENSION_NAME, "another_tier");

        assertEquals(stats.getTotalHits(), stats.getHitsByDimensions(List.of(matchingTierDim)));
        assertEquals(0, stats.getHitsByDimensions(List.of(nonMatchingTierDim)));
        for (int i = 0; i < statsAndExpectedResults.numShardIds; i++) {
            assertEquals(stats.getHitsByDimensions(List.of(getDim(i))), stats.getHitsByDimensions(List.of(getDim(i), matchingTierDim)));
            assertEquals(stats.getHitsByDimensions(List.of(getDim(i))), stats.getHitsByDimensions(List.of(matchingTierDim, getDim(i))));
            assertEquals(0, stats.getHitsByDimensions(List.of(getDim(i), nonMatchingTierDim)));
            assertEquals(0, stats.getHitsByDimensions(List.of(nonMatchingTierDim, getDim(i))));

        }
        // Check sending too many values causes an assertion error
        assertThrows(AssertionError.class, () -> stats.getHitsByDimensions(List.of(getDim(0), matchingTierDim, new CacheStatsDimension("test", "value"))));
    }

    public void testSerialization() throws Exception {
        StatsAndExpectedResults statsAndExpectedResults = getPopulatedStats(tierName);
        SingleDimensionCacheStats stats = statsAndExpectedResults.stats;
        Map<String, Map<String, Long>> expectedResults = statsAndExpectedResults.expectedShardResults;

        BytesStreamOutput os = new BytesStreamOutput();
        stats.writeTo(os);
        BytesStreamInput is = new BytesStreamInput(BytesReference.toBytes(os.bytes()));
        SingleDimensionCacheStats deserialized = new SingleDimensionCacheStats(is);

        StatsAndExpectedResults deserializedStatsAndExpectedResults = new StatsAndExpectedResults(deserialized, expectedResults, statsAndExpectedResults.numShardIds);
        checkShardResults(deserializedStatsAndExpectedResults);
        checkTotalResults(deserializedStatsAndExpectedResults);
        assertEquals(deserialized.allowedDimensionName, stats.allowedDimensionName);
        assertEquals(deserialized.tierDimensionValue, stats.tierDimensionValue);
    }

    private CacheStatsDimension getDim(int i) {
        return new CacheStatsDimension(dimensionName, String.valueOf(i));
    }

    private List<CacheStatsDimension> getDimList(int i) {
        ArrayList<CacheStatsDimension> result = new ArrayList<>();
        result.add(getDim(i));
        return result;
    }

    private long sumMap(Map<String, Long> inputMap) {
        long result = 0;
        for (String key : inputMap.keySet()) {
            result += inputMap.get(key);
        }
        return result;
    }

    private StatsAndExpectedResults getPopulatedStats(String tierName) {
        SingleDimensionCacheStats stats = new SingleDimensionCacheStats(dimensionName, tierName);

        int numShardIds = 10;
        Map<String, Long> expectedHits = new HashMap<>();
        Map<String, Long> expectedMisses = new HashMap<>();
        Map<String, Long> expectedEvictions = new HashMap<>();
        Map<String, Long> expectedMemorySize = new HashMap<>();
        Map<String, Long> expectedEntries = new HashMap<>();

        Random rand = Randomness.get();

        // For each shard id value, increment metrics some random number of times (possibly 0)
        for (int shardId = 0; shardId < numShardIds; shardId++) {

            String shardIdString = String.valueOf(shardId);
            List<CacheStatsDimension> dimensions = getDimList(shardId);

            for (Map<String, Long> map : new Map[]{expectedHits, expectedMisses, expectedEvictions, expectedMemorySize, expectedEntries}) {
                map.put(shardIdString, 0L);
            }

            int numHitIncrements = rand.nextInt(10);
            for (int i = 0; i < numHitIncrements; i++) {
                stats.incrementHitsByDimensions(dimensions);
                expectedHits.put(shardIdString, expectedHits.get(shardIdString) + 1);
            }

            int numMissIncrements = rand.nextInt(10);
            for (int i = 0; i < numMissIncrements; i++) {
                stats.incrementMissesByDimensions(dimensions);
                expectedMisses.put(shardIdString, expectedMisses.get(shardIdString) + 1);
            }

            int numEvictionIncrements = rand.nextInt(10);
            for (int i = 0; i < numEvictionIncrements; i++) {
                stats.incrementEvictionsByDimensions(dimensions);
                expectedEvictions.put(shardIdString, expectedEvictions.get(shardIdString) + 1);
            }

            int numMemorySizeIncrements = rand.nextInt(10);
            for (int i = 0; i < numMemorySizeIncrements; i++) {
                long memIncrementAmount = (long) rand.nextInt(5000);
                stats.incrementMemorySizeByDimensions(dimensions, memIncrementAmount);
                expectedMemorySize.put(shardIdString, expectedMemorySize.get(shardIdString) + memIncrementAmount);
            }

            int numEntryIncrements = rand.nextInt(9) + 1;
            for (int i = 0; i < numEntryIncrements; i++) {
                stats.incrementEntriesByDimensions(dimensions);
                expectedEntries.put(shardIdString, expectedEntries.get(shardIdString) + 1);
            }

            int numEntryDecrements = rand.nextInt(numEntryIncrements);
            for (int i = 0; i < numEntryDecrements; i++) {
                stats.decrementEntriesByDimensions(dimensions);
                expectedEntries.put(shardIdString, expectedEntries.get(shardIdString) - 1);
            }
        }
        Map<String, Map<String, Long>> expectedShardResults = new HashMap<>();
        expectedShardResults.put("hits", expectedHits);
        expectedShardResults.put("misses", expectedMisses);
        expectedShardResults.put("evictions", expectedEvictions);
        expectedShardResults.put("memory_size", expectedMemorySize);
        expectedShardResults.put("entries", expectedEntries);
        return new StatsAndExpectedResults(stats, expectedShardResults, numShardIds);
    }

    private void checkShardResults(StatsAndExpectedResults statsAndExpectedResults) {
        // check the resulting values on dimension level are what we expect
        Map<String, Map<String, Long>> expectedResults = statsAndExpectedResults.expectedShardResults;
        SingleDimensionCacheStats stats = statsAndExpectedResults.stats;
        for (int shardId = 0; shardId < statsAndExpectedResults.numShardIds; shardId++) {
            String shardIdString = String.valueOf(shardId);
            CacheStatsDimension dimension = getDim(shardId);

            // Check the individual metric getters
            assertEquals((long) expectedResults.get("hits").get(shardIdString), stats.getHitsByDimensions(List.of(dimension)));
            assertEquals((long) expectedResults.get("misses").get(shardIdString), stats.getMissesByDimensions(List.of(dimension)));
            assertEquals((long) expectedResults.get("evictions").get(shardIdString), stats.getEvictionsByDimensions(List.of(dimension)));
            assertEquals((long) expectedResults.get("memory_size").get(shardIdString), stats.getMemorySizeByDimensions(List.of(dimension)));
            assertEquals((long) expectedResults.get("entries").get(shardIdString), stats.getEntriesByDimensions(List.of(dimension)));

            // Check the total metric getter
            CacheStatsResponse response = stats.getStatsByDimensions(List.of(dimension));
            assertEquals((long) expectedResults.get("hits").get(shardIdString), response.hits);
            assertEquals((long) expectedResults.get("misses").get(shardIdString), response.misses);
            assertEquals((long) expectedResults.get("evictions").get(shardIdString), response.evictions);
            assertEquals((long) expectedResults.get("memory_size").get(shardIdString), response.memorySize);
            assertEquals((long) expectedResults.get("entries").get(shardIdString), response.entries);
        }
    }

    private void checkTotalResults(StatsAndExpectedResults statsAndExpectedResults) {
        // check resulting total values are what we expect
        Map<String, Map<String, Long>> expectedResults = statsAndExpectedResults.expectedShardResults;
        SingleDimensionCacheStats stats = statsAndExpectedResults.stats;

        // Check the individual metric getters
        assertEquals(sumMap(expectedResults.get("hits")), stats.getTotalHits());
        assertEquals(sumMap(expectedResults.get("misses")), stats.getTotalMisses());
        assertEquals(sumMap(expectedResults.get("evictions")), stats.getTotalEvictions());
        assertEquals(sumMap(expectedResults.get("memory_size")), stats.getTotalMemorySize());
        assertEquals(sumMap(expectedResults.get("entries")), stats.getTotalEntries());

        // Check the total metric getter
        CacheStatsResponse totalResponse = stats.getTotalStats();
        assertEquals(sumMap(expectedResults.get("hits")), totalResponse.hits);
        assertEquals(sumMap(expectedResults.get("misses")), totalResponse.misses);
        assertEquals(sumMap(expectedResults.get("evictions")), totalResponse.evictions);
        assertEquals(sumMap(expectedResults.get("memory_size")), totalResponse.memorySize);
        assertEquals(sumMap(expectedResults.get("entries")), totalResponse.entries);
    }

    // Convenience class to allow reusing setup code across tests
    private class StatsAndExpectedResults {
        private final SingleDimensionCacheStats stats;
        private final Map<String, Map<String, Long>> expectedShardResults;
        private final int numShardIds;
        private StatsAndExpectedResults(SingleDimensionCacheStats stats, Map<String, Map<String, Long>> expectedShardResults, int numShardIds) {
            this.stats = stats;
            this.expectedShardResults = expectedShardResults;
            this.numShardIds = numShardIds;
        }
    }
}
