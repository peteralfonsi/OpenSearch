/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.stats;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.BytesStreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.List;

import static org.opensearch.common.cache.stats.SingleDimensionCacheStatsTests.checkShardResults;
import static org.opensearch.common.cache.stats.SingleDimensionCacheStatsTests.checkTotalResults;
import static org.opensearch.common.cache.stats.SingleDimensionCacheStatsTests.getDim;
import static org.opensearch.common.cache.stats.SingleDimensionCacheStatsTests.getDimList;
import static org.opensearch.common.cache.stats.SingleDimensionCacheStatsTests.getPopulatedStats;
import static org.opensearch.common.cache.stats.SingleDimensionCacheStatsTests.StatsAndExpectedResults;

public class TieredSpilloverCacheStatsTests extends OpenSearchTestCase {
    private static String dimensionName = "shardId";
    public void testGets() throws Exception {
        StatsAndExpectedResults heapStatsAndResults = getPopulatedStats(dimensionName);
        StatsAndExpectedResults diskStatsAndResults = getPopulatedStats(dimensionName);
        SingleDimensionCacheStats heapStats = heapStatsAndResults.stats;
        SingleDimensionCacheStats diskStats = diskStatsAndResults.stats;
        TieredSpilloverCacheStats stats = new TieredSpilloverCacheStats(heapStats, diskStats);

        // test total gets
        assertEquals(heapStats.getTotalHits() + diskStats.getTotalHits(), stats.getTotalHits());
        assertEquals(heapStats.getTotalMisses() + diskStats.getTotalMisses(), stats.getTotalMisses());
        assertEquals(heapStats.getTotalEvictions() + diskStats.getTotalEvictions(), stats.getTotalEvictions());
        assertEquals(heapStats.getTotalMemorySize() + diskStats.getTotalMemorySize(), stats.getTotalMemorySize());
        assertEquals(heapStats.getTotalEntries() + diskStats.getTotalEntries(), stats.getTotalEntries());

        // test gets by non-tier dimension
        for (int i = 0; i < heapStatsAndResults.numShardIds; i++) {
            List<CacheStatsDimension> dims = getDimList(i, dimensionName);
            assertEquals(heapStats.getHitsByDimensions(dims) + diskStats.getHitsByDimensions(dims), stats.getHitsByDimensions(dims));
            assertEquals(heapStats.getMissesByDimensions(dims) + diskStats.getMissesByDimensions(dims), stats.getMissesByDimensions(dims));
            assertEquals(heapStats.getEvictionsByDimensions(dims) + diskStats.getEvictionsByDimensions(dims), stats.getEvictionsByDimensions(dims));
            assertEquals(heapStats.getMemorySizeByDimensions(dims) + diskStats.getMemorySizeByDimensions(dims), stats.getMemorySizeByDimensions(dims));
            assertEquals(heapStats.getEntriesByDimensions(dims) + diskStats.getEntriesByDimensions(dims), stats.getEntriesByDimensions(dims));
        }

        // test gets by tier dimension only
        List<CacheStatsDimension> heapDims = List.of(new CacheStatsDimension(TieredSpilloverCacheStats.TIER_DIMENSION_NAME, TieredSpilloverCacheStats.TIER_DIMENSION_VALUE_ON_HEAP));
        List<CacheStatsDimension> diskDims = List.of(new CacheStatsDimension(TieredSpilloverCacheStats.TIER_DIMENSION_NAME, TieredSpilloverCacheStats.TIER_DIMENSION_VALUE_DISK));
        List<List<CacheStatsDimension>> tiersList = Arrays.asList(heapDims, diskDims);
        List<SingleDimensionCacheStats> statsList = Arrays.asList(heapStats, diskStats);
        for (int i = 0; i < 2; i++) {
            SingleDimensionCacheStats tierStats = statsList.get(i);
            List<CacheStatsDimension> tierDims = tiersList.get(i);

            assertEquals(tierStats.getTotalHits(), stats.getHitsByDimensions(tierDims));
            assertEquals(tierStats.getTotalMisses(), stats.getMissesByDimensions(tierDims));
            assertEquals(tierStats.getTotalEvictions(), stats.getEvictionsByDimensions(tierDims));
            assertEquals(tierStats.getTotalMemorySize(), stats.getMemorySizeByDimensions(tierDims));
            assertEquals(tierStats.getTotalEntries(), stats.getEntriesByDimensions(tierDims));
        }

        // test gets by tier dimension and other dimension
        for (int i = 0; i < heapStatsAndResults.numShardIds; i++) {
            List<CacheStatsDimension> shardDimsOnly = List.of(getDim(i, dimensionName));
            heapDims = List.of(
                new CacheStatsDimension(TieredSpilloverCacheStats.TIER_DIMENSION_NAME, TieredSpilloverCacheStats.TIER_DIMENSION_VALUE_ON_HEAP),
                getDim(i, dimensionName));
            diskDims = List.of(
                new CacheStatsDimension(TieredSpilloverCacheStats.TIER_DIMENSION_NAME, TieredSpilloverCacheStats.TIER_DIMENSION_VALUE_DISK),
                getDim(i, dimensionName));
            tiersList = Arrays.asList(heapDims, diskDims);
            for (int j = 0; i < 2; i++) {
                SingleDimensionCacheStats tierStats = statsList.get(j);
                List<CacheStatsDimension> tierDims = tiersList.get(j);

                assertEquals(tierStats.getHitsByDimensions(shardDimsOnly), stats.getHitsByDimensions(tierDims));
                assertEquals(tierStats.getMissesByDimensions(shardDimsOnly), stats.getMissesByDimensions(tierDims));
                assertEquals(tierStats.getEvictionsByDimensions(shardDimsOnly), stats.getEvictionsByDimensions(tierDims));
                assertEquals(tierStats.getMemorySizeByDimensions(shardDimsOnly), stats.getMemorySizeByDimensions(tierDims));
                assertEquals(tierStats.getEntriesByDimensions(shardDimsOnly), stats.getEntriesByDimensions(tierDims));
            }
        }
    }

    public void testInvalidTierName() throws Exception {
        StatsAndExpectedResults heapStatsAndResults = getPopulatedStats(dimensionName);
        StatsAndExpectedResults diskStatsAndResults = getPopulatedStats(dimensionName);
        TieredSpilloverCacheStats stats = new TieredSpilloverCacheStats(heapStatsAndResults.stats, diskStatsAndResults.stats);

        List<CacheStatsDimension> invalidTierDims = List.of(new CacheStatsDimension(TieredSpilloverCacheStats.TIER_DIMENSION_NAME, "nonexistent_tier"));
        assertThrows(AssertionError.class, () -> stats.getEntriesByDimensions(invalidTierDims));
    }

    public void testSerialization() throws Exception {
        StatsAndExpectedResults heapStatsAndResults = getPopulatedStats(dimensionName);
        StatsAndExpectedResults diskStatsAndResults = getPopulatedStats(dimensionName);
        SingleDimensionCacheStats heapStats = heapStatsAndResults.stats;
        SingleDimensionCacheStats diskStats = diskStatsAndResults.stats;
        TieredSpilloverCacheStats stats = new TieredSpilloverCacheStats(heapStats, diskStats);

        BytesStreamOutput os = new BytesStreamOutput();
        stats.writeTo(os);
        BytesStreamInput is = new BytesStreamInput(BytesReference.toBytes(os.bytes()));
        TieredSpilloverCacheStats deserialized = new TieredSpilloverCacheStats(is);

        StatsAndExpectedResults deserializedHeapStatsAndResults = new StatsAndExpectedResults(deserialized.heapStats, heapStatsAndResults.expectedShardResults, heapStatsAndResults.numShardIds);
        checkShardResults(deserializedHeapStatsAndResults);
        checkTotalResults(deserializedHeapStatsAndResults);

        StatsAndExpectedResults deserializedDiskStatsAndResults = new StatsAndExpectedResults(deserialized.diskStats, diskStatsAndResults.expectedShardResults, diskStatsAndResults.numShardIds);
        checkShardResults(deserializedDiskStatsAndResults);
        checkTotalResults(deserializedDiskStatsAndResults);
    }
}
