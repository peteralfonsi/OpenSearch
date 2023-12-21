/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.cache.request;

import org.opensearch.common.cache.tier.enums.CacheStoreType;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.BytesStreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.Map;

public class RequestCacheStatsTests extends OpenSearchTestCase {
    public void testConstructorsAndAdd() throws Exception {
        RequestCacheStats emptyStats = new RequestCacheStats();
        for (CacheStoreType cacheStoreType : CacheStoreType.values()) {
            assertTierState(emptyStats, cacheStoreType, 0, 0, 0, 0, 0);
        }
        assertDiskStatsState(emptyStats, 0, 0);
        Map<CacheStoreType, StatsHolder> testHeapMap = new HashMap<>();
        testHeapMap.put(CacheStoreType.ON_HEAP, new StatsHolder(1, 2, 3, 4, 5));
        Map<CacheStoreType, ShardRequestCache.TierStatsAccumulator> tierSpecificMap = new HashMap<>();
        tierSpecificMap.put(CacheStoreType.DISK, new ShardRequestCache.DiskStatsAccumulator(6, 7));
        RequestCacheStats heapAndSpecificOnlyStats = new RequestCacheStats(testHeapMap, tierSpecificMap);
        for (CacheStoreType cacheStoreType : CacheStoreType.values()) {
            if (cacheStoreType == CacheStoreType.ON_HEAP) {
                assertTierState(heapAndSpecificOnlyStats, cacheStoreType, 1, 2, 3, 4, 5);
            } else {
                assertTierState(heapAndSpecificOnlyStats, cacheStoreType, 0, 0, 0, 0, 0);
            }
        }
        assertDiskStatsState(heapAndSpecificOnlyStats, 6, 7);

        Map<CacheStoreType, StatsHolder> testBothTiersMap = new HashMap<>();
        testBothTiersMap.put(CacheStoreType.ON_HEAP, new StatsHolder(11, 12, 13, 14, 15));
        testBothTiersMap.put(CacheStoreType.DISK, new StatsHolder(6, 7, 8, 9, 10));
        Map<CacheStoreType, ShardRequestCache.TierStatsAccumulator> newTierSpecificMap = new HashMap<>();
        newTierSpecificMap.put(CacheStoreType.ON_HEAP, new ShardRequestCache.OnHeapStatsAccumulator());
        newTierSpecificMap.put(CacheStoreType.DISK, new ShardRequestCache.DiskStatsAccumulator(8, 9));
        RequestCacheStats bothTiersStats = new RequestCacheStats(testBothTiersMap, newTierSpecificMap);
        assertTierState(bothTiersStats, CacheStoreType.ON_HEAP, 11, 12, 13, 14, 15);
        assertTierState(bothTiersStats, CacheStoreType.DISK, 6, 7, 8, 9, 10);

        bothTiersStats.add(heapAndSpecificOnlyStats);
        assertTierState(bothTiersStats, CacheStoreType.ON_HEAP, 12, 14, 16, 18, 20);
        assertTierState(bothTiersStats, CacheStoreType.DISK, 6, 7, 8, 9, 10);
        assertDiskStatsState(bothTiersStats, 14, 16);
    }

    public void testSerialization() throws Exception {
        // This test also implicitly tests StatsHolder serialization
        BytesStreamOutput os = new BytesStreamOutput();

        Map<CacheStoreType, StatsHolder> testMap = new HashMap<>();
        testMap.put(CacheStoreType.ON_HEAP, new StatsHolder(11, 12, 13, 14, 15));
        testMap.put(CacheStoreType.DISK, new StatsHolder(6, 7, 8, 9, 10));
        Map<CacheStoreType, ShardRequestCache.TierStatsAccumulator> tierSpecificMap = new HashMap<>();
        tierSpecificMap.put(CacheStoreType.ON_HEAP, new ShardRequestCache.OnHeapStatsAccumulator());
        tierSpecificMap.put(CacheStoreType.DISK, new ShardRequestCache.DiskStatsAccumulator(20, 21));
        RequestCacheStats stats = new RequestCacheStats(testMap, tierSpecificMap);
        stats.writeTo(os);
        BytesStreamInput is = new BytesStreamInput(BytesReference.toBytes(os.bytes()));
        RequestCacheStats deserialized = new RequestCacheStats(is);

        assertTierState(deserialized, CacheStoreType.ON_HEAP, 11, 12, 13, 14, 15);
        assertTierState(deserialized, CacheStoreType.DISK, 6, 7, 8, 9, 10);
        assertDiskStatsState(deserialized, 20, 21);
    }

    private void assertTierState(
        RequestCacheStats stats,
        CacheStoreType tierType,
        long memSize,
        long evictions,
        long hitCount,
        long missCount,
        long entries
    ) {
        assertEquals(memSize, stats.getMemorySizeInBytes(tierType));
        assertEquals(evictions, stats.getEvictions(tierType));
        assertEquals(hitCount, stats.getHitCount(tierType));
        assertEquals(missCount, stats.getMissCount(tierType));
        assertEquals(entries, stats.getEntries(tierType));
    }

    private void assertDiskStatsState(RequestCacheStats stats, long totalGetTime, long totalDiskReaches) {
        assertEquals(totalGetTime, ((ShardRequestCache.DiskStatsAccumulator) stats.getTierSpecificStats(CacheStoreType.DISK)).getTotalGetTime());
        assertEquals(
            totalDiskReaches,
            ((ShardRequestCache.DiskStatsAccumulator) stats.getTierSpecificStats(CacheStoreType.DISK)).getTotalDiskReaches()
        );
    }
}
