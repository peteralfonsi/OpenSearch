/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.cache.request;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.BytesStreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.Map;

public class RequestCacheStatsTests extends OpenSearchTestCase {
    public void testConstructorsAndAdd() throws Exception {
        RequestCacheStats emptyStats = new RequestCacheStats();
        for (TierType tierType : TierType.values()) {
            assertTierState(emptyStats, tierType, 0, 0, 0, 0, 0);
        }
        Map<TierType, StatsHolder> testHeapMap = new HashMap<>();
        testHeapMap.put(TierType.ON_HEAP, new StatsHolder(1, 2, 3, 4, 5));
        RequestCacheStats heapOnlyStats = new RequestCacheStats(testHeapMap);
        for (TierType tierType : TierType.values()) {
            if (tierType == TierType.ON_HEAP) {
                assertTierState(heapOnlyStats, tierType, 1, 2, 3, 4, 5);
            } else {
                assertTierState(heapOnlyStats, tierType, 0, 0, 0, 0, 0);
            }
        }

        Map<TierType, StatsHolder> testBothTiersMap = new HashMap<>();
        testBothTiersMap.put(TierType.ON_HEAP, new StatsHolder(11, 12, 13, 14, 15));
        testBothTiersMap.put(TierType.DISK, new StatsHolder(6, 7, 8, 9, 10));
        RequestCacheStats bothTiersStats = new RequestCacheStats(testBothTiersMap);
        assertTierState(bothTiersStats, TierType.ON_HEAP, 11, 12, 13, 14, 15);
        assertTierState(bothTiersStats, TierType.DISK, 6, 7, 8, 9, 10);

        bothTiersStats.add(heapOnlyStats);
        assertTierState(bothTiersStats, TierType.ON_HEAP, 12, 14, 16, 18, 20);
        assertTierState(bothTiersStats, TierType.DISK, 6, 7, 8, 9, 10);
    }

    public void testSerialization() throws Exception {
        // This test also implicitly tests StatsHolder serialization
        BytesStreamOutput os = new BytesStreamOutput();

        Map<TierType, StatsHolder> testMap = new HashMap<>();
        testMap.put(TierType.ON_HEAP, new StatsHolder(11, 12, 13, 14, 15));
        testMap.put(TierType.DISK, new StatsHolder(6, 7, 8, 9, 10));
        RequestCacheStats stats = new RequestCacheStats(testMap);
        stats.writeTo(os);
        BytesStreamInput is = new BytesStreamInput(BytesReference.toBytes(os.bytes()));
        RequestCacheStats deserialized = new RequestCacheStats(is);

        assertTierState(deserialized, TierType.ON_HEAP, 11, 12, 13, 14, 15);
        assertTierState(deserialized, TierType.DISK, 6, 7, 8, 9, 10);
    }

    private void assertTierState(
        RequestCacheStats stats,
        TierType tierType,
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
}
